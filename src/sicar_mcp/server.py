"""sicar-mcp: Brazil's CAR (Cadastro Ambiental Rural) rural property perimeters as MCP tools. Public SICAR data, no owner information."""
from __future__ import annotations

import functools
import json
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any, Optional

import duckdb
from mcp.server.fastmcp import FastMCP

MAX_ROWS = 100
QUERY_TIMEOUT_S = float(os.environ.get("SICAR_QUERY_TIMEOUT", "40"))
STATUS = {"AT": "ativo", "PE": "pendente", "SU": "suspenso", "CA": "cancelado"}
UFS = {"AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"}

CAVEAT = ("CAR is self-declared by the property holder and is not proof of ownership or land title. Perimeters can overlap, can be wrong, and are not "
          "checked against the property registry (matricula). This dataset has no owner, CPF or CNPJ: those are not public in SICAR.")
NOTE = (
    "Data: public SICAR (Sistema Nacional de Cadastro Ambiental Rural) property perimeters, read from the Servico Florestal Brasileiro public GeoServer "
    "and stored as a snapshot. Every answer carries the snapshot date. Areas are in hectares as SICAR declares them; geometry is EPSG:4674 (SIRGAS 2000, "
    "longitude/latitude). Property perimeters come from the snapshot; per-property totals for Reserva Legal, APP, native vegetation, consolidated area, hydrography, administrative easement, restricted use and fallow come from the Base dos Dados copy of SICAR (an older extraction, see layers_source). "
    + CAVEAT + " Property codes look like 'DF-5300108-3CE57FCD...'; the state prefix in a code does not always match the state layer it was published in."
)
mcp = FastMCP("sicar-mcp", instructions=NOTE)
_con: Optional[duckdb.DuckDBPyConnection] = None


def data_dir() -> Path:
    return Path(os.environ.get("SICAR_DATA_DIR", "/data/sicar"))


def meta() -> dict:
    p = data_dir() / "meta.json"
    return json.loads(p.read_text()) if p.exists() else {}


def con() -> duckdb.DuckDBPyConnection:
    global _con
    if _con is None:
        d = data_dir()
        if not (d / "meta.json").exists():
            raise RuntimeError("Dataset is not built yet.")
        c = duckdb.connect()
        c.execute(f"SET memory_limit='{os.environ.get('SICAR_MEMORY_LIMIT', '1200MB')}'; SET threads={int(os.environ.get('SICAR_THREADS', '2'))}; SET temp_directory='{d}/.tmp'")
        try:
            c.execute("LOAD spatial")
        except Exception:
            c.execute("INSTALL spatial; LOAD spatial")
        c.execute(f"CREATE VIEW a AS SELECT * FROM read_parquet('{d}/attrs.parquet')")
        if (d / "layers.parquet").exists():
            c.execute(f"CREATE VIEW l AS SELECT * FROM read_parquet('{d}/layers.parquet')")
        c.execute(f"CREATE TABLE muns AS SELECT * FROM read_parquet('{d}/municipios.parquet')")
        _con = c
    return _con


def run(sql: str, params: list | None = None) -> list[dict]:
    cur = con().cursor()
    timer = threading.Timer(QUERY_TIMEOUT_S, cur.interrupt)
    timer.start()
    try:
        cur.execute(sql, params or [])
        rows = cur.fetchall()
    except duckdb.InterruptException:
        raise RuntimeError(f"Query took longer than {QUERY_TIMEOUT_S:.0f}s and was stopped. Narrow the filters.")
    finally:
        timer.cancel()
    cols = [x[0] for x in cur.description]
    return [dict(zip(cols, r)) for r in rows]


def _ans(**kw) -> dict:
    return {"snapshot_date": meta().get("snapshot_date"), "caveat": CAVEAT, **kw}


def _prop(r: dict) -> dict:
    r = dict(r)
    r["status"] = STATUS.get(r.get("status_imovel"), r.get("status_imovel"))
    r["area_ha"] = r.pop("area", None)
    r["bbox"] = [r.pop("xmin", None), r.pop("ymin", None), r.pop("xmax", None), r.pop("ymax", None)]
    return r


LAYERS = [("reserva_legal", "Reserva Legal (legal reserve)"), ("app", "APP (permanent preservation area)"),
          ("vegetacao_nativa", "native vegetation remnant"), ("area_consolidada", "consolidated area (land use before 2008)"),
          ("hidrografia", "hydrography (water bodies and watercourses as polygons)"), ("servidao_administrativa", "administrative easement"),
          ("uso_restrito", "restricted-use area"), ("area_pousio", "fallow area")]
LAYERS_SOURCE = ("Base dos Dados (basedosdados.br_sfb_sicar, BigQuery), SICAR extraction dated 2026-06-02 to 2026-08-04 per state; older than the property snapshot. "
                 "Hectares are the sum of declared layer polygons per property, as published; layers can overlap each other and a property can be reported with a polygon "
                 "total above its declared area. Properties with no polygon in a layer have no entry for that layer.")


def _has_layers() -> bool:
    return (data_dir() / "layers.parquet").exists()


def _layer_block(r: dict, area) -> dict | None:
    if not r:
        return None
    out = {}
    for k, label in LAYERS:
        ha = r.get(k + "_ha")
        if ha is None:
            out[k] = None
            continue
        out[k] = {"hectares": ha, "polygons": r.get(k + "_n"), "share_of_declared_area": round(ha / area, 3) if area else None}
    return out


def _files(cods: list[int] | None = None, point: tuple | None = None, bbox: tuple | None = None) -> list[str]:
    w, p = [], []
    if cods is not None:
        w.append("cod_municipio_ibge IN (" + ",".join("?" * len(cods)) + ")"); p += list(cods)
    if point:
        w.append("xmin <= ? AND xmax >= ? AND ymin <= ? AND ymax >= ?"); p += [point[0], point[0], point[1], point[1]]
    if bbox:
        w.append("xmin <= ? AND xmax >= ? AND ymin <= ? AND ymax >= ?"); p += [bbox[2], bbox[0], bbox[3], bbox[1]]
    return [r["file"] for r in run("SELECT DISTINCT file FROM muns WHERE " + (" AND ".join(w) or "TRUE"), p)]


def _read(files: list[str]) -> str:
    return "read_parquet([" + ",".join("'" + f.replace("'", "") + "'" for f in files) + "])"


def _split_pages(key):
    def deco(fn):
        @functools.wraps(fn)
        def inner(*a, **kw):
            rows, more, nxt = fn(*a, **kw)
            return _ans(**{key: rows, "pagination": {"has_more": more, "next_offset": nxt, "returned": len(rows)}})
        return inner
    return deco


def _page(limit: int, offset: int) -> tuple[int, int]:
    if not isinstance(limit, int) or not 1 <= limit <= MAX_ROWS:
        raise ValueError(f"limit must be an integer from 1 to {MAX_ROWS}")
    if not isinstance(offset, int) or offset < 0:
        raise ValueError("offset must be a non-negative integer")
    return limit, offset


@mcp.tool(description="Describe the dataset: snapshot date, number of properties, coverage, fields and limits. Call this first if you are unsure what is available.")
def dataset_info() -> dict:
    m = meta()
    by = run("SELECT uf, count(*) AS properties FROM a GROUP BY uf ORDER BY uf")
    return _ans(note=NOTE, properties=m.get("properties"), by_uf=by, status_codes=STATUS,
                fields={"cod_imovel": "CAR registration code", "status_imovel": "AT active, PE pending, SU suspended, CA cancelled", "condicao": "analysis status text from SICAR",
                        "area_ha": "declared area in hectares", "m_fiscal": "area in fiscal modules", "tipo_imovel": "IRU rural property, AST settlement, PCT traditional community",
                        "dat_criacao / data_atualizacao": "registration and last update timestamps in SICAR", "bbox": "[min lon, min lat, max lon, max lat]"},
                source="https://geoserver.car.gov.br/geoserver/sicar/wfs (Servico Florestal Brasileiro, SICAR public consultation)")


@mcp.tool(description="Get one rural property by CAR code (cod_imovel, e.g. 'DF-5300108-3CE57FCD1D064C3290726CED4DBCBDD1'). Returns status, declared area, municipality, "
                      "dates and bounding box, plus the snapshot date. Set include_geometry to also get the perimeter as GeoJSON (can be large). Use it to check that a "
                      "CAR code exists, is active, and matches the area and municipality a seller states. It does not return the owner.")
def get_property(cod_imovel: str, include_geometry: bool = False) -> dict:
    code = (cod_imovel or "").strip()
    if len(code) < 10:
        raise ValueError("cod_imovel looks too short; expected something like 'DF-5300108-3CE57FCD1D064C3290726CED4DBCBDD1'")
    rows = run("SELECT * FROM a WHERE cod_imovel = ?", [code])
    if not rows:
        return _ans(found=False, message="No property with that code in this snapshot. It may be mistyped, registered after the snapshot, or cancelled and removed.")
    r = _prop(rows[0])
    out = {"found": True, "property": r}
    if _has_layers():
        lr = run("SELECT * FROM l WHERE cod_imovel = ?", [code])
        out["layers"] = _layer_block(lr[0], r.get("area_ha")) if lr else None
        out["layers_source"] = LAYERS_SOURCE
    if include_geometry:
        fs = _files([r["cod_municipio_ibge"]], bbox=tuple(r["bbox"]))
        g = run(f"SELECT ST_AsGeoJSON(ST_GeomFromWKB(geom)) AS g FROM {_read(fs)} WHERE cod_imovel = ?", [code]) if fs else []
        out["geometry"] = json.loads(g[0]["g"]) if g else None
    return _ans(**out)


@mcp.tool(description="Find the CAR properties whose perimeter contains a point (latitude, longitude in decimal degrees, WGS84/SIRGAS). Perimeters can overlap, so several "
                      "properties may come back. Use it to check which registered property a coordinate falls in.")
def find_property_at(latitude: float, longitude: float, limit: int = 25) -> dict:
    if not (-35 <= latitude <= 6 and -75 <= longitude <= -30):
        raise ValueError("coordinate is outside Brazil; pass latitude (about -34 to 6) and longitude (about -74 to -34) in decimal degrees")
    limit, _ = _page(limit, 0)
    fs = _files(point=(longitude, latitude))
    if not fs:
        return _ans(properties=[], message="No municipality in the dataset covers this point.")
    rows = run(f"""SELECT cod_imovel, status_imovel, condicao, area, uf, municipio, cod_municipio_ibge, m_fiscal, tipo_imovel, dat_criacao, data_atualizacao, xmin, ymin, xmax, ymax
                   FROM {_read(fs)} WHERE xmin <= ? AND xmax >= ? AND ymin <= ? AND ymax >= ? AND ST_Contains(ST_GeomFromWKB(geom), ST_Point(?, ?)) LIMIT {limit * 2}""",
               [longitude, longitude, latitude, latitude, longitude, latitude])
    seen = set()
    rows = [r for r in rows if not (r["cod_imovel"] in seen or seen.add(r["cod_imovel"]))][:limit]
    return _ans(point={"latitude": latitude, "longitude": longitude}, properties=[_prop(r) for r in rows],
                message=None if rows else "The point is not inside any registered CAR perimeter in this snapshot.")


@mcp.tool(description="Properties whose perimeter overlaps a given property (by CAR code). Returns each neighbour with the overlap area in hectares and as a share of the "
                      "given property. Overlap between CAR perimeters is common and is not by itself proof of a dispute or error. Up to 50 results, largest overlap first.")
def overlap_check(cod_imovel: str, min_overlap_ha: float = 0.01, limit: int = 25) -> dict:
    limit, _ = _page(limit, 0)
    rows = run("SELECT * FROM a WHERE cod_imovel = ?", [(cod_imovel or "").strip()])
    if not rows:
        return _ans(found=False, message="No property with that code in this snapshot.")
    r = _prop(rows[0])
    bb = tuple(r["bbox"])
    fs = _files(bbox=bb)
    base = run(f"SELECT geom FROM {_read(_files([r['cod_municipio_ibge']], bbox=bb))} WHERE cod_imovel = ?", [r["cod_imovel"]])
    if not base:
        return _ans(found=True, property=r, overlaps=[], message="Geometry not found for this property.")
    res = run(f"""WITH me AS (SELECT ST_Transform(ST_MakeValid(ST_GeomFromWKB(?)), 'EPSG:4674', 'EPSG:5880', true) AS g),
        o AS (SELECT cod_imovel, status_imovel, area, uf, municipio, tipo_imovel, ST_Transform(ST_MakeValid(ST_GeomFromWKB(geom)), 'EPSG:4674', 'EPSG:5880', true) AS g
              FROM {_read(fs)} WHERE cod_imovel <> ? AND xmin <= ? AND xmax >= ? AND ymin <= ? AND ymax >= ?)
        SELECT o.cod_imovel, o.status_imovel, o.area AS area, o.uf, o.municipio, o.tipo_imovel, ST_Area(ST_Intersection(me.g, o.g)) / 10000.0 AS overlap_ha, ST_Area(me.g) / 10000.0 AS mine_ha
        FROM o, me WHERE ST_Intersects(me.g, o.g) ORDER BY overlap_ha DESC LIMIT {limit * 3}""", [bytes(base[0]["geom"]), r["cod_imovel"], bb[2], bb[0], bb[3], bb[1]])
    out = []
    for x in res:
        if x["overlap_ha"] is None or x["overlap_ha"] < min_overlap_ha:
            continue
        out.append({"cod_imovel": x["cod_imovel"], "status": STATUS.get(x["status_imovel"], x["status_imovel"]), "declared_area_ha": x["area"], "uf": x["uf"], "municipio": x["municipio"],
                    "tipo_imovel": x["tipo_imovel"], "overlap_ha": round(x["overlap_ha"], 3), "share_of_this_property": round(x["overlap_ha"] / x["mine_ha"], 4) if x["mine_ha"] else None})
    return _ans(property=r, overlaps=out[:limit], measured_area_ha=round(res[0]["mine_ha"], 3) if res else None,
                note="Overlap areas are measured on the perimeters in EPSG:5880 (equal-area projection for Brazil); declared_area_ha is what the holder declared and can differ from the drawn area.")


def _muns_for(uf: Optional[str], municipio: Optional[str], codigo: Optional[int]) -> list[int]:
    w, p = [], []
    if codigo:
        w.append("cod_municipio_ibge = ?"); p.append(int(codigo))
    if municipio:
        w.append("strip_accents(upper(municipio)) = strip_accents(upper(?))"); p.append(municipio.strip())
    if uf:
        if uf.upper() not in UFS:
            raise ValueError("uf must be a two-letter state code")
        w.append("uf = ?"); p.append(uf.upper())
    if not w:
        raise ValueError("give a municipio (with uf), or cod_municipio_ibge")
    return [r["cod_municipio_ibge"] for r in run("SELECT DISTINCT cod_municipio_ibge FROM muns WHERE " + " AND ".join(w), p)]


@mcp.tool(description="Reserva Legal, APP, native vegetation, consolidated area, hydrography, easement, restricted-use and fallow totals for one municipality (name plus uf, or IBGE code), from the Base dos Dados copy of SICAR: "
                      "number of properties with each layer, total hectares, and the share of the declared area of those same properties. Layer data is an older extraction than the property snapshot.")
def municipality_layers(municipio: Optional[str] = None, uf: Optional[str] = None, cod_municipio_ibge: Optional[int] = None) -> dict:
    if not _has_layers():
        return _ans(found=False, message="Layer data is not loaded in this deployment.")
    cods = _muns_for(uf, municipio, cod_municipio_ibge)
    if not cods:
        return _ans(found=False, message="No municipality matched. Check the spelling and pass uf (a state code) with the name.")
    ph = ",".join("?" * len(cods))
    sel = ", ".join(f"count(l.{k}_ha) AS {k}_properties, round(sum(l.{k}_ha),1) AS {k}_ha, round(sum(CASE WHEN l.{k}_ha IS NOT NULL THEN a.area END),1) AS {k}_declared_ha_of_those" for k, _ in LAYERS)
    rows = run(f"SELECT a.cod_municipio_ibge, any_value(a.municipio) AS municipio, any_value(a.uf) AS uf, count(*) AS properties, round(sum(a.area),1) AS declared_ha, {sel} "
               f"FROM a LEFT JOIN l USING (cod_imovel) WHERE a.cod_municipio_ibge IN ({ph}) GROUP BY 1 ORDER BY 1", cods)
    out = []
    for r in rows:
        d = {k: r[k] for k in ("cod_municipio_ibge", "municipio", "uf", "properties", "declared_ha")}
        for k, label in LAYERS:
            tot, dec = r[k + "_ha"], r[k + "_declared_ha_of_those"]
            d[k] = {"properties_with_layer": r[k + "_properties"], "hectares": tot, "share_of_declared_area_of_those_properties": round(tot / dec, 3) if tot and dec else None}
        out.append(d)
    return _ans(municipalities=out, layers_source=LAYERS_SOURCE)


@mcp.tool(description="Summary of CAR registrations in one municipality (by name plus uf, or IBGE code): number of properties, total declared hectares, breakdown by status and "
                      "property type, and size classes. Municipality names are matched ignoring accents and case.")
def municipality_summary(municipio: Optional[str] = None, uf: Optional[str] = None, cod_municipio_ibge: Optional[int] = None) -> dict:
    cods = _muns_for(uf, municipio, cod_municipio_ibge)
    if not cods:
        return _ans(found=False, message="No municipality matched. Check the spelling and pass uf (a state code) with the name.")
    ph = ",".join("?" * len(cods))
    tot = run(f"SELECT cod_municipio_ibge, any_value(municipio) AS municipio, any_value(uf) AS uf, count(*) AS properties, round(sum(area),1) AS declared_ha FROM a WHERE cod_municipio_ibge IN ({ph}) GROUP BY 1 ORDER BY 1", cods)
    st = run(f"SELECT cod_municipio_ibge, status_imovel AS status, count(*) AS n, round(sum(area),1) AS ha FROM a WHERE cod_municipio_ibge IN ({ph}) GROUP BY 1,2 ORDER BY 1,3 DESC", cods)
    ty = run(f"SELECT cod_municipio_ibge, tipo_imovel, count(*) AS n FROM a WHERE cod_municipio_ibge IN ({ph}) GROUP BY 1,2 ORDER BY 1,3 DESC", cods)
    sz = run(f"""SELECT cod_municipio_ibge, CASE WHEN area < 10 THEN 'under 10 ha' WHEN area < 100 THEN '10-100 ha' WHEN area < 1000 THEN '100-1000 ha' ELSE '1000 ha and over' END AS size_class, count(*) AS n
                 FROM a WHERE cod_municipio_ibge IN ({ph}) GROUP BY 1,2 ORDER BY 1,3 DESC""", cods)
    out = []
    for t in tot:
        c = t["cod_municipio_ibge"]
        out.append({**t, "by_status": [{"status": STATUS.get(x["status"], x["status"]), "properties": x["n"], "declared_ha": x["ha"]} for x in st if x["cod_municipio_ibge"] == c],
                    "by_type": [{"tipo_imovel": x["tipo_imovel"], "properties": x["n"]} for x in ty if x["cod_municipio_ibge"] == c],
                    "by_size": [{"size_class": x["size_class"], "properties": x["n"]} for x in sz if x["cod_municipio_ibge"] == c]})
    return _ans(municipalities=out)


@mcp.tool(description="Search CAR properties by municipality (name plus uf, or IBGE code), status (AT, PE, SU, CA), type (IRU, AST, PCT) and declared area in hectares. "
                      "Returns up to 100 per page; page with pagination.next_offset. Ordered by declared area, largest first, unless order_by is 'recent' (last update).")
@_split_pages("properties")
def search_properties(uf: Optional[str] = None, municipio: Optional[str] = None, cod_municipio_ibge: Optional[int] = None, status: Optional[str] = None,
                      tipo_imovel: Optional[str] = None, min_area_ha: Optional[float] = None, max_area_ha: Optional[float] = None, order_by: str = "area", limit: int = 25, offset: int = 0):
    limit, offset = _page(limit, offset)
    if order_by not in ("area", "recent"):
        raise ValueError("order_by must be 'area' or 'recent'")
    w, p = [], []
    if municipio or cod_municipio_ibge:
        cods = _muns_for(uf, municipio, cod_municipio_ibge)
        if not cods:
            return [], False, None
        w.append("cod_municipio_ibge IN (" + ",".join("?" * len(cods)) + ")"); p += cods
    elif uf:
        if uf.upper() not in UFS:
            raise ValueError("uf must be a two-letter state code")
        w.append("uf = ?"); p.append(uf.upper())
    else:
        raise ValueError("give a uf, or a municipio / cod_municipio_ibge, so the search stays small")
    if status:
        if status.upper() not in STATUS:
            raise ValueError("status must be AT, PE, SU or CA")
        w.append("status_imovel = ?"); p.append(status.upper())
    if tipo_imovel:
        w.append("tipo_imovel = ?"); p.append(tipo_imovel.upper())
    if min_area_ha is not None:
        w.append("area >= ?"); p.append(min_area_ha)
    if max_area_ha is not None:
        w.append("area <= ?"); p.append(max_area_ha)
    order = "area DESC NULLS LAST" if order_by == "area" else "data_atualizacao DESC NULLS LAST"
    rows = run(f"SELECT * FROM a WHERE {' AND '.join(w)} ORDER BY {order}, cod_imovel LIMIT {limit + 1} OFFSET {offset}", p)
    more = len(rows) > limit
    return [_prop(r) for r in rows[:limit]], more, (offset + limit if more else None)


class RateLimit:
    def __init__(self, app, per_minute: int):
        self.app, self.per_minute, self.hits = app, per_minute, {}

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope["path"] != "/healthz":
            h = dict(scope["headers"])
            ip = (h.get(b"fly-client-ip") or h.get(b"x-forwarded-for", b"").split(b",")[0] or b"?").decode().strip()
            now = time.time()
            q_ = [t for t in self.hits.get(ip, []) if now - t < 60]
            if len(q_) >= self.per_minute:
                await send({"type": "http.response.start", "status": 429, "headers": [(b"content-type", b"application/json"), (b"retry-after", b"60")]})
                await send({"type": "http.response.body", "body": b'{"error":"rate limit exceeded, try again in a minute"}'})
                return
            q_.append(now)
            self.hits[ip] = q_
            if len(self.hits) > 5000:
                self.hits = {k: v for k, v in self.hits.items() if v and now - v[-1] < 60}
        await self.app(scope, receive, send)


def _forbid_extra_arguments() -> None:
    for t in mcp._tool_manager.list_tools():
        model = t.fn_metadata.arg_model
        model.model_config["extra"] = "forbid"
        model.model_rebuild(force=True)


_forbid_extra_arguments()


def http_app():
    from mcp.server.transport_security import TransportSecuritySettings
    from starlette.responses import JSONResponse
    hosts = [h for h in os.environ.get("SICAR_ALLOWED_HOSTS", "").split(",") if h]
    mcp.settings.stateless_http = True
    mcp.settings.json_response = True
    mcp.settings.transport_security = TransportSecuritySettings(
        enable_dns_rebinding_protection=bool(hosts), allowed_hosts=hosts, allowed_origins=["*"] if hosts else [])

    @mcp.custom_route("/healthz", methods=["GET"])
    async def healthz(request):
        return JSONResponse({"ok": True, "snapshot": meta().get("snapshot_date")})

    return RateLimit(mcp.streamable_http_app(), int(os.environ.get("SICAR_RATE_PER_MIN", "40")))


def main() -> None:
    argv = sys.argv[1:]
    if "--check" in argv:
        print(dataset_info())
        return
    if "--http" in argv:
        import uvicorn
        uvicorn.run(http_app(), host=os.environ.get("HOST", "0.0.0.0"), port=int(os.environ.get("PORT", "8080")), log_level="warning", timeout_keep_alive=5)
        return
    mcp.run()


if __name__ == "__main__":
    main()
