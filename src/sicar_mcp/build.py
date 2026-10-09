"""Turn the raw harvest (gzip GeoJSON pages) into the served dataset.

  python -m sicar_mcp.build RAW_DIR OUT_DIR [UF ...]

OUT_DIR/geom/<UF>/<municipio>.parquet   one file per municipality: attributes, bbox, WKB geometry, sorted along a Hilbert curve
OUT_DIR/attrs.parquet                   attributes only, one row per property, sorted by cod_imovel
OUT_DIR/municipios.parquet              one row per municipality file: uf, code, name, feature count, bbox
OUT_DIR/meta.json                       snapshot date, counts
"""
import glob, json, os, sys, time
import duckdb

COLS = "cod_imovel, status_imovel, dat_criacao, data_atualizacao, area, condicao, uf, municipio, cod_municipio_ibge, m_fiscal, tipo_imovel"


def connect(mem="1400MB", threads=2, tmpdir=os.environ.get("SICAR_DUCK_TMP", "/data/sicar/_duck")):
    c = duckdb.connect()
    os.makedirs(tmpdir, exist_ok=True)
    c.execute(f"SET memory_limit='{mem}'; SET threads={threads}; SET preserve_insertion_order=false; SET temp_directory='{tmpdir}'")
    try:
        c.execute("LOAD spatial")
    except Exception:
        c.execute("INSTALL spatial; LOAD spatial")
    return c


PAGE_SQL = """SELECT
    json_extract_string(pr,'$.cod_imovel') AS cod_imovel, json_extract_string(pr,'$.status_imovel') AS status_imovel,
    json_extract_string(pr,'$.dat_criacao') AS dat_criacao, json_extract_string(pr,'$.data_atualizacao') AS data_atualizacao,
    CAST(json_extract(pr,'$.area') AS DOUBLE) AS area, json_extract_string(pr,'$.condicao') AS condicao,
    json_extract_string(pr,'$.uf') AS uf, json_extract_string(pr,'$.municipio') AS municipio,
    CAST(json_extract(pr,'$.cod_municipio_ibge') AS BIGINT) AS cod_municipio_ibge,
    CAST(json_extract(pr,'$.m_fiscal') AS DOUBLE) AS m_fiscal, json_extract_string(pr,'$.tipo_imovel') AS tipo_imovel,
    ST_XMin(g) AS xmin, ST_YMin(g) AS ymin, ST_XMax(g) AS xmax, ST_YMax(g) AS ymax, ST_AsWKB(g) AS geom
  FROM (SELECT ft->'properties' AS pr, ST_GeomFromGeoJSON(ft->'geometry') AS g
        FROM (SELECT unnest(features) AS ft FROM read_json('{f}', format='auto', maximum_object_size=268435456, columns={{features: 'JSON[]'}})))"""


def build_municipality(c, files, dest, tmp):
    parts = []
    for i, f in enumerate(files):
        p = f"{tmp}/p{i}.parquet"
        c.execute(f"COPY ({PAGE_SQL.format(f=f)}) TO '{p}' (FORMAT parquet, COMPRESSION zstd)")
        parts.append("'" + p + "'")
    src = f"read_parquet([{','.join(parts)}])"
    try:
        c.execute(f"""COPY (SELECT * FROM {src}
            ORDER BY ST_Hilbert((xmin+xmax)/2, (ymin+ymax)/2, {{min_x: -75.0, min_y: -35.0, max_x: -30.0, max_y: 6.0}}::BOX_2D))
            TO '{dest}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 2000)""")
    except duckdb.OutOfMemoryException:
        # very large municipality: sort by bbox corner only on the narrow columns is not possible in memory, so keep page order (still correct, just less pruning)
        print("unsorted", dest, flush=True)
        c.execute(f"COPY (SELECT * FROM {src}) TO '{dest}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 2000)")
    for i in range(len(files)):
        os.remove(f"{tmp}/p{i}.parquet")


def main(raw, out, ufs):
    c = connect()
    os.makedirs(out + "/geom", exist_ok=True)
    t0, n = time.time(), 0
    tmp = out + "/_tmp"
    os.makedirs(tmp, exist_ok=True)
    ufs = ufs or sorted(d for d in os.listdir(raw) if os.path.isdir(f"{raw}/{d}"))
    for uf in ufs:
        os.makedirs(f"{out}/geom/{uf}", exist_ok=True)
        muns = sorted({os.path.basename(f)[:-5] for f in glob.glob(f"{raw}/{uf}/*.done")})
        for m in muns:
            dest = f"{out}/geom/{uf}/{m}.parquet"
            if os.path.exists(dest):
                continue
            files = sorted(glob.glob(f"{raw}/{uf}/{m}_*.json.gz"))
            if not files:  # municipality with no registered properties
                print("empty", uf, m, flush=True)
                continue
            build_municipality(c, files, dest + ".tmp", tmp)
            os.replace(dest + ".tmp", dest)
            n += 1
            if n % 100 == 0:
                print(uf, n, round(time.time() - t0), flush=True)
    print("municipalities built", n, flush=True)


def finish(out, snapshot):
    c = connect()
    g = f"{out}/geom/*/*.parquet"
    # properties edited while the harvest ran can appear twice; keep the most recently updated row
    dups = [r[0] for r in c.execute(f"SELECT cod_imovel FROM read_parquet('{g}') GROUP BY 1 HAVING count(*) > 1").fetchall()]
    lst = ",".join("'" + d.replace("'", "") + "'" for d in dups) or "''"
    cols = COLS + ", xmin, ymin, xmax, ymax"
    c.execute(f"""COPY (
        SELECT {cols} FROM read_parquet('{g}') WHERE cod_imovel NOT IN ({lst})
        UNION ALL
        SELECT {cols} FROM (SELECT {cols}, row_number() OVER (PARTITION BY cod_imovel ORDER BY data_atualizacao DESC NULLS LAST) AS rn
                            FROM read_parquet('{g}') WHERE cod_imovel IN ({lst})) WHERE rn = 1
        ORDER BY cod_imovel) TO '{out}/attrs.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)""")
    c.execute(f"""COPY (SELECT uf, cod_municipio_ibge, any_value(municipio) AS municipio, count(*) AS n, min(xmin) AS xmin, min(ymin) AS ymin, max(xmax) AS xmax, max(ymax) AS ymax,
        filename AS file FROM read_parquet('{g}', filename=true) GROUP BY ALL) TO '{out}/municipios.parquet' (FORMAT parquet)""")
    n, d = c.execute(f"SELECT count(*), count(DISTINCT cod_imovel) FROM read_parquet('{out}/attrs.parquet')").fetchone()
    meta = {"snapshot_date": snapshot, "properties": n, "distinct_cod_imovel": d, "rows_before_dedup": c.execute(f"SELECT count(*) FROM read_parquet('{g}')").fetchone()[0], "built_at": time.strftime("%Y-%m-%d")}
    json.dump(meta, open(out + "/meta.json", "w"))
    print(meta)


if __name__ == "__main__":
    a = sys.argv
    if a[1] == "finish":
        finish(a[2], a[3])
    else:
        main(a[1], a[2], a[3:])
