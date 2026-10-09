"""Harvest CAR property perimeters from the public SICAR GeoServer (WFS), one municipality at a time.

Each municipality is read in pages sorted by property code (keyset paging: deep startIndex paging is very slow on this server).
Raw pages are stored as gzip GeoJSON so the run can resume after a crash. Usage:
  python -m sicar_mcp.harvest <raw_dir> [UF,UF,...] [workers]
"""
import gzip, json, os, sys, threading, time, urllib.parse, urllib.request
from queue import Queue, Empty

WFS = "https://geoserver.car.gov.br/geoserver/sicar/wfs"
# The server answers HTTP 500 to user agents that name a bot (tested with curl), so a plain browser-style string is used.
UA = "Mozilla/5.0"
import ssl
CTX = ssl.create_default_context()
CTX.set_ciphers("DEFAULT@SECLEVEL=1")  # the server only completes the TLS 1.2 handshake with relaxed cipher policy
PAGE = 5000
IBGE = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"


def fetch(url, tries=6):
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=120, context=CTX) as r:
                b = r.read()
                return gzip.decompress(b) if b[:2] == b"\x1f\x8b" else b
        except Exception as e:
            err = e
            time.sleep(min(60, 3 * 2 ** i))
    raise err


def url_for(uf, mun, last):
    cql = f"cod_municipio_ibge={mun}" + (f" AND cod_imovel > '{last}'" if last else "")
    q = {"service": "WFS", "version": "2.0.0", "request": "GetFeature", "typeNames": f"sicar:sicar_imoveis_{uf.lower()}",
         "outputFormat": "application/json", "count": PAGE, "sortBy": "cod_imovel", "cql_filter": cql}
    return WFS + "?" + urllib.parse.urlencode(q)


def do_slice(raw, uf, mun, log):
    d = os.path.join(raw, uf)
    done = os.path.join(d, f"{mun}.done")
    if os.path.exists(done):
        return
    last, n, page = None, 0, 0
    while True:
        body = fetch(url_for(uf, mun, last))
        feats = json.loads(body)["features"]
        if not feats:
            break
        with gzip.open(os.path.join(d, f"{mun}_{page:03d}.json.gz"), "wb") as f:
            f.write(body)
        n += len(feats)
        page += 1
        last = feats[-1]["properties"]["cod_imovel"]
        time.sleep(0.3)
        if len(feats) < PAGE:
            break
    open(done, "w").write(str(n))
    log(f"{uf} {mun} {n}")


def main(raw, ufs, workers=4):
    mun = json.loads(fetch(IBGE))
    by = {}
    for m in mun:
        uf = (m.get("regiao-imediata") or {}).get("regiao-intermediaria", {}).get("UF", {}).get("sigla") or m["microrregiao"]["mesorregiao"]["UF"]["sigla"]
        by.setdefault(uf, []).append(m["id"])
    q = Queue()
    for uf in ufs:
        os.makedirs(os.path.join(raw, uf), exist_ok=True)
        for m in by.get(uf, []):
            q.put((uf, m))
    lock = threading.Lock()
    lf = open(os.path.join(raw, "harvest.log"), "a")

    def log(s):
        with lock:
            lf.write(s + "\n"); lf.flush()

    def work():
        while True:
            try:
                uf, m = q.get_nowait()
            except Empty:
                return
            try:
                do_slice(raw, uf, m, log)
            except Exception as e:
                log(f"FAIL {uf} {m} {type(e).__name__} {e}")

    ts = [threading.Thread(target=work) for _ in range(workers)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    log("finished " + ",".join(ufs))


if __name__ == "__main__":
    ufs = sys.argv[2].split(",") if len(sys.argv) > 2 and sys.argv[2] != "ALL" else "AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO".split()
    main(sys.argv[1], ufs, int(sys.argv[3]) if len(sys.argv) > 3 else 4)
