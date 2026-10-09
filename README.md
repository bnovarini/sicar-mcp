# sicar-mcp

An MCP server for Brazil's rural property registry, the CAR (Cadastro Ambiental Rural). It answers questions about the property perimeters that SICAR publishes: does this CAR code exist, is it active, how big is it, which municipality, what other registered properties overlap it, and which property contains this coordinate.

It reads only public data. There is no owner name, CPF or CNPJ in it, because SICAR does not publish them. CAR is self-declared: a CAR code is not proof of ownership or of land title, and perimeters can overlap or be wrong.

Hosted endpoint (no install, no key): `https://sicar-mcp.fly.dev/mcp`

```json
{ "mcpServers": { "sicar": { "url": "https://sicar-mcp.fly.dev/mcp" } } }
```

## Tools

| Tool | What it does |
|---|---|
| `dataset_info` | Snapshot date, property counts by state, fields, limits. |
| `get_property` | One property by CAR code: status, declared area, municipality, dates, bounding box, optional GeoJSON perimeter. |
| `find_property_at` | Properties whose perimeter contains a latitude/longitude. |
| `overlap_check` | Properties overlapping a given property, with overlap area in hectares and as a share of the property. |
| `municipality_summary` | Registrations in a municipality: counts, declared hectares, status, type and size breakdowns. |
| `search_properties` | Filter by state or municipality, status, type and size. Paged. |

Every answer carries `snapshot_date` and a caveat. `search_properties` returns `{"properties": [...], "pagination": {"has_more", "next_offset", "returned"}}`.

## Data

- Source: the public SICAR consultation GeoServer of the Servico Florestal Brasileiro (`https://geoserver.car.gov.br/geoserver/sicar/wfs`, layers `sicar_imoveis_<uf>`), property perimeter layer only. APP, Reserva Legal, native vegetation and the other CAR layers are not in the public service and are not included.
- Snapshot: harvested 8-9 October 2026. 8,537,561 properties in 27 state layers. Per-state counts match the server's own feature counts at the start of the harvest (the harvest found 27 more in ten states, registered while it ran; none missing).
- Fields: `cod_imovel`, `status_imovel` (AT active, PE pending, SU suspended, CA cancelled), `condicao`, declared `area` (ha), `m_fiscal`, `tipo_imovel`, `uf`, `municipio`, `cod_municipio_ibge`, creation and update dates, perimeter (EPSG:4674).
- The state prefix in a CAR code does not always match the state layer it was published in (a few BA- codes sit in the SE layer). Lookups use the code as published.
- Overlap areas are measured on the drawn perimeters in EPSG:5880 (an equal-area projection for Brazil). They can differ from the declared area.
- License: the code is MIT. The terms for redistributing the SICAR data have not been confirmed with the Servico Florestal Brasileiro; check them before republishing the dataset. The server returns query results, not the raw layers.
- The GeoServer answers HTTP 500 to some default bot user agents, so the harvester sends a browser-style `Mozilla/5.0` user agent. It pages one municipality at a time, four workers, and fetched about 2.3 GB of compressed GeoJSON in about three hours.

## Run it yourself

```
pip install .
python -m sicar_mcp.harvest /data/raw ALL 4          # about 3 hours; resumable
python -m sicar_mcp.build /data/raw /data/sicar        # one Parquet file per municipality, Hilbert-sorted
python -m sicar_mcp.build finish /data/sicar 2026-10-08
SICAR_DATA_DIR=/data/sicar python -m sicar_mcp.server --http
```

Parquet output is about 3.7 GB in total. The server uses DuckDB with the spatial extension; point lookups read only the municipality files whose bounding box contains the point. `deploy/` has the Fly.io configuration used for the hosted endpoint (a 2 GB shared machine and a 12 GB volume).

## Not included

- Owner or holder information. It is restricted in SICAR. A seller can give you their CAR code; this server can check that the code exists, is active and matches the stated area and municipality. Ownership is proven by the matricula at the cartorio.
- APP, Reserva Legal, vegetation and hydrography layers (captcha-protected downloads on the SICAR site).
- Rural credit, embargoes, deforestation alerts and other overlays.
