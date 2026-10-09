#!/bin/sh
set -e
if [ "$SICAR_MODE" = "build" ]; then
  mkdir -p /data/sicar
  python -u -m sicar_mcp.build /data/raw /data/sicar
  python -u -m sicar_mcp.build finish /data/sicar "${SICAR_SNAPSHOT:-unknown}"
  echo BUILD_DONE
  sleep infinity
fi
exec python -m sicar_mcp.server --http
