FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir "duckdb>=1.1" "mcp>=1.2,<2" uvicorn \
 && python -c "import duckdb; duckdb.connect().execute('INSTALL spatial')"
COPY pyproject.toml README.md ./
COPY src ./src
COPY deploy/run.sh ./run.sh
ENV PYTHONPATH=/app/src SICAR_DATA_DIR=/data/sicar
CMD ["sh", "/app/run.sh"]
