FROM apache/superset:4.1.1

USER root

RUN pip install --no-cache-dir \
    sqlalchemy-trino==0.5.0 \
    trino==0.336.0

USER superset
