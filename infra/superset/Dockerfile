FROM apache/superset:3.1.3

USER root
RUN pip install "trino[sqlalchemy]"
USER superset

COPY --chmod=755 entrypoint.sh /entrypoint.sh
COPY --chown=superset:superset conf/ /app/conf/
COPY --chown=superset:superset dashboard.zip /app/dashboard.zip

ENTRYPOINT ["/entrypoint.sh"]
