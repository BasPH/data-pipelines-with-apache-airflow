FROM postgres:12.1-alpine AS builder

COPY postgres-init.sh /docker-entrypoint-initdb.d/postgres-init.sh
RUN mkdir -p /data && \
    /usr/local/bin/docker-entrypoint.sh postgres || true && \
    cp -R /var/lib/postgresql/data/* /data

FROM postgres:12.1-alpine
COPY --from=builder /data /var/lib/postgresql/data
