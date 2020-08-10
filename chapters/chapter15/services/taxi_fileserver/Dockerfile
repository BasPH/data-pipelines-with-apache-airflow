FROM nginx:1.19-alpine

RUN apk update && \
    apk add postgresql-client && \
    mkdir /data

ENV DATA_YEAR=2019

COPY get_last_hour.sh /etc/periodic/15min/get_last_hour
COPY nginx.conf /etc/nginx/nginx.conf
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /etc/periodic/15min/get_last_hour && \
    chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
