FROM postgres:12

USER root

# Install build dependencies and wal2json
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        git \
        build-essential \
        postgresql-server-dev-12 \
    && git clone --depth 1 --branch wal2json_2_3 https://github.com/eulerto/wal2json.git /wal2json \
    && cd /wal2json \
    && make && make install \
    && cd / \
    && rm -rf wal2json \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER postgres
