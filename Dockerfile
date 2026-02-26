FROM postgres_base AS base

###
### BUILDER
###
FROM base AS builder
ARG POSTGRES_VERSION

RUN apt-get update -qq && \
    apt-get install -y \
    postgresql-server-dev-${POSTGRES_VERSION} \
    build-essential libreadline-dev zlib1g-dev flex bison libxml2-dev libxslt-dev \
    libssl-dev libxml2-utils xsltproc pkg-config libc++-dev libc++abi-dev libglib2.0-dev \
    libtinfo5 cmake libstdc++-12-dev liblz4-dev ccache ninja-build git libcurl4-openssl-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

ENV PATH=/usr/lib/ccache:$PATH
ENV CCACHE_DIR=/ccache

# permissions so we can run as `postgres` (uid=999,gid=999)
RUN mkdir /out
RUN chown -R postgres:postgres . /usr/lib/postgresql /usr/share/postgresql /out
USER postgres

COPY --chown=postgres:postgres Makefile Makefile.global pg_ducklake.control ./
COPY --chown=postgres:postgres .git/modules/third_party/pg_duckdb/HEAD .git/modules/third_party/pg_duckdb/HEAD
COPY --chown=postgres:postgres .git/modules/third_party/pg_duckdb/modules/third_party/duckdb/HEAD .git/modules/third_party/pg_duckdb/modules/third_party/duckdb/HEAD
COPY --chown=postgres:postgres .git/modules/third_party/ducklake/HEAD .git/modules/third_party/ducklake/HEAD
COPY --chown=postgres:postgres sql sql
COPY --chown=postgres:postgres src src
COPY --chown=postgres:postgres include include
COPY --chown=postgres:postgres third_party third_party
COPY --chown=postgres:postgres test test

# workaround for missing submodule in pg_duckdb build
RUN rm -rf third_party/pg_duckdb/.git && \
    mkdir -p third_party/pg_duckdb/.git/modules/third_party/duckdb && \
    cp .git/modules/third_party/pg_duckdb/modules/third_party/duckdb/HEAD \
      third_party/pg_duckdb/.git/modules/third_party/duckdb/HEAD

RUN make clean-all

# build and install both extensions
RUN --mount=type=cache,target=/ccache/,uid=999,gid=999 echo "Available CPUs=$(nproc)" && \
    make -j$(nproc) pg_duckdb && \
    make -j$(nproc)
# install into location specified by pg_config for tests
RUN make install-pg_duckdb install
# install into /out for packaging
RUN DESTDIR=/out make install-pg_duckdb install

###
### CHECKER
###
FROM builder AS checker

USER postgres
RUN make installcheck

###
### OUTPUT
###
FROM base AS output

RUN apt-get update -qq && \
    apt-get install -y ca-certificates libcurl4

# Automatically preload pg_duckdb,pg_ducklake
RUN echo "shared_preload_libraries='pg_duckdb,pg_ducklake'" >> /usr/share/postgresql/postgresql.conf.sample
COPY --chown=postgres:postgres docker/init.d/ /docker-entrypoint-initdb.d/

COPY --from=builder /out /
USER postgres
