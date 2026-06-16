# E2E harness: throwaway PG cluster (via $PG_CONFIG), fresh db per test, and
# optional MinIO via docker or MINIO_ENDPOINT (E2E_REQUIRE_S3=1: fail, not skip).
# httpfs is not bundled, so the first s3:// access INSTALLs it (needs network).

import os
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import gettempdir

import asyncpg
import duckdb
import filelock
import pytest

PGHOST = "127.0.0.1"


def run(cmd, **kwargs):
    kwargs.setdefault("check", True)
    kwargs.setdefault("text", True)
    return subprocess.run([str(c) for c in cmd], **kwargs)


def capture(cmd, **kwargs):
    return run(cmd, stdout=subprocess.PIPE, **kwargs).stdout


def get_bin_dir():
    pg_config = os.environ.get("PG_CONFIG", "pg_config")
    return capture([pg_config, "--bindir"]).strip()


def skip_or_fail(reason):
    """Honor E2E_REQUIRE_S3: optional infrastructure gaps normally skip."""
    if os.environ.get("E2E_REQUIRE_S3"):
        pytest.fail(reason)
    pytest.skip(reason)


# ---------------------------------------------------------------------------
# Port allocation (same scheme as pg_duckdb/test/pycheck): a filelock plus a
# bind probe, in a range above most OS ephemeral-port starts.

PORT_LOWER_BOUND = 10200
PORT_UPPER_BOUND = 32768

_next_port = PORT_LOWER_BOUND


class PortLock:
    def __init__(self):
        global _next_port
        while True:
            _next_port += 1
            if _next_port >= PORT_UPPER_BOUND:
                _next_port = PORT_LOWER_BOUND

            self.lock = filelock.FileLock(
                Path(gettempdir()) / f"port-{_next_port}.lock"
            )
            try:
                self.lock.acquire(timeout=0)
            except filelock.Timeout:
                continue

            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
                try:
                    s.bind((PGHOST, _next_port))
                    self.port = _next_port
                    break
                except Exception:
                    self.lock.release()
                    continue

    def release(self):
        self.lock.release()


# ---------------------------------------------------------------------------
# Throwaway PostgreSQL cluster


class PgCluster:
    def __init__(self, pgdata: Path):
        self.pgdata = pgdata
        self.log_path = pgdata / "pg.log"
        self.bindir = get_bin_dir()
        self.port_lock = PortLock()
        self.port = self.port_lock.port
        self.host = PGHOST

    def bin(self, name):
        return os.path.join(self.bindir, name)

    def subprocess_env(self):
        # Drop AWS_* so "no DuckDB secret" deterministically means "no S3
        # access"; drop PG* (except PG_CONFIG and PGPASSWORD) so psql/pg_ctl see
        # only our flags. PGPASSWORD is kept for the external-cluster mode.
        return {
            k: v
            for k, v in os.environ.items()
            if not k.startswith("AWS_")
            and (not k.startswith("PG") or k in ("PG_CONFIG", "PGPASSWORD"))
        }

    def initdb(self):
        run(
            [
                self.bin("initdb"),
                "-A",
                "trust",
                "--nosync",
                "--username",
                "postgres",
                "--pgdata",
                self.pgdata,
            ],
            stdout=subprocess.DEVNULL,
            env=self.subprocess_env(),
        )
        with (self.pgdata / "postgresql.conf").open("a") as conf:
            conf.write(f"listen_addresses = '{PGHOST}'\n")
            conf.write("unix_socket_directories = '/tmp'\n")
            conf.write("shared_preload_libraries = 'pg_ducklake'\n")
            # No background flush/compaction: tests drive maintenance
            # explicitly so file layouts stay deterministic.
            conf.write("ducklake.maintenance_enabled = false\n")
            # No max_parallel_workers=0 workaround needed (unlike regression.conf):
            # the kernel defaults DuckDB to one thread, so PG parallelism is safe.
            conf.write("fsync = off\n")
            conf.write("logging_collector = off\n")
            conf.write("log_destination = stderr\n")

    def pgctl(self, command, **kwargs):
        kwargs.setdefault("env", self.subprocess_env())
        run([self.bin("pg_ctl"), "-w", "--pgdata", self.pgdata, *command], **kwargs)

    def start(self):
        try:
            self.pgctl(["-o", f"-p {self.port}", "-l", self.log_path, "start"])
        except Exception:
            if self.log_path.exists():
                print(self.log_path.read_text())
            raise

    def stop(self):
        self.pgctl(["-m", "fast", "stop"], check=False)

    def psql(self, dbname, *statements):
        """Run statements through psql (simple query protocol)."""
        cmd = [
            self.bin("psql"),
            "-X",
            "-Atq",
            "-v",
            "ON_ERROR_STOP=1",
            "-h",
            self.host,
            "-p",
            str(self.port),
            "-U",
            "postgres",
            "-d",
            dbname,
        ]
        for statement in statements:
            cmd += ["-c", statement]
        return capture(cmd, env=self.subprocess_env())

    def wait_ready(self, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            ready = run(
                [self.bin("pg_isready"), "-h", self.host, "-p", str(self.port)],
                check=False,
                stdout=subprocess.DEVNULL,
                env=self.subprocess_env(),
            )
            if ready.returncode == 0:
                return
            time.sleep(0.5)
        raise TimeoutError("PostgreSQL did not become ready")

    def assert_no_crash(self):
        if self.log_path.exists():
            log = self.log_path.read_text()
            assert "was terminated by signal" not in log, (
                "a backend crashed during the e2e run, see " + str(self.log_path)
            )


class ExternalPgCluster(PgCluster):
    """Targets an already-running PostgreSQL (e.g. the pg_ducklake docker
    container) instead of initdb-ing a throwaway cluster. Selected when
    E2E_PG_HOST is set; reads E2E_PG_HOST/E2E_PG_PORT and PGPASSWORD. psql/
    pg_isready are taken from PATH (no PG_CONFIG needed on the runner)."""

    def __init__(self):
        self.host = os.environ.get("E2E_PG_HOST", PGHOST)
        self.port = int(os.environ.get("E2E_PG_PORT", "5432"))
        self.log_path = None

    def bin(self, name):
        return name  # from PATH

    def initdb(self):
        pass

    def start(self):
        pass

    def stop(self):
        pass

    def assert_no_crash(self):
        pass


@pytest.fixture(scope="session")
def cluster(tmp_path_factory):
    if os.environ.get("E2E_PG_HOST"):
        pg = ExternalPgCluster()
        pg.wait_ready()
        yield pg
        return
    pg = PgCluster(tmp_path_factory.mktemp("pgdata"))
    pg.initdb()
    pg.start()
    yield pg
    pg.stop()
    pg.port_lock.release()
    pg.assert_no_crash()


@pytest.fixture
def db(cluster):
    """A fresh database with pg_ducklake installed, dropped afterwards."""
    name = f"e2e_{uuid.uuid4().hex[:10]}"
    cluster.wait_ready()
    cluster.psql("postgres", f"CREATE DATABASE {name}")
    cluster.psql(name, "CREATE EXTENSION pg_ducklake")
    yield name
    run(
        [
            cluster.bin("psql"),
            "-X",
            "-h",
            cluster.host,
            "-p",
            str(cluster.port),
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-c",
            f"DROP DATABASE {name} WITH (FORCE)",
        ],
        check=False,
        env=cluster.subprocess_env(),
    )


# ---------------------------------------------------------------------------
# MinIO (S3-compatible) infrastructure


@dataclass
class S3Server:
    endpoint: str  # host:port, no scheme
    access_key: str
    secret_key: str


@dataclass
class S3:
    server: S3Server
    bucket: str
    client: object = field(repr=False)  # minio.Minio

    def secret_sql(self, name="e2e_minio"):
        """DuckDB CREATE SECRET statement, usable both inside the
        pg_ducklake backend (via ducklake.duckdb_raw_query) and in an
        external duckdb client."""
        s = self.server
        return (
            f"CREATE OR REPLACE SECRET {name} ("
            f"TYPE S3, KEY_ID '{s.access_key}', SECRET '{s.secret_key}', "
            f"ENDPOINT '{s.endpoint}', URL_STYLE 'path', USE_SSL false, "
            f"REGION 'us-east-1')"
        )

    def object_names(self):
        return [
            o.object_name for o in self.client.list_objects(self.bucket, recursive=True)
        ]


def _docker_available():
    if shutil.which("docker") is None:
        return False
    try:
        run(["docker", "info"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            timeout=15)
        return True
    except Exception:
        return False


def _wait_for_minio(endpoint, timeout=60):
    deadline = time.time() + timeout
    url = f"http://{endpoint}/minio/health/live"
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        time.sleep(0.5)
    raise TimeoutError(f"MinIO at {endpoint} did not become healthy")


@pytest.fixture(scope="session")
def minio_server(request):
    """An S3Server, or None (with a reason in `minio_server_skip_reason`)
    when no S3 endpoint can be provided."""
    if os.environ.get("MINIO_ENDPOINT"):
        server = S3Server(
            endpoint=os.environ["MINIO_ENDPOINT"],
            access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        )
        _wait_for_minio(server.endpoint)
        return server

    if not _docker_available():
        request.session.minio_skip_reason = "docker is not available for MinIO"
        return None

    if os.environ.get("E2E_MINIO_IMAGE"):
        images = [os.environ["E2E_MINIO_IMAGE"]]
    else:
        # quay.io mirrors docker hub's minio/minio; try both since either
        # registry may be unreachable from a given network
        images = ["minio/minio:latest", "quay.io/minio/minio:latest"]
    cid = None
    errors = []
    for image in images:
        try:
            cid = capture(
                [
                    "docker", "run", "-d", "--rm",
                    "-e", "MINIO_ROOT_USER=minioadmin",
                    "-e", "MINIO_ROOT_PASSWORD=minioadmin",
                    # random host port, loopback only
                    "-p", "127.0.0.1:0:9000",
                    image, "server", "/data",
                ],
                timeout=300,  # may pull the image
                stderr=subprocess.PIPE,
            ).strip()
            break
        except subprocess.CalledProcessError as e:
            errors.append(f"{image}: {e.stderr.strip()}")
        except Exception as e:
            errors.append(f"{image}: {e}")
    if cid is None:
        request.session.minio_skip_reason = (
            "could not start MinIO container: " + "; ".join(errors)
        )
        return None

    request.addfinalizer(
        lambda: run(["docker", "stop", cid], check=False,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    )
    endpoint = capture(["docker", "port", cid, "9000/tcp"]).splitlines()[0].strip()
    _wait_for_minio(endpoint)
    return S3Server(endpoint=endpoint, access_key="minioadmin",
                    secret_key="minioadmin")


@pytest.fixture
def s3(minio_server, request):
    """A per-test bucket on the session MinIO server."""
    if minio_server is None:
        skip_or_fail(getattr(request.session, "minio_skip_reason", "no S3 server"))

    from minio import Minio

    client = Minio(
        minio_server.endpoint,
        access_key=minio_server.access_key,
        secret_key=minio_server.secret_key,
        secure=False,
    )
    bucket = f"e2e-{uuid.uuid4().hex[:12]}"
    client.make_bucket(bucket)
    ctx = S3(server=minio_server, bucket=bucket, client=client)
    yield ctx
    try:
        for name in ctx.object_names():
            client.remove_object(bucket, name)
        client.remove_bucket(bucket)
    except Exception:
        pass  # best-effort; the container is removed at session end anyway


@pytest.fixture
def s3_second_bucket(minio_server, request):
    """A second bucket on the same MinIO -- one endpoint secret covers both."""
    if minio_server is None:
        skip_or_fail(getattr(request.session, "minio_skip_reason", "no S3 server"))

    from minio import Minio

    client = Minio(
        minio_server.endpoint,
        access_key=minio_server.access_key,
        secret_key=minio_server.secret_key,
        secure=False,
    )
    bucket = f"e2e-alt-{uuid.uuid4().hex[:12]}"
    client.make_bucket(bucket)
    ctx = S3(server=minio_server, bucket=bucket, client=client)
    yield ctx
    try:
        for name in ctx.object_names():
            client.remove_object(bucket, name)
        client.remove_bucket(bucket)
    except Exception:
        pass


@pytest.fixture
def s3_wrong_creds(minio_server, request):
    """The right endpoint with a bad secret key: CREATE SECRET still succeeds
    (shape-only validation) but S3 access fails."""
    if minio_server is None:
        skip_or_fail(getattr(request.session, "minio_skip_reason", "no S3 server"))
    from dataclasses import replace

    return replace(minio_server, secret_key="wrong-secret-key")


# ---------------------------------------------------------------------------
# Lake: one pg_ducklake database plus its storage backend


async def set_inlining(conn, row_limit):
    """Set DuckLake's data_inlining_row_limit on an existing connection. An
    INSERT/DELETE of at most row_limit rows is stored inline in the PG metadata
    catalog; larger changes (or row_limit=0) are written as Parquet/delete files.
    Takes effect for the file direction without a recycle; enabling inline reads
    on a table requires the table to have been created while inlining was on."""
    await conn.execute(
        f"CALL ducklake.set_option('data_inlining_row_limit', {int(row_limit)})"
    )


async def active_file_counts(conn, table_name):
    """(active data-file count, active delete-file count) for a ducklake table,
    read straight from the DuckLake metadata catalog. Inline-resident rows have
    no data/delete file, so these counts confirm which storage path was used."""
    row = await conn.fetchrow(
        """
        SELECT
          (SELECT count(*) FROM ducklake.ducklake_data_file df
             WHERE df.table_id = t.table_id AND df.end_snapshot IS NULL) AS data_files,
          (SELECT count(*) FROM ducklake.ducklake_delete_file xf
             WHERE xf.table_id = t.table_id AND xf.end_snapshot IS NULL) AS delete_files
        FROM ducklake.ducklake_table t
        WHERE t.table_name = $1 AND t.end_snapshot IS NULL
        """,
        table_name,
    )
    return row["data_files"], row["delete_files"]


class Lake:
    def __init__(self, cluster, dbname, s3=None):
        self.cluster = cluster
        self.dbname = dbname
        self.s3 = s3

    @property
    def storage(self):
        return "s3" if self.s3 else "local"

    @property
    def table_path(self):
        assert self.s3
        return f"s3://{self.s3.bucket}/lake/"

    async def connect(self, configure=True, user="postgres", inlining=None, **kwargs):
        """Open an asyncpg connection to this lake's database.

        user:     role to connect as (the throwaway cluster trusts local logins).
        inlining: when not None, enable DuckLake data inlining at this row limit
                  and recycle the DuckDB instance, so tables CREATED afterwards
                  read inlined rows back (the recycle-before-create ordering is
                  load-bearing). Toggle per-statement later with set_inlining().
        """
        kwargs.setdefault("password", os.environ.get("PGPASSWORD"))
        conn = await asyncpg.connect(
            host=self.cluster.host,
            port=self.cluster.port,
            user=user,
            database=self.dbname,
            **kwargs,
        )
        if configure and self.s3:
            await self.configure_s3(conn)
        if inlining is not None:
            await set_inlining(conn, inlining)
            await conn.execute("CALL ducklake.recycle_ddb()")
            if configure and self.s3:
                await self.configure_s3(conn)  # recycle dropped the secret
        return conn

    async def configure_s3(self, conn):
        """Configure this backend's DuckDB for the MinIO bucket. Secrets are
        per-DuckDB-instance and non-persistent: rerun on every new PG
        connection and again after ducklake.recycle_ddb()."""
        # httpfs is not bundled into pg_ducklake; INSTALL downloads it on
        # first use (cached in the duckdb extension dir afterwards).
        try:
            await conn.execute(
                "SELECT ducklake.raw_query('INSTALL httpfs')"
            )
        except asyncpg.PostgresError as e:
            skip_or_fail(f"backend cannot INSTALL httpfs (offline?): {e}")
        await conn.execute("SELECT ducklake.raw_query('LOAD httpfs')")
        await conn.execute(
            f"SELECT ducklake.raw_query($e2e${self.s3.secret_sql()}$e2e$)"
        )
        await conn.execute(
            f"SET ducklake.default_table_path = '{self.table_path}'"
        )

    def psql(self, *statements):
        """Simple-protocol escape hatch (each statement via psql -c): needed
        for SETOF duckdb_row results that asyncpg's extended protocol cannot
        describe. S3 configuration is prepended when applicable."""
        if self.s3:
            statements = (
                "SELECT ducklake.raw_query('INSTALL httpfs')",
                "SELECT ducklake.raw_query('LOAD httpfs')",
                f"SELECT ducklake.raw_query($e2e${self.s3.secret_sql()}$e2e$)",
                f"SET ducklake.default_table_path = '{self.table_path}'",
            ) + statements
        return self.cluster.psql(self.dbname, *statements)

    def duckdb(self, read_only=False):
        """An external duckdb client attached to this lake's DuckLake
        catalog through the PostgreSQL metadata server."""
        con = duckdb.connect()
        try:
            for ext in ("ducklake", "postgres"):
                con.execute(f"INSTALL {ext}")
                con.execute(f"LOAD {ext}")
            if self.s3:
                con.execute("INSTALL httpfs")
                con.execute("LOAD httpfs")
        except duckdb.Error as e:
            con.close()
            skip_or_fail(f"duckdb client cannot install extensions (offline?): {e}")
        if self.s3:
            con.execute(self.s3.secret_sql("e2e_minio_client"))
        options = "METADATA_SCHEMA 'ducklake'"
        if read_only:
            options += ", READ_ONLY"
        pw = os.environ.get("PGPASSWORD")
        pw_opt = f" password={pw}" if pw else ""
        con.execute(
            f"ATTACH 'ducklake:postgres:host={self.cluster.host} "
            f"port={self.cluster.port} dbname={self.dbname} user=postgres{pw_opt}' "
            f"AS lake ({options})"
        )
        return con


@pytest.fixture(params=["local", "s3"])
def lake(request, cluster, db):
    """The same lake-backed database, parametrized over storage backends.
    The 's3' variant disables data inlining so every write genuinely performs
    S3 I/O (inlined rows would otherwise never touch the bucket)."""
    s3ctx = request.getfixturevalue("s3") if request.param == "s3" else None
    if s3ctx:
        # catalog option, persisted in metadata: applies to all backends of
        # this database; set_option only writes PG tables, no secret needed
        cluster.psql(db, "CALL ducklake.set_option('data_inlining_row_limit', 0)")
    yield Lake(cluster, db, s3ctx)
    if s3ctx:
        # canary: an s3-parametrized test that never produced a single
        # bucket object was not actually testing s3 storage
        assert s3ctx.object_names(), (
            "s3-parametrized test wrote nothing to the bucket; "
            "ducklake.default_table_path is probably not taking effect"
        )


@pytest.fixture
async def conn(lake):
    c = await lake.connect()
    try:
        yield c
    finally:
        await c.close()


@pytest.fixture(params=["local", "s3"])
def inlining_lake(request, cluster, db):
    """Like `lake` but for data-inlining tests: it does NOT force inlining off
    and has no bucket canary, so a test may legitimately keep data inline (in the
    PG catalog) and never touch S3. Enable inlining per connection with
    Lake.connect(inlining=...) and toggle per-statement with set_inlining()."""
    s3ctx = request.getfixturevalue("s3") if request.param == "s3" else None
    return Lake(cluster, db, s3ctx)
