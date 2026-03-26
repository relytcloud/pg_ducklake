### Install duckdb CLI, using brew
```bash
brew install duckdb
```
These instructions have been tested with DuckDB v1.3.0.


### Install required third-party tools, using brew

```bash
brew install minio/stable/minio
brew install minio/stable/mc
```
Those are provided by minio, please consult their docs at https://min.io/docs/minio/kubernetes/upstream/index.html and their term of usage.

Minio is a local implementation of the S3 protocol, that allow both to mock other S3 services and be run as alternative infrastructure.


### Checking current versions / permissions of tools

```bash
$ duckdb --version
v1.3.0 71c5c07cdd

$ minio -version       
minio version RELEASE.2025-04-22T22-12-26Z (commit-id=0d7408fc9969caf07de6a8c3a84f9fbb10a6739e)
Runtime: go1.24.2 darwin/arm64
License: GNU AGPLv3 - https://www.gnu.org/licenses/agpl-3.0.html
Copyright: 2015-2025 MinIO, Inc.

$ mc --version
mc version RELEASE.2025-04-16T18-13-26Z (commit-id=b00526b153a31b36767991a4f5ce2cced435ee8e)
Runtime: go1.24.2 darwin/arm64
Copyright (c) 2015-2025 MinIO, Inc.
License GNU AGPLv3 <https://www.gnu.org/licenses/agpl-3.0.html>
```

### Minio/1: Start Minio server

```bash
mkdir -p path/to/some_folder
minio server path/to/some_folder
```
Note this will start a Web server, and populate the relevant folder with all data and metadata needed to host the local S3 buckets

This might fail if the port is already in use, you can customize the port passing `--address ':NUMBER'` to `minio server`.

Take note of the HTTP endpoint, port and passwords.
In the following blocks I will use: '10.1.0.202:9000', 'minioadmin', 'minioadmin'. Change as necessary.


### Minio/2: Set alias and create bucket

Move to a new tab, and do:
```bash
mc alias set 'myminio' 'http://10.1.0.202:9000' 'minioadmin' 'minioadmin'
mc mb myminio/demo-ducklake-minio-bucket
```
Endpoint and passwords have to be taken from previous step

### DuckDB
Start DuckDb
```bash
duckdb
```

Enter the following SQL statements:
```sql
--- Setup relevant DuckDB temporary secret to gain access to the local MinIO S3 bucket
create secret (type s3, key_id 'minioadmin', secret 'minioadmin', endpoint '10.1.0.202:9000', use_ssl false, url_style 'path');

--- Attach a local DuckDB file (minio-ducklake-demo.ducklake) and a local (but using S3 protocol) bucket
ATTACH 'ducklake:minio-ducklake-demo.ducklake' as db (DATA_PATH 's3://demo-ducklake-minio-bucket');

--- Use the just attached DuckLake as default Database
USE db;

--- Create a table with some data (in the ducklake)
CREATE TABLE numbers AS (SELECT random() FROM range(100000));

--- Check which files are in the bucket
FROM glob('s3://demo-ducklake-minio-bucket/**');

--- This will show both minio-ducklake-demo.ducklake (and it's WAL file) and the minio relevant files, just to check it's actually local data
FROM glob('**');

--- Remove some data
DELETE FROM numbers WHERE #1 < 0.1;

--- Check which files are in the bucket, now there should also be a delete file
FROM glob('s3://demo-ducklake-minio-bucket/**');


--- Now you have a fully local DuckLake, backed by a local DuckDB Database and a local S3-bucket (via Minio)
```

#### Cleanup
To stop the Minio server, find the tab where you started `minio server` and kill the process.
To clear data, just remove all created files and folders.
