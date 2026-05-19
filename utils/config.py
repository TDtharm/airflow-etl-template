from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # -- Postgres --
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "postgres"
    postgres_user: str = "postgres"
    postgres_password: str = ""

    # -- MSSQL --
    mssql_host: str = "localhost"
    mssql_port: int = 1433
    mssql_db: str = "master"
    mssql_user: str = "sa"
    mssql_password: str = ""

    # -- Qdrant --
    qdrant_host: str = "localhost"
    qdrant_port: int = 6333
    qdrant_api_key: str | None = None

    # -- Impala --
    impala_host: str = "localhost"
    impala_port: int = 21050
    impala_db: str = "default"
    impala_user: str = ""
    impala_password: str = ""
    impala_auth_mechanism: str = "PLAIN"  # PLAIN, LDAP, GSSAPI
    impala_use_ssl: bool = False
    impala_kerberos_service_name: str = "impala"
    impala_ca_cert: str | None = None

    # -- HDFS --
    hdfs_url: str = "http://localhost:9870"
    hdfs_user: str = "hdfs"
    hdfs_auth_mechanism: str = "PLAIN"  # PLAIN, LDAP, GSSAPI
    hdfs_password: str = ""
    hdfs_kerberos_principal: str | None = None
    hdfs_root: str = "/"

    # -- MinIO --
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_secure: bool = False

    # -- NATS --
    nats_servers: str = "nats://localhost:4222"
    nats_user: str | None = None
    nats_password: str | None = None
