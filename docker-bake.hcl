variable "REPO" {
  default = "pgducklake/pgducklake"
}

variable "POSTGRES_VERSION" {
  default = "16"
}

target "shared" {
  platforms = [
    "linux/amd64",
    "linux/arm64"
  ]
}

target "postgres" {
  inherits = ["shared"]

  contexts = {
    postgres_base = "docker-image://postgres:${POSTGRES_VERSION}-bookworm"
  }

  args = {
    POSTGRES_VERSION = "${POSTGRES_VERSION}"
  }

  tags = [
    "${REPO}:${POSTGRES_VERSION}-dev",
  ]
}

target "pg_ducklake" {
  inherits = ["postgres"]
  target = "output"
}

target "pg_ducklake_14" {
  inherits = ["pg_ducklake"]

  args = {
    POSTGRES_VERSION = "14"
  }
}

target "pg_ducklake_15" {
  inherits = ["pg_ducklake"]

  args = {
    POSTGRES_VERSION = "15"
  }
}

target "pg_ducklake_16" {
  inherits = ["pg_ducklake"]

  args = {
    POSTGRES_VERSION = "16"
  }
}

target "pg_ducklake_17" {
  inherits = ["pg_ducklake"]

  args = {
    POSTGRES_VERSION = "17"
  }
}

target "pg_ducklake_18" {
  inherits = ["pg_ducklake"]

  args = {
    POSTGRES_VERSION = "18"
  }
}

target "default" {
  inherits = ["pg_ducklake_18"]
}
