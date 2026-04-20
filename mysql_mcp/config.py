from __future__ import annotations

import os
from dataclasses import dataclass


class ConfigurationError(ValueError):
    """Raised when required MySQL configuration is missing or invalid."""


@dataclass(frozen=True, slots=True)
class MySQLSettings:
    # 这里集中保存运行 MCP 所需的数据库配置。
    host: str
    port: int
    user: str
    password: str
    database: str | None
    charset: str
    connect_timeout: int
    read_only: bool
    query_default_limit: int
    query_max_limit: int

    @classmethod
    def from_env(cls) -> "MySQLSettings":
        # 所有配置统一从环境变量读取，避免把敏感信息写死在代码里。
        host = _get_required_env("MYSQL_HOST")
        user = _get_required_env("MYSQL_USER")
        password = _get_required_env("MYSQL_PASSWORD")
        port = _get_int_env("MYSQL_PORT", default=3306, minimum=1)
        connect_timeout = _get_int_env("MYSQL_CONNECT_TIMEOUT", default=10, minimum=1)
        read_only = _get_bool_env("MYSQL_READ_ONLY", default=True)
        query_default_limit = _get_int_env("MYSQL_QUERY_LIMIT_DEFAULT", default=10, minimum=1)
        query_max_limit = _get_int_env("MYSQL_QUERY_LIMIT_MAX", default=50, minimum=1)
        if query_max_limit > 50:
            raise ConfigurationError("Environment variable MYSQL_QUERY_LIMIT_MAX cannot be greater than 50.")
        if query_default_limit > query_max_limit:
            raise ConfigurationError(
                "Environment variable MYSQL_QUERY_LIMIT_DEFAULT cannot be greater than MYSQL_QUERY_LIMIT_MAX."
            )
        charset = os.getenv("MYSQL_CHARSET", "utf8mb4").strip() or "utf8mb4"
        database = _get_optional_env("MYSQL_DATABASE")
        return cls(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset=charset,
            connect_timeout=connect_timeout,
            read_only=read_only,
            query_default_limit=query_default_limit,
            query_max_limit=query_max_limit,
        )

    def connection_kwargs(self, database: str | None = None) -> dict[str, object]:
        # 统一生成 pymysql.connect 所需参数，业务代码就不用到处拼装连接配置。
        selected_database = database if database is not None else self.database
        kwargs: dict[str, object] = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "charset": self.charset,
            "connect_timeout": self.connect_timeout,
            "autocommit": False,
        }
        if selected_database:
            kwargs["database"] = selected_database
        return kwargs


def _get_required_env(name: str) -> str:
    # 必填配置缺失时直接报错，避免连库时才出现更模糊的问题。
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigurationError(f"Missing required environment variable: {name}")
    return value


def _get_optional_env(name: str) -> str | None:
    value = os.getenv(name, "").strip()
    return value or None


def _get_int_env(name: str, *, default: int, minimum: int) -> int:
    # 统一做整数配置解析和下限校验。
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"Environment variable {name} must be an integer.") from exc
    if value < minimum:
        raise ConfigurationError(f"Environment variable {name} must be at least {minimum}.")
    return value


def _get_bool_env(name: str, *, default: bool) -> bool:
    # 允许常见的 true/false、1/0 写法，方便不同环境配置。
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default

    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigurationError(f"Environment variable {name} must be a boolean.")
