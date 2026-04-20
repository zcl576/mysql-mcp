from __future__ import annotations

import base64
import datetime as dt
import re
from decimal import Decimal
from typing import Any, Mapping, Sequence

import pymysql
from pymysql import err as pymysql_err

from mysql_mcp.config import ConfigurationError, MySQLSettings

JsonValue = dict[str, Any] | list[Any] | str | int | float | bool | None


class MySQLMcpError(RuntimeError):
    """Base error for user-facing MySQL MCP failures."""


class StatementValidationError(MySQLMcpError):
    """Raised when a SQL statement fails local validation."""


class DatabaseSelectionError(MySQLMcpError):
    """Raised when a tool needs an explicit database but none is configured."""


def serialize_value(value: Any) -> JsonValue:
    # 把 MySQL 查询结果转换成稳定的 JSON 结构，便于 MCP 客户端消费。
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        encoded = base64.b64encode(bytes(value)).decode("ascii")
        return {"type": "bytes", "base64": encoded}
    if isinstance(value, Mapping):
        return {str(key): serialize_value(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [serialize_value(item) for item in value]
    return str(value)


def ensure_single_statement(sql: str) -> str:
    # MCP 对外只允许执行一条 SQL，避免拼接多语句带来风险。
    candidate = sql.strip()
    if not candidate:
        raise StatementValidationError("SQL cannot be empty.")

    semicolon_index = _find_top_level_semicolon(candidate)
    if semicolon_index == -1:
        return candidate

    normalized = candidate[:semicolon_index].rstrip()
    if not normalized:
        raise StatementValidationError("SQL cannot be empty.")

    remainder = candidate[semicolon_index + 1 :]
    if _has_meaningful_sql_content(remainder):
        raise StatementValidationError(
            "Only one SQL statement is allowed per request. Remove extra statements and try again."
        )
    return normalized


def normalize_params(params: Mapping[str, Any] | Sequence[Any] | None) -> Mapping[str, Any] | Sequence[Any] | None:
    # 统一把参数规范成 dict / list / None 三种形态。
    if params is None:
        return None
    if isinstance(params, Mapping):
        return dict(params)
    if isinstance(params, Sequence) and not isinstance(params, (str, bytes, bytearray)):
        return list(params)
    raise StatementValidationError("SQL params must be either an object, an array, or null.")


def detect_statement_type(sql: str) -> str:
    # 取 SQL 第一个关键字，用来判断是查询还是写入语句。
    parts = sql.lstrip().split(None, 1)
    if not parts:
        raise StatementValidationError("SQL cannot be empty.")
    return parts[0].upper()


def normalize_limit(limit: int | None, settings: MySQLSettings) -> int:
    # 所有对外的 limit 都走同一套默认值和上限校验。
    if limit is None:
        return settings.query_default_limit
    if not isinstance(limit, int):
        raise StatementValidationError("limit must be an integer when provided.")
    if limit < 1:
        raise StatementValidationError("limit must be at least 1.")
    if limit > settings.query_max_limit:
        raise StatementValidationError(
            f"limit cannot be greater than the configured maximum of {settings.query_max_limit}."
        )
    return limit


def ensure_allowed_statement(sql: str, settings: MySQLSettings) -> None:
    # 只读模式下只放行安全查询语句，避免模型误执行写操作。
    if not settings.read_only:
        return

    statement_type = detect_statement_type(sql)
    if statement_type in {"SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN"}:
        return

    raise StatementValidationError(
        "This server is running in read-only mode. Only SELECT, SHOW, DESCRIBE, DESC, and EXPLAIN are allowed."
    )


class MySQLService:
    def __init__(self, settings: MySQLSettings):
        self.settings = settings

    @classmethod
    def from_env(cls) -> "MySQLService":
        # Service 只是配置 + 行为的薄封装，实例化入口统一走环境变量。
        try:
            settings = MySQLSettings.from_env()
        except ConfigurationError as exc:
            raise MySQLMcpError(str(exc)) from exc
        return cls(settings)

    def execute_sql(
        self,
        sql: str,
        params: Mapping[str, Any] | Sequence[Any] | None = None,
        database: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        # 主执行流程：
        # 1. 校验 SQL 和参数
        # 2. 建立连接
        # 3. 执行并按“查询 / 写入”两种结果结构返回
        normalized_sql = ensure_single_statement(sql)
        normalized_params = normalize_params(params)
        ensure_allowed_statement(normalized_sql, self.settings)
        statement_type = detect_statement_type(normalized_sql)
        normalized_limit = normalize_limit(limit, self.settings)
        connection = self._connect(database)

        try:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(normalized_sql, normalized_params)
                warnings = self._fetch_warnings(connection)
                if cursor.description is not None:
                    # 有 description 说明这是一条会返回结果集的语句。
                    raw_rows = cursor.fetchmany(normalized_limit + 1)
                    has_more = len(raw_rows) > normalized_limit
                    rows = [serialize_value(row) for row in raw_rows[:normalized_limit]]
                    columns = [description[0] for description in cursor.description]
                    return {
                        "statement_type": statement_type,
                        "columns": columns,
                        "rows": rows,
                        "row_count": len(rows),
                        "limit_applied": normalized_limit,
                        "has_more": has_more,
                        "warnings": warnings,
                    }

                connection.commit()
                # 没有结果集时按写操作结构返回影响行数和自增 ID。
                return {
                    "statement_type": statement_type,
                    "affected_rows": cursor.rowcount,
                    "last_insert_id": connection.insert_id(),
                    "warnings": warnings,
                }
        except pymysql.MySQLError as exc:
            self._rollback_quietly(connection)
            raise self._translate_mysql_error(exc) from exc
        finally:
            connection.close()

    def list_tables(self, database: str | None = None) -> dict[str, Any]:
        # 通过 information_schema 取表列表，比让模型手写 SQL 更稳定。
        resolved_database = self._require_database(database)
        result = self.execute_sql(
            """
            SELECT TABLE_NAME AS table_name
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s
            ORDER BY TABLE_NAME
            """,
            params=[resolved_database],
            database=resolved_database,
            limit=self.settings.query_max_limit,
        )
        tables = [row["table_name"] for row in result["rows"]]
        return {
            "database": resolved_database,
            "tables": tables,
            "count": len(tables),
        }

    def list_databases(self) -> dict[str, Any]:
        # 返回当前账号可见的数据库列表。
        result = self.execute_sql(
            """
            SELECT SCHEMA_NAME AS database_name
            FROM INFORMATION_SCHEMA.SCHEMATA
            ORDER BY SCHEMA_NAME
            """,
            database=None,
            limit=self.settings.query_max_limit,
        )
        databases = [row["database_name"] for row in result["rows"]]
        return {
            "databases": databases,
            "count": len(databases),
        }

    def describe_table(self, table_name: str, database: str | None = None) -> dict[str, Any]:
        # 返回字段名、类型、是否可空等信息，适合作为建表结构查看工具。
        normalized_table_name = table_name.strip()
        if not normalized_table_name:
            raise StatementValidationError("table_name cannot be empty.")

        resolved_database = self._require_database(database)
        result = self.execute_sql(
            """
            SELECT
                COLUMN_NAME AS column_name,
                COLUMN_TYPE AS column_type,
                IS_NULLABLE AS is_nullable,
                COLUMN_DEFAULT AS column_default,
                COLUMN_KEY AS column_key,
                EXTRA AS extra
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
            """,
            params=[resolved_database, normalized_table_name],
            database=resolved_database,
            limit=self.settings.query_max_limit,
        )
        columns = result["rows"]
        if not columns:
            raise MySQLMcpError(
                f"Table '{normalized_table_name}' was not found in database '{resolved_database}'."
            )
        return {
            "database": resolved_database,
            "table_name": normalized_table_name,
            "columns": columns,
            "column_count": len(columns),
        }

    def preview_table(
        self,
        table_name: str,
        database: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        # 给模型一个“安全预览表数据”的入口，避免每次自己拼 SELECT *。
        normalized_table_name = table_name.strip()
        if not normalized_table_name:
            raise StatementValidationError("table_name cannot be empty.")

        resolved_database = self._require_database(database)
        normalized_limit = normalize_limit(limit, self.settings)
        qualified_table = f"{_quote_identifier(resolved_database)}.{_quote_identifier(normalized_table_name)}"
        return self.execute_sql(
            f"SELECT * FROM {qualified_table} LIMIT %s",
            params=[normalized_limit],
            database=resolved_database,
            limit=normalized_limit,
        )

    def explain_sql(
        self,
        sql: str,
        params: Mapping[str, Any] | Sequence[Any] | None = None,
        database: str | None = None,
    ) -> dict[str, Any]:
        # 用 EXPLAIN 查看执行计划，只接受查询类语句。
        normalized_sql = ensure_single_statement(sql)
        ensure_allowed_statement(normalized_sql, self.settings)
        statement_type = detect_statement_type(normalized_sql)
        if statement_type not in {"SELECT", "SHOW", "DESCRIBE", "DESC"}:
            raise StatementValidationError(
                "explain_sql only supports SELECT, SHOW, DESCRIBE, and DESC statements."
            )
        return self.execute_sql(
            f"EXPLAIN {normalized_sql}",
            params=params,
            database=database,
            limit=self.settings.query_max_limit,
        )

    def _connect(self, database: str | None = None) -> pymysql.connections.Connection:
        # 底层连接异常统一翻译成更友好的业务错误。
        try:
            return pymysql.connect(**self.settings.connection_kwargs(database))
        except pymysql.MySQLError as exc:
            raise self._translate_mysql_error(exc) from exc

    def _require_database(self, database: str | None) -> str:
        # 某些工具必须明确落到某个数据库上，这里统一兜底校验。
        resolved = database.strip() if database else self.settings.database
        if not resolved:
            raise DatabaseSelectionError(
                "This tool requires a database. Set MYSQL_DATABASE or pass the database argument explicitly."
            )
        return resolved

    @staticmethod
    def _fetch_warnings(connection: pymysql.connections.Connection) -> list[dict[str, JsonValue]]:
        # MySQL 的 warning 结构不完全稳定，这里统一序列化成字典列表。
        try:
            raw_warnings = connection.show_warnings() or []
        except pymysql.MySQLError:
            return []

        warnings: list[dict[str, JsonValue]] = []
        for item in raw_warnings:
            if isinstance(item, Mapping):
                warnings.append({str(key): serialize_value(value) for key, value in item.items()})
                continue
            if isinstance(item, Sequence) and not isinstance(item, (str, bytes, bytearray)):
                level = item[0] if len(item) > 0 else None
                code = item[1] if len(item) > 1 else None
                message = item[2] if len(item) > 2 else None
                warnings.append(
                    {
                        "level": serialize_value(level),
                        "code": serialize_value(code),
                        "message": serialize_value(message),
                    }
                )
                continue
            warnings.append({"message": serialize_value(item)})
        return warnings

    @staticmethod
    def _rollback_quietly(connection: pymysql.connections.Connection) -> None:
        # 写操作失败时尽量回滚，但不让回滚本身的异常覆盖原始错误。
        try:
            connection.rollback()
        except Exception:
            return

    @staticmethod
    def _translate_mysql_error(exc: pymysql.MySQLError) -> MySQLMcpError:
        # 把底层数据库错误码映射成更容易理解的提示。
        code = exc.args[0] if exc.args else None
        detail = exc.args[1] if len(exc.args) > 1 else str(exc)

        if code == 1045:
            return MySQLMcpError("Authentication failed. Check MYSQL_USER and MYSQL_PASSWORD.")
        if code in {1049}:
            return MySQLMcpError(f"Unknown database: {detail}")
        if code in {1064}:
            return MySQLMcpError(f"SQL syntax error: {detail}")
        if code in {1146}:
            return MySQLMcpError(f"Table not found: {detail}")
        if code in {1044, 1142, 1143, 1227}:
            return MySQLMcpError(f"Permission denied by MySQL: {detail}")
        if code in {2002, 2003, 2005, 2006, 2013}:
            return MySQLMcpError(f"Could not connect to MySQL: {detail}")

        if isinstance(exc, pymysql_err.OperationalError):
            return MySQLMcpError(f"MySQL operational error: {detail}")
        if isinstance(exc, pymysql_err.ProgrammingError):
            return MySQLMcpError(f"MySQL programming error: {detail}")
        if isinstance(exc, pymysql_err.IntegrityError):
            return MySQLMcpError(f"MySQL integrity error: {detail}")
        return MySQLMcpError(f"MySQL error: {detail}")


def _find_top_level_semicolon(sql: str) -> int:
    # 在不进入字符串 / 注释的前提下，寻找真正表示“多语句”的分号。
    in_single = False
    in_double = False
    in_backtick = False
    in_line_comment = False
    in_block_comment = False
    index = 0

    while index < len(sql):
        current = sql[index]
        next_char = sql[index + 1] if index + 1 < len(sql) else ""

        if in_line_comment:
            if current == "\n":
                in_line_comment = False
            index += 1
            continue

        if in_block_comment:
            if current == "*" and next_char == "/":
                in_block_comment = False
                index += 2
                continue
            index += 1
            continue

        if in_single:
            if current == "\\":
                index += 2
                continue
            if current == "'":
                in_single = False
            index += 1
            continue

        if in_double:
            if current == "\\":
                index += 2
                continue
            if current == '"':
                in_double = False
            index += 1
            continue

        if in_backtick:
            if current == "`" and next_char == "`":
                index += 2
                continue
            if current == "`":
                in_backtick = False
            index += 1
            continue

        if current == "-" and next_char == "-" and _line_comment_start(sql, index):
            in_line_comment = True
            index += 2
            continue
        if current == "#":
            in_line_comment = True
            index += 1
            continue
        if current == "/" and next_char == "*":
            in_block_comment = True
            index += 2
            continue
        if current == "'":
            in_single = True
            index += 1
            continue
        if current == '"':
            in_double = True
            index += 1
            continue
        if current == "`":
            in_backtick = True
            index += 1
            continue
        if current == ";":
            return index
        index += 1

    return -1


def _has_meaningful_sql_content(fragment: str) -> bool:
    # 用来判断分号后面是否还有真正的 SQL 内容，而不只是空白或注释。
    in_line_comment = False
    in_block_comment = False
    index = 0

    while index < len(fragment):
        current = fragment[index]
        next_char = fragment[index + 1] if index + 1 < len(fragment) else ""

        if in_line_comment:
            if current == "\n":
                in_line_comment = False
            index += 1
            continue

        if in_block_comment:
            if current == "*" and next_char == "/":
                in_block_comment = False
                index += 2
                continue
            index += 1
            continue

        if current.isspace():
            index += 1
            continue
        if current == "-" and next_char == "-" and _line_comment_start(fragment, index):
            in_line_comment = True
            index += 2
            continue
        if current == "#":
            in_line_comment = True
            index += 1
            continue
        if current == "/" and next_char == "*":
            in_block_comment = True
            index += 2
            continue
        return True

    return False


def _line_comment_start(sql: str, index: int) -> bool:
    # MySQL 的 -- 注释要求前后是空白边界，这里单独抽出来判断。
    previous_char = sql[index - 1] if index > 0 else ""
    following_char = sql[index + 2] if index + 2 < len(sql) else ""
    previous_ok = not previous_char or previous_char.isspace()
    following_ok = not following_char or following_char.isspace()
    return previous_ok and following_ok


def _quote_identifier(identifier: str) -> str:
    # 预览表数据时不能直接拼裸字符串，这里先做标识符白名单校验再加反引号。
    normalized = identifier.strip()
    if not normalized:
        raise StatementValidationError("Identifier cannot be empty.")
    if not re.fullmatch(r"[0-9A-Za-z_$]+", normalized):
        raise StatementValidationError(
            "Identifiers may only contain letters, numbers, underscore, and dollar sign."
        )
    return f"`{normalized}`"
