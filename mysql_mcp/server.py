from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import FastMCP

from mysql_mcp.core import MySQLService

# FastMCP 负责把普通 Python 函数暴露成 MCP 工具。
mcp = FastMCP(
    "MySQL Query MCP",
    instructions=(
        "Execute single-statement SQL queries against a MySQL database configured through "
        "environment variables. Use list_tables and describe_table for schema discovery."
    ),
    json_response=True,
)


def _service() -> MySQLService:
    # 每次工具调用时按当前环境配置创建 service。
    return MySQLService.from_env()


@mcp.tool()
def execute_sql(
    sql: str,
    params: dict[str, Any] | list[Any] | None = None,
    database: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Execute one SQL statement against MySQL."""

    # 自由 SQL 执行入口，适合复杂查询。
    return _service().execute_sql(sql=sql, params=params, database=database, limit=limit)


@mcp.tool()
def list_tables(database: str | None = None) -> dict[str, Any]:
    """List tables for the configured database or an explicitly provided one."""

    # 结构化工具优先于自由 SQL，更适合让模型稳定调用。
    return _service().list_tables(database=database)


@mcp.tool()
def list_databases() -> dict[str, Any]:
    """List databases visible to the configured MySQL user."""

    return _service().list_databases()


@mcp.tool()
def describe_table(table_name: str, database: str | None = None) -> dict[str, Any]:
    """Describe a table using information_schema metadata."""

    return _service().describe_table(table_name=table_name, database=database)


@mcp.tool()
def preview_table(
    table_name: str,
    database: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Preview rows from a table using a safe SELECT * ... LIMIT query."""

    return _service().preview_table(table_name=table_name, database=database, limit=limit)


@mcp.tool()
def explain_sql(
    sql: str,
    params: dict[str, Any] | list[Any] | None = None,
    database: str | None = None,
) -> dict[str, Any]:
    """Run EXPLAIN for a read-only SQL statement."""

    return _service().explain_sql(sql=sql, params=params, database=database)


def main() -> None:
    # 这里使用 stdio 传输，适合本地 MCP 客户端直接拉起子进程接入。
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
