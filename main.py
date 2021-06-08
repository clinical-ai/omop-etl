from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

import psycopg2
import typer
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from pydantic.error_wrappers import ErrorWrapper
from tqdm import tqdm

from schema import REQUIRED_FIELDS, DisabledColumn, TargetTable

api = FastAPI()


@dataclass(eq=True, frozen=True)
class Result:
    script: str
    warnings: List[dict]


def check_columns(table: TargetTable) -> List[ErrorWrapper]:
    req_cols = REQUIRED_FIELDS.get_fields(table.name)
    req_cols = req_cols.difference({table.primary_key.name})
    cols = set()
    cols = {c.name for c in table.columns if not isinstance(c, DisabledColumn)}
    cols.add(table.primary_key.name)

    return [
        ErrorWrapper(
            ValueError(f'Column "{c}" is not defined'), tuple(["body", "columns"])
        )
        for c in req_cols
        if c not in cols
    ]


def table_warnings(table: TargetTable) -> List[dict]:
    col_warnings = check_columns(table)
    warnings = [*col_warnings]
    if warnings:
        return RequestValidationError(warnings).errors()
    return list()


@api.post("/api/translate")
def translate_table(table: TargetTable) -> Result:
    return Result(script=table.get_script(), warnings=table_warnings(table))


app = typer.Typer()


def load_rules(rules: Path) -> List[Tuple[str, TargetTable]]:
    tables = list()
    for fn in rules.iterdir():
        with fn.open() as f:
            try:
                name = ".".join(fn.name.split(".")[:-1])
                tables.append((name, TargetTable.parse_string(f.read())))
            except Exception as ex:
                print(f"Failed to processe: {fn}")
    return tables


@app.command()
def compile(
    rules: Path = typer.Option("rules", file_okay=False, dir_okay=True, readable=True,),
    output: Path = typer.Option(
        "sql", file_okay=False, dir_okay=True, writable=True, readable=True,
    ),
):
    if not output.exists():
        output.mkdir()
    for name, table in load_rules(rules):
        out_fn = output / f"{name}.sql"
        with out_fn.open("w") as f:
            f.write(table.get_script())


@app.command()
def execute(
    rules: Path = typer.Option("rules", file_okay=False, dir_okay=True, readable=True,),
    database: str = "postgres",
    password: str = "password",
    host: str = "127.0.0.1",
    user: str = "postgres",
    port: int = 5432,
):

    conn = psycopg2.connect(
        database=database,
        password=password,
        host=host,
        port=port,
        user=user,
        sslmode="disable",
        gssencmode="disable",
    )

    cur = conn.cursor()

    rules_iter = load_rules(rules)
    rules_iter = tqdm(rules_iter, desc="Tables")
    cur.execute("SET search_path TO cerner;")
    for _, table in rules_iter:
        if table.name in {"location"}:
            continue
        rules_iter.set_postfix(table=table.name, stage="CREATE")
        stmt = table.primary_key.create_pk_table(table.name)
        cur.execute(stmt)
        rules_iter.set_postfix(table=table.name, stage="INSERT")
        for stmt in table.get_insert_statements():
            cur.execute(stmt)
            pass
        conn.commit()
        rules_iter.set_postfix(table=table.name, stage="UPDATE")
        cols_iter = [col for col in table.columns if col.enabled]
        cols_iter = tqdm(cols_iter, desc="Columns")
        for col in cols_iter:
            stmt = col.get_update_statement(table.name, table.primary_key)
            cols_iter.set_postfix(column=col.name)
            try:
                cur.execute(stmt)
            except Exception as ex:
                rules_iter.write(f"Table: {table.name} Column '{col.name}' failed")
                rules_iter.write(str(ex))
                raise ex
    conn.commit()


if __name__ == "__main__":
    app()
