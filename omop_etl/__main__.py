from collections import defaultdict
from email.policy import default
from pathlib import Path
from typing import List, Tuple

import psycopg2
from pydantic import ValidationError
import typer
from tqdm import tqdm

from omop_etl.schema import REQUIRED_FIELDS, Dependency, DisabledColumn, TargetTable


app = typer.Typer()


def load_rules(rules: Path) -> List[Tuple[str, TargetTable]]:
    tables = list()
    for fn in rules.iterdir():
        with fn.open() as f:
            try:
                s = f.read()
                name = ".".join(fn.name.split(".")[:-1])
                tables.append((name, TargetTable.parse_string(s)))
            except ValidationError as ex:
                name = ".".join(fn.name.split(".")[:-1])
                tables.append((name, Dependency.parse_string(s)))
            except Exception as ex:
                raise ex
                print(f"Failed to processe: {fn}")
    return tables


@app.command()
def compile(
    rules: Path = typer.Option("rules", file_okay=False, dir_okay=True, readable=True,),
    output: Path = typer.Option(
        "sql", file_okay=False, dir_okay=True, writable=True, readable=True,
    ),
    one_file: bool = True,
    drop_tables: bool = False,
):
    if not output.exists():
        output.mkdir()
    if not one_file:
        for name, table in load_rules(rules):
            out_fn = output / f"{name}.sql"
            with out_fn.open("w") as f:
                f.write(table.get_script())
    else:
        script = ""
        to_process = list()

        tables = load_rules(rules)

        deps = filter(lambda x: not isinstance(x[-1], TargetTable), tables)
        tables = filter(lambda x: isinstance(x[-1], TargetTable), tables)
        envs = dict()
        for name, table in deps:
            print(name)
            init, env = table.translate()
            init = [stmt.to_sql() for stmt in init]
            init = "\n".join(init)
            script += f"{init}\n"
            envs[name] = env

        for name, table in tables:
            # print(name)
            env = table.default_env
            env["DropTables"] = drop_tables
            if table.depends_on is not None:
                for dep in table.depends_on:
                    if dep in envs:
                        schema = envs[dep]["DefaultSchema"]
                        if schema is not None:
                            env["DefaultSchema"] = schema
                        env["TempTables"] = {
                            *env["TempTables"],
                            *envs[dep]["TempTables"],
                        }
            init, env = table.get_initialization(env)
            to_process.append((table, env))
            script += f"{init}\n"

        for table, env in to_process:
            table: TargetTable = table
            script += table.get_script(env=env, include_initialization=False)
            script += "\n"

        out_fn = output / "etl.sql"
        with out_fn.open("w") as f:
            f.write(script)


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
