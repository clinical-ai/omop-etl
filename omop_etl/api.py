from dataclasses import dataclass
from typing import List

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from pydantic.error_wrappers import ErrorWrapper

from omop_etl.schema import REQUIRED_FIELDS, DisabledColumn, TargetTable

app = FastAPI()


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


@app.post("/api/translate")
def translate_table(table: TargetTable) -> Result:
    return Result(script=table.get_script(), warnings=table_warnings(table))

