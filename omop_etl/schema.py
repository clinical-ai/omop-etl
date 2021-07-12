import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type, TypeVar, Union

import pandas as pd
import pydantic
import yaml
from fastapi.params import Query
from pydantic import Field, root_validator, validator

from omop_etl.generation import *

C = TypeVar("C", bound="BaseModel")

Environment = Dict[str, Any]
TranslateResponse = Tuple[List[Serializable], Environment]


def parse_table(table, default_schema="cerner") -> Union[Table, None]:
    if isinstance(table, Query):
        return QueryTable(alias=table.alias, query=table.query)
    elif isinstance(table, str):
        if re.fullmatch("\\w+", table):
            return Table(table, default_schema)
        elif re.fullmatch("\\w+\\.\\w+", table):
            schema, table = table.split(".")
            return Table(table, schema)


class Translatable(ABC):
    @abstractmethod
    def translate(self, env: Environment) -> TranslateResponse:
        raise NotImplementedError


class BaseModel(pydantic.BaseModel):
    @classmethod
    def parse_string(cls: Type[C], s) -> C:
        data = yaml.load(s, Loader=yaml.FullLoader)
        return cls.parse_obj(data)


class RequiredFields:
    def __init__(self) -> None:
        self.df = pd.read_csv(Path("schema", "required_omop_columns.csv"))

    def get_fields(self, name):
        return set(self.df[self.df.table == name.lower()]["column"])


REQUIRED_FIELDS = RequiredFields()


class BaseColumn(BaseModel):
    name: str
    enabled = True


class Query(BaseModel):
    alias: str
    query: str

    def translate(self, env: Environment) -> TranslateResponse:
        return [QueryTable(alias=self.alias, query=self.query)], env


class TableReference(BaseModel, Translatable):
    alias: str = Field(alias="alias")
    table_schema: Optional[str] = Field(alias="schema")

    @staticmethod
    def from_str(table) -> None:

        if re.fullmatch("\\w+", table):
            alias = table
            schema = None

        elif re.fullmatch("\\w+\\.\\w+", table):
            schema, alias = table.split(".")

        return TableReference(alias=alias, schema=schema)

    def translate(self, env: Environment) -> TranslateResponse:
        schema = env["DefaultSchema"]

        if self.table_schema is not None:
            schema = self.table_schema
        table = Table(self.alias, schema)

        return [table], env

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, TableReference):
            return self.alias == other.alias and self.table_schema == other.table_schema
        return False


class ConstantTargetColumn(BaseColumn, Translatable):
    constant: Union[str, int, float]

    def translate(self, env: Environment):
        assert "TargetTable" in env
        target_table = env["TargetTable"]
        if not self.enabled:
            return list(), env

        target_table = Table(target_table, "omop")
        col = Column(self.name, target_table)
        if isinstance(self.constant, str):
            const = f"'{self.constant}'"
        else:
            const = self.constant
        return [UpdateStatement(col, Expression(const))], env


class PrimaryKeySource(BaseColumn, Translatable):
    table: Union[Query, TableReference, str]
    columns: Dict[str, str]
    constraints: List[str] = tuple()

    @validator("table", pre=True)
    def check_add_default_primary_key(cls, source, values, **kwargs):
        if isinstance(source, str):
            return TableReference.from_str(source)
        return source

    def translate(self, env: Environment) -> TranslateResponse:
        assert "TargetTable" in env
        target_table = env["TargetTable"]
        tables, _ = self.table.translate(env)
        table_ref = tables[0].alias
        cols = self.columns.items()
        pk_cols = tuple([f"{table_ref}_{c}" for c, _ in cols])
        select_cols = tuple(
            [Expression(f"{table_ref}.{c} as {table_ref}_{c}") for c, _ in cols]
        )
        crit = (
            Criterion([Expression(s) for s in self.constraints])
            if len(self.constraints) > 0
            else None
        )

        select = SelectStatement(expressions=select_cols, source=tables, criterion=crit)
        stmts = [InsertFromStatement(pk_cols, Table(target_table, "mapping"), select)]
        return (stmts, env)

    class Config:
        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type["DisabledColumn"]) -> None:
            schema["required"] = [f for f in schema.get("required", []) if f != "name"]


class ForeignKey(BaseModel):
    table: str
    column: str


class DisabledColumn(BaseModel, Translatable):
    enabled: bool

    def translate(self, env: Environment) -> TranslateResponse:
        return list(), env

    @validator("enabled")
    def validate_enabled(cls, val):
        assert not val
        return False

    class Config:
        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type["DisabledColumn"]) -> None:
            schema.get("properties", {})["enabled"]["enum"] = [False]


class TargetColumn(BaseColumn, Translatable):
    tables: List[Union[Query, TableReference, str]]
    constraints: Optional[List[str]]
    expression: str
    primary_key: str
    references: Optional[ForeignKey]

    class Config:
        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type["DisabledColumn"]) -> None:
            schema["required"] = [
                f for f in schema.get("required", []) if f != "primary_key"
            ]

    @validator("tables", each_item=True)
    def check_add_default_primary_key(cls, source, values, **kwargs):
        if isinstance(source, str):
            return TableReference.from_str(source)
        return source

    def translate(self, env: Environment) -> TranslateResponse:
        assert "PrimaryKeyConstraints" in env
        assert "TargetTable" in env
        target_table = env["TargetTable"]
        constraints = env["PrimaryKeyConstraints"]

        if not self.enabled:
            return None, env

        frm = [Table(target_table, "mapping")]

        whr = Criterion(constraints[self.primary_key])
        frm.extend(t for table in self.tables for t in table.translate(env)[0])

        if self.constraints:
            whr.extend(self.constraints)

        exp = self.expression

        if self.references is not None:
            t = Table(self.references.table, "mapping")
            frm.append(t)
            whr.append(Expression(f"{t.to_sql()}.{self.references.column} = {exp}"))
            exp = f"{t.to_sql()}.id"

        col = Column(self.name, Table(target_table, "omop"))
        statements = [
            UpdateStatement(col, expression=Expression(exp), criterion=whr, source=frm)
        ]
        return (statements, env)


class PrimaryKey(BaseColumn, Translatable):
    sources: Dict[str, PrimaryKeySource]

    @validator("sources", pre=True)
    def check_add_default_primary_key(cls, sources, values, **kwargs):
        for name, source in sources.items():
            if isinstance(source, dict):
                if "name" not in source:
                    source["name"] = name
            elif isinstance(source, PrimaryKeySource):
                source.name = name
        return sources

    def create_table(self, env: Environment):

        columns = list()
        for alias, pk in self.sources.items():
            table = pk.table.alias
            for col, dtype in pk.columns.items():
                columns.append(ColumnDefinition(f"{table}_{col}", datatype=dtype))

        return CreateTableStatement(
            primary_key=self.name,
            table=Table(env["TargetTable"], "mapping"),
            columns=columns,
        )

    def update_environment(self, env: Environment) -> Environment:
        assert "TargetTable" in env
        target_table = env["TargetTable"]
        map_name = env["MappingTable"]
        default_schema = env["DefaultSchema"]
        constraints = dict()
        for k, pk in self.sources.items():
            predicates = [
                Expression(f"omop.{target_table}.{self.name} = {map_name}.id")
            ]
            if isinstance(pk.table, Query):
                table_ref = pk.table.alias
                fq_table_ref = table_ref
            elif isinstance(pk.table, TableReference):
                table_ref = pk.table.alias
                pk_schema = pk.table.table_schema
                if pk_schema is None:
                    pk_schema = default_schema
                fq_table_ref = f"{pk_schema}.{table_ref}"
            else:
                raise ValueError(f"table of type {type(pk.table)} are not supported")
            predicates.extend(
                [
                    Expression(f"{fq_table_ref}.{c} = {map_name}.{table_ref}_{c}")
                    for c in pk.columns
                ]
            )
            constraints[k] = Criterion(predicates)

        env["PrimaryKeyConstraints"] = constraints
        return env

    def translate(self, env: Environment) -> TranslateResponse:
        env = self.update_environment(env)
        target_table = env["TargetTable"]
        stmts = list()
        stmts.append(self.create_table(env))
        for pk, pk_data in self.sources.items():
            stmt, _ = pk_data.translate(env)
            stmts.extend(stmt)
        select = SelectStatement(
            expressions=(Expression(f"mapping.{target_table}.id"),),
            source=(Table(target_table, "mapping"),),
        )
        stmts.append(
            InsertFromStatement(
                columns=(self.name,), target=Table(target_table, "omop"), source=select
            )
        )
        return stmts, env


AllColumns = Union[TargetColumn, ConstantTargetColumn, DisabledColumn]


class TargetTable(BaseModel, Translatable):
    name: str
    primary_key: PrimaryKey
    columns: List[Union[DisabledColumn, TargetColumn, ConstantTargetColumn]]

    @validator("columns", each_item=True, pre=True)
    def check_add_default_primary_key(cls, col, values, **kwargs):
        if "primary_key" in values:
            pks = values["primary_key"].sources
            if len(pks) == 1:
                pk = list(pks.keys())[0]
                if "primary_key" not in col:
                    col["primary_key"] = pk
        return col

    @validator("columns", each_item=True)
    def check_primary_keys_in_columns(cls, col, values, **kwargs):
        if "primary_key" not in values:
            return col
        pks = values["primary_key"].sources
        if isinstance(col, TargetColumn) and col.primary_key not in pks and col.enabled:

            possible_keys = ", ".join([f'"{k}"' for k in pks])
            raise ValueError(
                f"primary_key '{col.primary_key}' not defined. Available primary_keys are: ({possible_keys})"
            )
        return col

    def get_insert_statements(self) -> Iterable[str]:
        stmts = self.primary_key.get_insert_statements(self.name)
        return [stmt.to_sql() for stmt in stmts]

    def get_update_statements(self) -> List[str]:
        for col in self.columns:
            stmt = col.get_update_statement(self.name, self.primary_key)
            if stmt is not None:
                yield stmt.to_sql()

    def get_delete_statements(self) -> List[str]:
        cols = REQUIRED_FIELDS.get_fields(self.name)
        return [f"DELETE FROM OMOP.{self.name} WHERE {col} is null;" for col in cols]

    def get_script(self):
        stmts, _ = self.translate()
        stmts = [stmt.to_sql() for stmt in stmts]
        return "\n".join(stmts)

    def translate(self, env: Environment = dict()) -> TranslateResponse:
        env = {
            "TargetTable": self.name,
            "MappingTable": f"mapping.{self.name}",
            "DefaultSchema": "cerner",
        }
        script = list()
        statements, env = self.primary_key.translate(env)
        script.extend(statements)
        for col in self.columns:
            statements, _ = col.translate(env)
            if statements is not None:
                script.extend(statements)
        return script, env

