import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type, TypeVar, Union

import pandas as pd
import pydantic
from sqlalchemy import table
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


class TempTable(Query):
    def translate(self, env: Environment) -> TranslateResponse:
        if "TempTables" not in env:
            env["TempTables"] = set()
        env["TempTables"].add(self.alias)
        return [CreateTempTableStatement(alias=self.alias, query=self.query)], env


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
        if self.alias in env["TempTables"]:
            schema = None

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
    references: Optional[Union[ForeignKey, Dict[str, ForeignKey]]]

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
            if isinstance(self.references, ForeignKey):
                ref_mapping_table = self.references.table
                ref_mapping_column = self.references.column
            else:
                ref_mapping_table, *_ = self.references.keys()
                ref = self.references[ref_mapping_table]
                ref_mapping_column = f"{ref.table}_{ref.column}"

            t = Table(ref_mapping_table, "mapping")
            frm.append(t)
            whr.append(Expression(f"{t.to_sql()}.{ref_mapping_column} is not null"))
            whr.append(Expression(f"{t.to_sql()}.{ref_mapping_column} = {exp}"))
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
        stmts = list()
        columns = list()
        for _, pk in self.sources.items():
            table = pk.table.alias
            for col, dtype in pk.columns.items():
                columns.append(ColumnDefinition(f"{table}_{col}", datatype=dtype))

        table = Table(env["TargetTable"], "mapping")
        if "DropTables" in env and env["DropTables"]:
            stmts.append(DropTableStatement(table=table))
        stmts.append(
            CreateTableStatement(primary_key=self.name, table=table, columns=columns,)
        )
        return stmts

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
                if table_ref not in env["TempTables"]:
                    fq_table_ref = f"{pk_schema}.{table_ref}"
                else:
                    fq_table_ref = table_ref
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
        stmts.extend(self.create_table(env))
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


class Dependency(BaseModel, Translatable):
    default_schema: Optional[str] = None
    pre_init: Optional[List[TempTable]]
    post_init: Optional[List[TempTable]]
    scripts: Optional[List[str]]
    depends_on: Optional[List[str]]

    @property
    def default_env(self):
        return {
            "DefaultSchema": self.default_schema,
            "TempTables": set(),
        }

    def translate_pre_init(self, env: Environment = None) -> Tuple[str, Environment]:
        env = env or self.default_env
        statements = list()
        if self.scripts is not None:
            for s in self.scripts:
                statements.append(Script(s))
        if self.pre_init is not None:
            for table in self.pre_init:
                stmt, env = table.translate(env)
                statements.extend(stmt)
        return statements, env

    def translate_post_init(self, env: Environment = None) -> Tuple[str, Environment]:
        env = env or self.default_env
        statements = list()
        if self.post_init is not None:
            for table in self.post_init:
                stmt, env = table.translate(env)
                statements.extend(stmt)
        return statements, env

    def translate(self, env: Environment = None) -> TranslateResponse:
        env = env or self.default_env
        statements, env = self.translate_pre_init(env)
        stmts, env = self.translate_post_init(env)
        statements.extend(stmts)
        return statements, env


class TargetTable(Dependency):
    name: str
    primary_key: PrimaryKey
    columns: List[Union[DisabledColumn, TargetColumn, ConstantTargetColumn]]
    default_schema: Optional[str] = "cerner"

    @property
    def default_env(self):
        return {
            "TargetTable": self.name,
            "MappingTable": f"mapping.{self.name}",
            "DefaultSchema": self.default_schema,
            "TempTables": set(),
        }

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

    def get_script(
        self,
        env: Environment = None,
        include_initialization: bool = True,
        include_process: bool = True,
    ):
        stmts, _ = self.translate(
            env=env,
            include_initialization=include_initialization,
            include_process=include_process,
        )
        stmts = [stmt.to_sql() for stmt in stmts]
        return "\n".join(stmts)

    def get_initialization(self, env: Environment = None) -> Tuple[str, Environment]:
        stmts, env = self.translate_initialization(env)
        stmts = [stmt.to_sql() for stmt in stmts]
        return "\n".join(stmts), env

    def translate_initialization(
        self, env: Environment = None
    ) -> Tuple[str, Environment]:
        env = env or self.default_env
        statements, env = self.translate_pre_init(env)

        insert, env = self.primary_key.translate(env)
        statements.extend(insert)

        stmts, env = self.translate_post_init(env)
        statements.extend(stmts)

        return statements, env

    def translate(
        self,
        env: Environment = None,
        include_initialization: bool = True,
        include_process: bool = True,
    ) -> TranslateResponse:
        env = env or self.default_env
        script = list()
        if include_initialization:
            statements, env = self.translate_initialization(env)
            script.extend(statements)
        if include_process:
            for col in self.columns:
                statements, _ = col.translate(env)
                if statements is not None:
                    script.extend(statements)
        return script, env

