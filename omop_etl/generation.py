from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Tuple


class Serializable(ABC):
    @abstractmethod
    def to_sql(self):
        raise NotImplementedError


class Expression(str, Serializable):
    def to_sql(self):
        return self.replace("\n", " ")


class Statement(Expression):
    pass


class Criterion(List[Expression], Serializable):
    def to_sql(self):
        return " and ".join([f"({e})" for e in self])

    def __eq__(self, o: object) -> bool:
        return set(self) == set(o)


@dataclass(eq=True)
class Table(Serializable):
    alias: str
    schema: Optional[str] = None

    def to_sql(self):
        if self.schema is None:
            return self.alias
        else:
            return f"{self.schema}.{self.alias}"

    def __hash__(self):
        return hash(self.alias) + hash(self.schema)


@dataclass(eq=True)
class QueryTable(Serializable):
    alias: str
    query: str

    def to_sql(self):
        q = self.query.replace("\n", " ")
        return f"({q}) as {self.alias}"

    def __hash__(self):
        return hash(self.alias) + hash(self.query)


@dataclass
class Column(Serializable):
    name: str
    table: Table

    def to_sql(self):
        t = self.table.to_sql()
        return f"{t}.{self.name}"


@dataclass
class ColumnDefinition(Serializable):
    name: str
    datatype: str

    def to_sql(self):
        return f"{self.name} {self.datatype} null"


@dataclass(eq=True)
class DropTableStatement(Serializable):
    table: Table

    def to_sql(self):
        return f"drop table if exists {self.table.to_sql()};"

    def __hash__(self):
        return hash("drop table") + hash(self.table)


@dataclass(eq=True)
class CreateTableStatement(Serializable):
    primary_key: str
    table: Table
    columns: Tuple[ColumnDefinition]

    def __post_init__(self):
        self.columns = tuple(self.columns)

    def to_sql(self):
        columns = ", ".join(map(lambda c: c.to_sql(), self.columns))
        return f"create table {self.table.to_sql()} (id serial PRIMARY KEY, {columns});"


@dataclass(eq=True)
class CreateTempTableStatement(Serializable):
    alias: str
    query: str

    def to_sql(self):
        return f"create temp table {self.alias} as {self.query};"


@dataclass(eq=True)
class SelectStatement(Serializable):
    expressions: Tuple[Expression]
    source: Tuple[Table]
    criterion: Optional[Criterion] = None

    def __post_init__(self):
        self.expressions = tuple(self.expressions)
        self.source = tuple(self.source)
        if self.criterion is not None:
            self.criterion = Criterion(self.criterion)

    def to_sql(self):
        sel = ", ".join([e.to_sql() for e in self.expressions])
        frm = ", ".join([t.to_sql() for t in self.source])
        if self.criterion is None:
            return f"select {sel} from {frm};"
        else:
            whr = self.criterion.to_sql()
            return f"select {sel} from {frm} where {whr};"

    def __hash__(self) -> bool:
        return hash(self.expressions) + hash(self.source) + hash(self.criterion)


@dataclass(eq=True)
class InsertFromStatement(Serializable):
    columns: Tuple[str]
    target: Table
    source: SelectStatement

    def __post_init__(self):
        self.columns = tuple(self.columns)

    def to_sql(self):
        select = self.source.to_sql()
        columns = ", ".join(self.columns)
        target_table = self.target.to_sql()
        return f"insert into {target_table} ({columns}) {select}"

    def __hash__(self) -> bool:
        return hash(self.columns) + hash(self.target) + hash(self.source)


@dataclass(eq=True)
class UpdateStatement(Serializable):
    column: Column
    expression: Expression
    criterion: Optional[Criterion] = None
    source: Optional[Tuple[Table]] = None

    def __post_init__(self):
        if self.source is not None:
            self.source = tuple(self.source)

    def to_sql(self):
        target_column = self.column
        target_table = target_column.table.to_sql()

        exp = self.expression.to_sql()

        clauses = list()

        clauses.append(f"update {target_table} set {target_column.name} = {exp}")

        if self.source is not None:
            frm = ", ".join([t.to_sql() for t in self.source])
            clauses.append(f"from {frm}")

        if self.criterion is not None:
            whr = self.criterion.to_sql()
            clauses.append(f"where {whr}")

        stmt = " ".join(clauses)
        return f"{stmt};"
