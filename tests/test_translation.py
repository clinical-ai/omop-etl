import os
import re

import pytest
from omop_etl.schema import *

from tests.utils import *


def trim_whitespace(s):
    return re.sub("\s+", " ", s.strip())


@pytest.fixture(params=["foo", "cerner.foo"])
def primary_key_source(request):
    return PrimaryKey(
        name="id",
        sources={
            "foo_pk": PrimaryKeySource(
                name="foo_pk", table=request.param, columns={"id": "integer"}
            )
        },
    )


@pytest.fixture
def baz_env():
    return {
        "TargetTable": "baz",
        "MappingTable": "mapping.baz",
        "DefaultSchema": "cerner",
    }


@pytest.fixture
def baz_pk_env():
    return {
        "TargetTable": "baz",
        "MappingTable": "mapping.foo",
        "DefaultSchema": "cerner",
        "PrimaryKeyConstraints": {
            "foo_pk": Criterion(
                ["omop.baz.id = mapping.baz.id", "cerner.foo.id = mapping.baz.foo_id"]
            )
        },
    }


def load_table(name) -> TargetTable:
    fn = os.path.join(".", "tests", "rules", name)
    with open(fn) as f:
        return TargetTable.parse_string(f.read())


@pytest.mark.parametrize(
    argnames="pk_name,source,expected",
    argvalues=[
        (
            "id",
            PrimaryKeySource(name="pk", table="foo", columns={"id": "integer"}),
            ["omop.baz.id = mapping.baz.id", "cerner.foo.id = mapping.baz.foo_id",],
        ),
        (
            "id",
            PrimaryKeySource(
                name="pk", table="foo", columns={"col1": "integer", "col2": "integer"}
            ),
            [
                "omop.baz.id = mapping.baz.id",
                "cerner.foo.col1 = mapping.baz.foo_col1",
                "cerner.foo.col2 = mapping.baz.foo_col2",
            ],
        ),
        (
            "personId",
            PrimaryKeySource(name="pk", table="foo", columns={"id": "integer"}),
            [
                "omop.baz.personId = mapping.baz.id",
                "cerner.foo.id = mapping.baz.foo_id",
            ],
        ),
        (
            "personId",
            PrimaryKeySource(
                name="pk", table="foo", columns={"col1": "integer", "col2": "integer"}
            ),
            [
                "omop.baz.personId = mapping.baz.id",
                "cerner.foo.col1 = mapping.baz.foo_col1",
                "cerner.foo.col2 = mapping.baz.foo_col2",
            ],
        ),
    ],
    ids=["id-simple", "id-two_cols", "persondId-simple", "persondId-two_cols",],
)
def test_pk_update_env(baz_env, pk_name, source, expected):
    pk = PrimaryKey(name=pk_name, sources={"pk": source,},)
    env = pk.update_environment(baz_env)

    assert "PrimaryKeyConstraints" in env

    actual = env["PrimaryKeyConstraints"]

    assert "pk" in actual

    assert actual["pk"] == expected


def test_translate_pk_source_with_query(baz_env):
    source = PrimaryKeySource(
        name="pk",
        table=Query(alias="fake", query="select * from cerner.fake"),
        columns={"id": "integer"},
    )
    stmts, env = source.translate(baz_env)
    assert len(stmts) == 1
    actual = stmts[0]

    select_cols = (Expression(f"fake.id as fake_id"),)
    tables = (QueryTable(query="select * from cerner.fake", alias="fake"),)
    select = SelectStatement(expressions=select_cols, source=tables)
    expected = InsertFromStatement(["fake_id"], Table("baz", "mapping"), select)
    assert expected == actual


def test_terse_translate_column(baz_pk_env):
    col = TargetColumn(
        name="alpha", primary_key="foo_pk", tables=["foo"], expression="foo.staff_id"
    )

    expected = UpdateStatement(
        column=Column("alpha", Table("baz", "omop")),
        expression=Expression("foo.staff_id"),
        criterion=baz_pk_env["PrimaryKeyConstraints"]["foo_pk"],
        source=[Table("baz", "mapping"), Table("foo", "cerner")],
    )

    statements, env = col.translate(baz_pk_env)
    assert len(statements) == 1
    actual = statements[0]

    assert isinstance(actual.criterion, Criterion)
    assert isinstance(actual.expression, Expression)
    assert isinstance(actual.column, Column)

    assert expected == actual


def test_translate_column(baz_pk_env):
    col = TargetColumn(
        name="alpha", primary_key="foo_pk", tables=["foo"], expression="foo.staff_id"
    )

    expected = UpdateStatement(
        column=Column("alpha", Table("baz", "omop")),
        expression=Expression("foo.staff_id"),
        criterion=baz_pk_env["PrimaryKeyConstraints"]["foo_pk"],
        source=[Table("baz", "mapping"), Table("foo", "cerner")],
    )

    statements, env = col.translate(baz_pk_env)
    assert len(statements) == 1
    actual = statements[0]

    assert isinstance(actual.criterion, Criterion)
    assert isinstance(actual.expression, Expression)
    assert isinstance(actual.column, Column)

    assert expected == actual


def test_translate_foreign_key(baz_pk_env):
    fk = ForeignKey(table="bar", column="staff_id")
    col = TargetColumn(
        name="alpha",
        primary_key="foo_pk",
        tables=["foo"],
        expression="foo.staff_id",
        references=fk,
    )

    whr = Criterion(
        [
            *baz_pk_env["PrimaryKeyConstraints"]["foo_pk"],
            "mapping.bar.staff_id is not null",
            "mapping.bar.staff_id = foo.staff_id",
        ]
    )
    expected = UpdateStatement(
        column=Column("alpha", Table("baz", "omop")),
        expression=Expression("mapping.bar.id"),
        criterion=whr,
        source=[
            Table("baz", "mapping"),
            Table("foo", "cerner"),
            Table("bar", "mapping"),
        ],
    )

    actual, env = col.translate(baz_pk_env)
    actual = actual[0]

    assert expected == actual

    assert isinstance(actual.criterion, Criterion)
    assert isinstance(actual.expression, Expression)
    assert isinstance(actual.column, Column)


def compare_sql(expected, actual):
    expected = trim_whitespace(expected).lower()
    actual = trim_whitespace(actual).lower()
    assert expected == actual


def test_translate_primary_key_source(baz_env):
    pk = PrimaryKeySource(name="PK", table="foo", columns={"id": "int"})
    select = SelectStatement(
        expressions=("foo.id as foo_id",), source=(Table("foo", "cerner"),)
    )
    expected = InsertFromStatement(("foo_id",), Table("baz", "mapping"), select)

    actual, env = pk.translate(baz_env)
    actual = actual[0]

    assert expected.source.source == actual.source.source
    assert expected == actual

    pk = PrimaryKeySource(
        name="PK", table="foo", columns={"col1": "int", "col2": "int"}
    )

    select = SelectStatement(
        ("foo.col1 as foo_col1", "foo.col2 as foo_col2"), (Table("foo", "cerner"),),
    )
    expected = InsertFromStatement(
        ("foo_col1", "foo_col2"), Table("baz", "mapping"), select
    )

    actual, env = pk.translate(baz_env)
    actual = actual[0]
    assert expected == actual


def test_translate_primary_key_source_with_constaint(baz_env):
    pk = PrimaryKeySource(
        name="PK", table="foo", columns={"id": "int"}, constraints=["foo.alpha==0"]
    )
    select = SelectStatement(
        expressions=("foo.id as foo_id",),
        source=(Table("foo", "cerner"),),
        criterion=Criterion(["foo.alpha==0"]),
    )
    expected = InsertFromStatement(("foo_id",), Table("baz", "mapping"), select)

    actual, env = pk.translate(baz_env)
    actual = actual[0]
    assert expected.source.source == actual.source.source
    assert expected == actual

    pk = PrimaryKeySource(
        name="PK",
        table="foo",
        columns={"col1": "int", "col2": "int"},
        constraints=["foo.alpha==0"],
    )

    select = SelectStatement(
        expressions=("foo.col1 as foo_col1", "foo.col2 as foo_col2"),
        source=(Table("foo", "cerner"),),
        criterion=Criterion(["foo.alpha==0"]),
    )
    expected = InsertFromStatement(
        ("foo_col1", "foo_col2"), Table("baz", "mapping"), select
    )

    actual, env = pk.translate(baz_env)
    actual = actual[0]
    assert expected == actual


@pytest.mark.parametrize(argnames="pk_name", argvalues=["id", "personId", "pk_id"])
def test_translate_primary_key(baz_env, pk_name):
    pk = PrimaryKey(
        name=pk_name,
        sources={"PK": PrimaryKeySource(name="PK", table="foo", columns={"id": "int"})},
    )

    expected_a = InsertFromStatement(
        columns=("foo_id",),
        target=Table("baz", "mapping"),
        source=SelectStatement(
            expressions=("foo.id as foo_id",), source=(Table("foo", "cerner"),)
        ),
    )
    expected_b = InsertFromStatement(
        columns=(pk_name,),
        target=Table("baz", "omop"),
        source=SelectStatement(
            expressions=((Expression(f"mapping.baz.id"),)),
            source=(Table("baz", "mapping"),),
        ),
    )

    stmts, env = pk.translate(baz_env)
    assert expected_a in stmts
    assert expected_b in stmts


@pytest.mark.parametrize(argnames="pk_name", argvalues=["id", "personId", "pk_id"])
def test_translate_two_source_primary_key(baz_env, pk_name):
    pk = PrimaryKey(
        name=pk_name,
        sources={
            "PK1": PrimaryKeySource(name="PK1", table="foo", columns={"id": "int"}),
            "PK2": PrimaryKeySource(name="PK2", table="bar", columns={"id": "int"}),
        },
    )

    expected_a = InsertFromStatement(
        columns=("foo_id",),
        target=Table("baz", "mapping"),
        source=SelectStatement(
            expressions=("foo.id as foo_id",), source=(Table("foo", "cerner"),)
        ),
    )
    expected_b = InsertFromStatement(
        columns=("bar_id",),
        target=Table("baz", "mapping"),
        source=SelectStatement(
            expressions=("bar.id as bar_id",), source=(Table("bar", "cerner"),)
        ),
    )
    expected_c = InsertFromStatement(
        columns=(pk_name,),
        target=Table("baz", "omop"),
        source=SelectStatement(
            expressions=((Expression(f"mapping.baz.id"),)),
            source=(Table("baz", "mapping"),),
        ),
    )

    stmts, env = pk.translate(baz_env)
    assert expected_a in stmts
    assert expected_b in stmts
    assert expected_c in stmts


@pytest.mark.parametrize("table_name", [("baz"), ("test")])
@pytest.mark.parametrize("pk", [("id"), ("foo_id")])
def test_translate_disjoint_pk(table_name: str, pk: str):
    c = {
        "id": "integer",
    }
    key = PrimaryKey(
        name=pk,
        sources={
            "foo": PrimaryKeySource(name="foo", table="foo", columns=c),
            "bar": PrimaryKeySource(name="bar", table="bar", columns=c),
        },
    )

    actual = key.create_table({"TargetTable": table_name})
    expected = CreateTableStatement(
        pk,
        table=Table(table_name, "mapping"),
        columns=[
            ColumnDefinition("foo_id", "integer"),
            ColumnDefinition("bar_id", "integer"),
        ],
    )

    assert expected == actual


def test_translate_disabled_cols_table():

    copy_table = load_table("copy.yaml")

    statements, _ = copy_table.translate()
    assert len(statements) == 5

    copy_table.columns[0].enabled = False
    statements, _ = copy_table.translate()
    assert len(statements) == 4

    copy_table.columns[1].enabled = False
    statements, _ = copy_table.translate()
    assert len(statements) == 3

    copy_table.columns[0].enabled = True
    statements, _ = copy_table.translate()
    assert len(statements) == 4

    copy_table.columns[1].enabled = True
    statements, _ = copy_table.translate()
    assert len(statements) == 5
