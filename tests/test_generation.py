from generation import *

from tests.utils import *


def test_expression_generation():
    target = Expression("foo.id")
    expected = "foo.id"
    actual = target.to_sql()
    assert expected == actual

    target = Expression("foo.bar.id = foo.id")
    expected = "foo.bar.id = foo.id"
    actual = target.to_sql()
    assert expected == actual


def test_criterion_generation():
    target = Criterion(["a = b", "b = c or b = d"])
    expected = "(a = b) and (b = c or b = d)"
    actual = target.to_sql()
    assert expected == actual

    target = Criterion(["foo.bar.id == foo.id",])
    expected = "(foo.bar.id == foo.id)"
    actual = target.to_sql()
    assert expected == actual


def test_table_generation():
    target = Table("foo")
    expected = "foo"
    actual = target.to_sql()
    assert expected == actual

    target = Table("foo", "bar")
    expected = "bar.foo"
    actual = target.to_sql()
    assert expected == actual


def test_column_generation():
    target = Column("id", Table("foo"))
    expected = "foo.id"
    actual = target.to_sql()
    assert expected == actual

    target = Column("id", Table("foo", "bar"))
    expected = "bar.foo.id"
    actual = target.to_sql()
    assert expected == actual


def test_select_generation():
    target = SelectStatement([Expression("*")], source=[Table("foo")])
    expected = "select * from foo;"
    actual = target.to_sql()
    assert expected == actual

    target = SelectStatement(
        expressions=[Expression("*")],
        criterion=Criterion(["foo.id = 3"]),
        source=[Table("foo")],
    )
    expected = "select * from foo where (foo.id = 3);"
    actual = target.to_sql()
    assert expected == actual


def test_short_update_generation():
    table = Table("foo", "bar")
    col = Column("name", table)
    exp = Expression("4")
    target = UpdateStatement(col, exp)
    expected = "update bar.foo set name = 4;"
    actual = target.to_sql()
    assert expected == actual

    whr = Criterion(["foo = 3"])
    target = UpdateStatement(col, exp, whr)
    expected = "update bar.foo set name = 4 where (foo = 3);"
    actual = target.to_sql()
    assert expected == actual

    whr = Criterion(["bar.baz.name"])
    target = UpdateStatement(col, exp, whr)
    expected = "update bar.foo set name = 4 where (bar.baz.name);"
    actual = target.to_sql()
    assert expected == actual


def test_update_generation():
    whr = Criterion(
        ["omop.baz.id = mapping.baz.id", "cerner.foo.id = mapping.baz.foo_id"]
    )
    target = UpdateStatement(
        column=Column("alpha", Table("baz", "omop")),
        expression=Expression("foo.staff_id"),
        criterion=whr,
        source=[Table("baz", "mapping"), Table("foo", "cerner")],
    )

    expected = """update omop.baz set alpha = foo.staff_id from mapping.baz, cerner.foo where (omop.baz.id = mapping.baz.id) and (cerner.foo.id = mapping.baz.foo_id);"""
    actual = target.to_sql()
    assert expected == actual


def test_foreign_key_generation():
    whr = Criterion(
        [
            "omop.baz.id = mapping.baz.id",
            "cerner.foo.id = mapping.baz.foo_id",
            "mapping.bar.staff_id = foo.staff_id",
        ]
    )
    target = UpdateStatement(
        column=Column("alpha", Table("baz", "omop")),
        expression=Expression("mapping.bar.id"),
        criterion=whr,
        source=[
            Table("baz", "mapping"),
            Table("bar", "mapping"),
            Table("foo", "cerner"),
        ],
    )

    expected = """update omop.baz set alpha = mapping.bar.id from mapping.baz, mapping.bar, cerner.foo where (omop.baz.id = mapping.baz.id) and (cerner.foo.id = mapping.baz.foo_id) and (mapping.bar.staff_id = foo.staff_id);"""
    actual = target.to_sql()
    assert expected == actual


def test_insert_generation():
    select = SelectStatement([Expression("id")], source=[Table("foo")])
    stmt = InsertFromStatement(columns=["id"], target=Table("bar"), source=select)
    expected = "insert into bar (id) select id from foo;"
    actual = stmt.to_sql()
    assert expected == actual

    select = SelectStatement([Expression("id")], source=[Table("foo", "b")])
    stmt = InsertFromStatement(columns=["id"], target=Table("bar", "a"), source=select)
    expected = "insert into a.bar (id) select id from b.foo;"
    actual = stmt.to_sql()
    assert expected == actual

    select = SelectStatement([Expression("alpha as id")], source=[Table("foo")])
    stmt = InsertFromStatement(columns=["id"], target=Table("bar", "a"), source=select)
    expected = "insert into a.bar (id) select alpha as id from foo;"
    actual = stmt.to_sql()
    assert expected == actual
