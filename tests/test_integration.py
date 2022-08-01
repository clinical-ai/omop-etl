import os

import pytest
from omop_etl.schema import *

from tests.utils import *


def load_table(name) -> TargetTable:
    fn = os.path.join(".", "tests", "rules", name)
    with open(fn) as f:
        return TargetTable.parse_string(f.read())


postgresql = factories.postgresql(
    "postgresql_proc", load=[Path("tests", "data", "schema.sql")],
)


class BaseTable(ABC):
    @abstractmethod
    def parse(self) -> TargetTable:
        raise NotImplementedError()

    def test_parse_table(self):
        table = self.parse()

    def translate(self) -> TranslateResponse:
        table = self.parse()
        return table.translate()

    def generate(self) -> str:
        statements, _ = self.translate()
        return [s.to_sql() for s in statements]

    def execute(self, db):
        statements = self.generate()
        for statement in statements:
            with db.cursor() as cur:
                cur.execute(statement)
        db.commit()

    def test_translate_create_statement(self):
        pytest.fail("Not implemented")

    def test_translate_insert_statements(self):
        pytest.fail("Not implemented")

    def test_translate_update_statements(self):
        pytest.fail("Not implemented")

    def test_generate_create_statement(self):
        pytest.fail("Not implemented")

    def test_generate_insert_statements(self):
        pytest.fail("Not implemented")

    def test_generate_update_statements(self):
        pytest.fail("Not implemented")

    @skip_if_no_db
    def test_execute(self, postgresql):
        pytest.fail("Not implemented")


class TestCopyTable(BaseTable):
    def parse(self):
        return load_table("copy.yaml")

    def test_generate_create_statement(self):
        statements = self.generate()
        actual = statements[0]

        expected = (
            "create table mapping.baz (id serial PRIMARY KEY, foo_id integer null);"
        )

        assert actual == expected

    def test_generate_insert_statements(self):
        statements = self.generate()
        expected = (
            "insert into mapping.baz (foo_id) select foo.id as foo_id from cerner.foo;"
        )
        assert expected in statements

        expected = "insert into omop.baz (id) select mapping.baz.id from mapping.baz;"
        assert expected in statements

    def test_generate_update_statements(self):
        statements = self.generate()[3:]

        expected = "update omop.baz set alpha = foo.alpha from mapping.baz, cerner.foo where (omop.baz.id = mapping.baz.id) and (cerner.foo.id = mapping.baz.foo_id);"
        assert statements[0] == expected

        expected = "update omop.baz set beta = bar.beta from mapping.baz, cerner.foo, cerner.bar where (omop.baz.id = mapping.baz.id) and (cerner.foo.id = mapping.baz.foo_id) and (foo.id = bar.id);"
        assert statements[1] == expected

    def test_translate_create_statement(self):
        statements, env = self.translate()
        actual = statements[0]
        expected = CreateTableStatement(
            "id", Table("baz", "mapping"), (ColumnDefinition("foo_id", "integer"),)
        )
        assert actual == expected

    def test_translate_insert_statements(self):
        statements, env = self.translate()
        statements = statements[1:3]
        expected = InsertFromStatement(
            ["foo_id"],
            Table("baz", "mapping"),
            SelectStatement(["foo.id as foo_id"], [Table("foo", "cerner")]),
        )
        assert expected == statements[1] or expected == statements[0]

        expected = InsertFromStatement(
            ["id"],
            Table("baz", "omop"),
            SelectStatement(["mapping.baz.id"], [Table("baz", "mapping")]),
        )
        assert expected == statements[1] or expected == statements[0]

    def test_translate_update_statements(self):
        statements, env = self.translate()
        statements = statements[3:]
        expected = UpdateStatement(
            column=Column("alpha", Table("baz", "omop")),
            expression="foo.alpha",
            criterion=Criterion(
                ["omop.baz.id = mapping.baz.id", "cerner.foo.id = mapping.baz.foo_id"]
            ),
            source=[Table("baz", "mapping"), Table("foo", "cerner")],
        )
        assert expected == statements[1] or expected == statements[0]

        expected = UpdateStatement(
            column=Column("beta", Table("baz", "omop")),
            expression="bar.beta",
            criterion=Criterion(
                [
                    "omop.baz.id = mapping.baz.id",
                    "cerner.foo.id = mapping.baz.foo_id",
                    "foo.id = bar.id",
                ]
            ),
            source=[
                Table("baz", "mapping"),
                Table("foo", "cerner"),
                Table("bar", "cerner"),
            ],
        )

        assert expected == statements[1] or expected == statements[0]

    @skip_if_no_db
    def test_execute(self, postgresql):
        self.execute(postgresql)
        cur = postgresql.cursor()

        cur.execute("SELECT alpha FROM cerner.foo")
        expected = cur.fetchall()

        cur.execute("SELECT alpha FROM omop.baz")
        actual = cur.fetchall()

        assert expected == actual

        cur.execute("SELECT beta FROM cerner.bar")
        expected = cur.fetchall()

        cur.execute("SELECT beta FROM omop.baz")
        actual = cur.fetchall()

        assert expected == actual


class TestCustomQueryTable(BaseTable):
    def parse(self):
        return load_table("custom_query.yaml")

    def test_generate_pre_init(self):
        statements, env = self.translate()
        actual = statements[1].to_sql()

        expected = (
            "create temp table temp_table_1 as select * from "
            "(VALUES (0::int, 1::numeric), (1::int, 2::numeric), (2::int, 3::numeric), (3::int, 4::numeric), (4::int, 5::numeric)) as t (id, beta);"
        )

        assert actual == expected

    def test_generate_create_statement(self):
        statements, env = self.translate()
        actual = statements[2].to_sql()

        expected = (
            "create table mapping.baz (id serial PRIMARY KEY, foo_id integer null);"
        )

        assert actual == expected

    def test_generate_insert_statements(self):
        statements = self.generate()[3:5]

        expected = (
            "insert into mapping.baz (foo_id) select foo.id as foo_id "
            "from (select x.id, alpha, beta, total_rows() as total from (values (0, 'a1'), (2, 'b1'), (4, 'c1')) x(id, alpha), temp_table_1 where x.id = temp_table_1.id) as foo;"
        )
        assert statements[0] == expected

        expected = "insert into omop.baz (id) select mapping.baz.id from mapping.baz;"
        assert statements[1] == expected

    def test_generate_update_statements(self):
        statements = self.generate()[6:]

        expected = (
            "update omop.baz set alpha = foo.alpha "
            "from mapping.baz, (select x.id, alpha, beta, total_rows() as total from (values (0, 'a1'), (2, 'b1'), (4, 'c1')) x(id, alpha), temp_table_1 where x.id = temp_table_1.id) as foo "
            "where (omop.baz.id = mapping.baz.id) and (foo.id = mapping.baz.foo_id);"
        )
        assert statements[0] == expected

        expected = (
            "update omop.baz set beta = foo.beta "
            "from mapping.baz, (select x.id, alpha, beta, total_rows() as total from (values (0, 'a1'), (2, 'b1'), (4, 'c1')) x(id, alpha), temp_table_1 where x.id = temp_table_1.id) as foo "
            "where (omop.baz.id = mapping.baz.id) and (foo.id = mapping.baz.foo_id);"
        )
        assert statements[1] == expected

    def test_translate_pre_init(self):
        statements, env = self.translate()
        actual = statements[1]
        expected = CreateTempTableStatement(
            alias="temp_table_1",
            query="select * from (VALUES (0::int, 1::numeric), (1::int, 2::numeric), (2::int, 3::numeric), (3::int, 4::numeric), (4::int, 5::numeric)) as t (id, beta)",
        )
        assert isinstance(actual, CreateTempTableStatement)
        assert actual == expected

        actual = statements[5]
        expected = CreateTempTableStatement(
            alias="temp_table_2",
            query="select mapping.baz.id, temp_table_1.beta from mapping.baz, temp_table_1 where mapping.baz.id = temp_table_1.id",
        )
        assert isinstance(actual, CreateTempTableStatement)
        assert actual == expected

    def test_translate_create_statement(self):
        statements, env = self.translate()
        actual = statements[2]
        expected = CreateTableStatement(
            "id", Table("baz", "mapping"), (ColumnDefinition("foo_id", "integer"),)
        )
        assert actual == expected

    def test_translate_insert_statements(self):
        statements, env = self.translate()
        statements = statements[3:5]
        expected = InsertFromStatement(
            ["foo_id"],
            Table("baz", "mapping"),
            SelectStatement(
                ["foo.id as foo_id"],
                [
                    QueryTable(
                        alias="foo",
                        query="select x.id, alpha, beta, total_rows() as total from (values (0, 'a1'), (2, 'b1'), (4, 'c1')) x(id, alpha), temp_table_1 where x.id = temp_table_1.id",
                    )
                ],
            ),
        )

        assert statements[0] == expected

        expected = InsertFromStatement(
            ["id"],
            Table("baz", "omop"),
            SelectStatement(["mapping.baz.id"], [Table("baz", "mapping")]),
        )

        assert statements[1] == expected

    def test_translate_update_statements(self):
        statements, env = self.translate()
        statements = statements[6:]

        expected = UpdateStatement(
            column=Column("alpha", Table("baz", "omop")),
            expression="foo.alpha",
            criterion=Criterion(
                ["omop.baz.id = mapping.baz.id", "foo.id = mapping.baz.foo_id"]
            ),
            source=[
                Table("baz", "mapping"),
                QueryTable(
                    alias="foo",
                    query="select x.id, alpha, beta, total_rows() as total from (values (0, 'a1'), (2, 'b1'), (4, 'c1')) x(id, alpha), temp_table_1 where x.id = temp_table_1.id",
                ),
            ],
        )
        assert expected in statements

        expected = UpdateStatement(
            column=Column("beta", Table("baz", "omop")),
            expression="foo.beta",
            criterion=Criterion(
                ["omop.baz.id = mapping.baz.id", "foo.id = mapping.baz.foo_id"]
            ),
            source=[
                Table("baz", "mapping"),
                QueryTable(
                    alias="foo",
                    query="select x.id, alpha, beta, total_rows() as total from (values (0, 'a1'), (2, 'b1'), (4, 'c1')) x(id, alpha), temp_table_1 where x.id = temp_table_1.id",
                ),
            ],
        )
        assert statements[1] == expected

    @skip_if_no_db
    def test_execute(self, postgresql):
        self.execute(postgresql)

        cur = postgresql.cursor()

        cur.execute("SELECT alpha, beta FROM omop.baz")
        actual = cur.fetchall()

        expected = [("a1", 1), ("b1", 3), ("c1", 5)]

        assert expected == actual


class TestExternalTable(TestCopyTable):
    def parse(self):
        return load_table("external.yaml")

    def test_parse_columns(self):
        columns = self.parse().columns

        alpha = TargetColumn(
            name="alpha",
            tables=[
                TableReference(alias="foo"),
                TableReference(alias="vocabulary", schema="external"),
            ],
            constraints=["foo.id = external.vocabulary.id"],
            expression="external.vocabulary.name",
            primary_key="foo_pk",
        )

        assert columns[0] == alpha

        beta = TargetColumn(
            name="beta",
            tables=[TableReference(alias="foo")],
            expression="foo.beta",
            primary_key="foo_pk",
        )

        assert columns[1] == beta

    def test_generate_update_statements(self):
        statements = self.generate()[3:]

        expected = (
            "update omop.baz set alpha = external.vocabulary.name "
            "from mapping.baz, cerner.foo, external.vocabulary "
            "where (omop.baz.id = mapping.baz.id) "
            "and (cerner.foo.id = mapping.baz.foo_id) "
            "and (foo.id = external.vocabulary.id);"
        )
        assert statements[0] == expected

        expected = (
            "update omop.baz set beta = foo.beta from mapping.baz, cerner.foo "
            "where (omop.baz.id = mapping.baz.id) and (cerner.foo.id = mapping.baz.foo_id);"
        )
        assert statements[1] == expected

    def test_translate_update_statements(self):
        statements, env = self.translate()
        statements = statements[3:]

        expected = UpdateStatement(
            column=Column("alpha", Table("baz", "omop")),
            expression="external.vocabulary.name",
            criterion=Criterion(
                [
                    "omop.baz.id = mapping.baz.id",
                    "cerner.foo.id = mapping.baz.foo_id",
                    "foo.id = external.vocabulary.id",
                ]
            ),
            source=[
                Table("baz", "mapping"),
                Table("foo", "cerner"),
                Table("vocabulary", "external"),
            ],
        )
        assert expected == statements[1] or expected == statements[0]

        expected = UpdateStatement(
            column=Column("beta", Table("baz", "omop")),
            expression="foo.beta",
            criterion=Criterion(
                ["omop.baz.id = mapping.baz.id", "cerner.foo.id = mapping.baz.foo_id"]
            ),
            source=[Table("baz", "mapping"), Table("foo", "cerner")],
        )
        assert expected == statements[1] or expected == statements[0]

    @skip_if_no_db
    def test_execute(self, postgresql):
        self.execute(postgresql)

        cur = postgresql.cursor()

        cur.execute("SELECT alpha, beta FROM omop.baz")
        actual = cur.fetchall()

        expected = [("vocab1", 4), ("vocab2", 5), ("vocab3", 9)]

        assert expected == actual


class TestJoinTable(BaseTable):
    def parse(self):
        return load_table("join.yaml")

    def test_generate_create_statement(self):
        statements, env = self.translate()
        actual = statements[0].to_sql()

        expected = "create table mapping.baz (id serial PRIMARY KEY, foo2bar_foo_id integer null, foo2bar_bar_id integer null);"

        assert actual == expected

    def test_generate_insert_statements(self):
        statements, env = self.translate()
        statements = [s.to_sql() for s in statements[1:3]]

        expected = (
            "insert into mapping.baz (foo2bar_foo_id, foo2bar_bar_id) "
            "select foo2bar.foo_id as foo2bar_foo_id, foo2bar.bar_id as foo2bar_bar_id "
            "from cerner.foo2bar;"
        )
        assert statements[0] == expected

        expected = "insert into omop.baz (id) select mapping.baz.id from mapping.baz;"
        assert statements[1] == expected

    def test_generate_update_statements(self):
        statements, env = self.translate()
        statements = [s.to_sql() for s in statements[3:]]

        expected_parts = [
            "update omop.baz set alpha = foo.alpha",
            "from mapping.baz, cerner.foo, cerner.bar, cerner.foo2bar",
            "where (omop.baz.id = mapping.baz.id)",
            "and (cerner.foo2bar.foo_id = mapping.baz.foo2bar_foo_id)",
            "and (cerner.foo2bar.bar_id = mapping.baz.foo2bar_bar_id)",
            "and (foo.id = foo2bar.foo_id)",
            "and (bar.id = foo2bar.bar_id);",
        ]
        expected = " ".join(expected_parts)
        assert statements[0] == expected

        update = "update omop.baz set beta = CASE WHEN foo.beta > bar.beta THEN foo.beta ELSE bar.beta END"
        expected_parts[0] = update
        expected = " ".join(expected_parts)
        assert statements[1] == expected

        update = "update omop.baz set gamma = CASE WHEN foo.gamma > bar.gamma THEN foo.gamma ELSE bar.gamma END"
        expected_parts[0] = update
        expected = " ".join(expected_parts)
        assert statements[2] == expected

    def test_translate_create_statement(self):
        statements, env = self.translate()
        actual = statements[0]
        expected = CreateTableStatement(
            "id",
            Table("baz", "mapping"),
            (
                ColumnDefinition("foo2bar_foo_id", "integer"),
                ColumnDefinition("foo2bar_bar_id", "integer"),
            ),
        )
        assert actual == expected

    def test_translate_insert_statements(self):
        statements, env = self.translate()
        statements = statements[1:3]
        assert isinstance(statements[0], InsertFromStatement)
        assert isinstance(statements[1], InsertFromStatement)

        expected = InsertFromStatement(
            ["foo2bar_foo_id", "foo2bar_bar_id"],
            Table("baz", "mapping"),
            SelectStatement(
                [
                    "foo2bar.foo_id as foo2bar_foo_id",
                    "foo2bar.bar_id as foo2bar_bar_id",
                ],
                [Table("foo2bar", "cerner")],
            ),
        )
        assert expected == statements[0] or expected == statements[1]

        expected = InsertFromStatement(
            ["id"],
            Table("baz", "omop"),
            SelectStatement(["mapping.baz.id"], [Table("baz", "mapping")]),
        )
        assert expected == statements[0] or expected == statements[1]

    def test_translate_update_statements(self):
        statements, env = self.translate()
        statements = statements[3:]
        assert isinstance(statements[0], UpdateStatement)
        assert isinstance(statements[1], UpdateStatement)

        expected = UpdateStatement(
            column=Column("alpha", Table("baz", "omop")),
            expression="foo.alpha",
            criterion=Criterion(
                [
                    "omop.baz.id = mapping.baz.id",
                    "cerner.foo2bar.foo_id = mapping.baz.foo2bar_foo_id",
                    "cerner.foo2bar.bar_id = mapping.baz.foo2bar_bar_id",
                    "foo.id = foo2bar.foo_id",
                    "bar.id = foo2bar.bar_id",
                ]
            ),
            source=[
                Table("baz", "mapping"),
                Table("foo", "cerner"),
                Table("bar", "cerner"),
                Table("foo2bar", "cerner"),
            ],
        )
        assert expected == statements[0]

        expected = UpdateStatement(
            column=Column("beta", Table("baz", "omop")),
            expression="CASE WHEN foo.beta > bar.beta THEN foo.beta ELSE bar.beta END",
            criterion=Criterion(
                [
                    "omop.baz.id = mapping.baz.id",
                    "cerner.foo2bar.foo_id = mapping.baz.foo2bar_foo_id",
                    "cerner.foo2bar.bar_id = mapping.baz.foo2bar_bar_id",
                    "foo.id = foo2bar.foo_id",
                    "bar.id = foo2bar.bar_id",
                ]
            ),
            source=[
                Table("baz", "mapping"),
                Table("foo", "cerner"),
                Table("bar", "cerner"),
                Table("foo2bar", "cerner"),
            ],
        )
        assert expected == statements[1]

        expected = UpdateStatement(
            column=Column("gamma", Table("baz", "omop")),
            expression="CASE WHEN foo.gamma > bar.gamma THEN foo.gamma ELSE bar.gamma END",
            criterion=Criterion(
                [
                    "omop.baz.id = mapping.baz.id",
                    "cerner.foo2bar.foo_id = mapping.baz.foo2bar_foo_id",
                    "cerner.foo2bar.bar_id = mapping.baz.foo2bar_bar_id",
                    "foo.id = foo2bar.foo_id",
                    "bar.id = foo2bar.bar_id",
                ]
            ),
            source=[
                Table("baz", "mapping"),
                Table("foo", "cerner"),
                Table("bar", "cerner"),
                Table("foo2bar", "cerner"),
            ],
        )
        assert expected == statements[2]

    @skip_if_no_db
    def test_execute(self, postgresql):
        self.execute(postgresql)

        cur = postgresql.cursor()

        cur.execute("SELECT alpha, beta, gamma FROM omop.baz")
        actual = cur.fetchall()

        expected = [("a", 4, 4), ("c", 6, 5)]

        assert expected == actual


class TestEventTable(BaseTable):
    def parse(self):
        return load_table("event.yaml")

    def test_generate_create_statement(self):
        statements = self.generate()
        actual = statements[0]

        expected = "create table mapping.events (id serial PRIMARY KEY, event_id integer null);"

        assert actual == expected

    def test_generate_insert_statements(self):
        statements = self.generate()[1:3]

        expected = (
            "insert into mapping.events (event_id) "
            "select event.id as event_id "
            "from cerner.event;"
        )
        assert statements[0] == expected

        expected = (
            "insert into omop.events (id) select mapping.events.id from mapping.events;"
        )
        assert statements[1] == expected

    def test_generate_update_statements(self):
        statements = self.generate()[3:]

        expected_parts = [
            "update omop.events set staff_id = mapping.person.id",
            "from mapping.events, cerner.event, mapping.person",
            "where (omop.events.id = mapping.events.id)",
            "and (cerner.event.id = mapping.events.event_id)",
            "and (mapping.person.staff_id is not null)",
            "and (mapping.person.staff_id = event.staff_id);",
        ]
        expected = " ".join(expected_parts)
        assert expected == statements[0]

        update = "update omop.events set patient_id = mapping.person.id"
        cond_a = "and (mapping.person.patient_id is not null)"
        cond_b = "and (mapping.person.patient_id = event.patient_id);"
        expected_parts[0] = update
        expected_parts[-2] = cond_a
        expected_parts[-1] = cond_b
        expected = " ".join(expected_parts)
        assert statements[1] == expected

    def test_translate_create_statement(self):
        statements, env = self.translate()
        actual = statements[0]
        expected = CreateTableStatement(
            "id",
            Table("events", "mapping"),
            (ColumnDefinition("event_id", "integer"),),
        )
        assert actual == expected

    def test_translate_insert_statements(self):
        statements, env = self.translate()
        statements = statements[1:3]
        assert isinstance(statements[0], InsertFromStatement)
        assert isinstance(statements[1], InsertFromStatement)

        expected = InsertFromStatement(
            ["event_id"],
            Table("events", "mapping"),
            SelectStatement(["event.id as event_id",], [Table("event", "cerner")],),
        )
        assert expected == statements[0] or expected == statements[1]

        expected = InsertFromStatement(
            ["id"],
            Table("events", "omop"),
            SelectStatement(["mapping.events.id"], [Table("events", "mapping")]),
        )
        assert expected == statements[0] or expected == statements[1]

    def test_translate_update_statements(self):
        statements, env = self.translate()
        statements = statements[3:]
        assert isinstance(statements[0], UpdateStatement)
        assert isinstance(statements[1], UpdateStatement)

        expected = UpdateStatement(
            column=Column("staff_id", Table("events", "omop")),
            expression="mapping.person.id",
            criterion=Criterion(
                [
                    "omop.events.id = mapping.events.id",
                    "cerner.event.id = mapping.events.event_id",
                    "mapping.person.staff_id is not null",
                    "mapping.person.staff_id = event.staff_id",
                ]
            ),
            source=[
                Table("events", "mapping"),
                Table("event", "cerner"),
                Table("person", "mapping"),
            ],
        )
        assert expected == statements[0]

        expected = UpdateStatement(
            column=Column("patient_id", Table("events", "omop")),
            expression="mapping.person.id",
            criterion=Criterion(
                [
                    "omop.events.id = mapping.events.id",
                    "cerner.event.id = mapping.events.event_id",
                    "mapping.person.patient_id is not null",
                    "mapping.person.patient_id = event.patient_id",
                ]
            ),
            source=[
                Table("events", "mapping"),
                Table("event", "cerner"),
                Table("person", "mapping"),
            ],
        )
        assert expected == statements[1]

    @skip_if_no_db
    def test_execute(self, postgresql):
        cur = postgresql.cursor()
        mapping_person = """
        create table mapping.person (id INTEGER, staff_id INTEGER, patient_id INTEGER, primary key (id));
        insert into mapping.person (id, staff_id) values (0, 101);
        insert into mapping.person (id, staff_id) values (1, 456);
        insert into mapping.person (id, staff_id) values (2, 457);
        insert into mapping.person (id, patient_id) values (3, 100);
        insert into mapping.person (id, patient_id) values (4, 456);
        insert into mapping.person (id, patient_id) values (5, 749);
        insert into mapping.person (id, patient_id) values (6, 999);
        """

        with postgresql.cursor() as cur:
            cur.execute(mapping_person)
        postgresql.commit()

        self.execute(postgresql)

        cur = postgresql.cursor()

        cur.execute("SELECT id, staff_id, patient_id FROM omop.events order by id")
        actual = cur.fetchall()

        expected = [(1, 1, 4), (2, 2, 4), (3, 0, 3), (4, None, 6)]

        assert expected == actual


class TestConstantTable(BaseTable):
    def parse(self):
        return load_table("constant.yaml")

    def test_generate_create_statement(self):
        statements = self.generate()
        actual = statements[0]

        expected = (
            "create table mapping.baz (id serial PRIMARY KEY, foo_id integer null);"
        )

        assert actual == expected

    def test_generate_insert_statements(self):
        statements = self.generate()[1:3]

        expected = (
            "insert into mapping.baz (foo_id) "
            "select foo.id as foo_id "
            "from cerner.foo;"
        )
        assert statements[0] == expected

        expected = "insert into omop.baz (id) select mapping.baz.id from mapping.baz;"
        assert statements[1] == expected

    def test_generate_update_statements(self):
        statements = self.generate()[3:]

        expected = "update omop.baz set alpha = 'alpha';"
        assert expected == statements[0]

        expected = "update omop.baz set beta = '1';"
        assert statements[1] == expected

        expected = "update omop.baz set gamma = '2';"
        assert statements[2] == expected

    def test_translate_create_statement(self):
        statements, env = self.translate()
        actual = statements[0]
        expected = CreateTableStatement(
            "id", Table("baz", "mapping"), (ColumnDefinition("foo_id", "integer"),),
        )
        assert actual == expected

    def test_translate_insert_statements(self):
        statements, env = self.translate()
        statements = statements[1:3]
        assert isinstance(statements[0], InsertFromStatement)
        assert isinstance(statements[1], InsertFromStatement)

        expected = InsertFromStatement(
            ["foo_id"],
            Table("baz", "mapping"),
            SelectStatement(["foo.id as foo_id",], [Table("foo", "cerner")],),
        )
        assert expected == statements[0] or expected == statements[1]

        expected = InsertFromStatement(
            ["id"],
            Table("baz", "omop"),
            SelectStatement(["mapping.baz.id"], [Table("baz", "mapping")]),
        )
        assert expected == statements[0] or expected == statements[1]

    def test_translate_update_statements(self):
        statements, env = self.translate()
        statements = statements[3:]
        assert isinstance(statements[0], UpdateStatement)
        assert isinstance(statements[1], UpdateStatement)

        expected = UpdateStatement(
            column=Column("alpha", Table("baz", "omop")), expression="'alpha'",
        )
        assert expected == statements[0]

        expected = UpdateStatement(
            column=Column("beta", Table("baz", "omop")), expression="'1'",
        )
        assert expected == statements[1]

        expected = UpdateStatement(
            column=Column("gamma", Table("baz", "omop")), expression="'2'",
        )
        assert expected == statements[2]

    @skip_if_no_db
    def test_execute(self, postgresql):
        self.execute(postgresql)

        cur = postgresql.cursor()
        expected = [("alpha", 1, 2) for i in range(3)]

        cur.execute("SELECT alpha, beta, gamma FROM omop.baz")
        actual = cur.fetchall()

        assert expected == actual


class TestMergeTable(BaseTable):
    def parse(self):
        return load_table("merge.yaml")

    def test_generate_create_statement(self):
        statements = self.generate()
        actual = statements[0]

        expected = "create table mapping.baz (id serial PRIMARY KEY, foo_id integer null, bar_id integer null);"

        assert actual == expected

    def test_generate_insert_statements(self):
        statements = self.generate()[1:4]
        expected = (
            "insert into mapping.baz (foo_id) select foo.id as foo_id from cerner.foo;"
        )
        assert statements[0] == expected

        expected = (
            "insert into mapping.baz (bar_id) select bar.id as bar_id from cerner.bar;"
        )
        assert statements[1] == expected

        expected = "insert into omop.baz (id) select mapping.baz.id from mapping.baz;"
        assert statements[2] == expected

    def test_generate_update_statements(self):
        statements = self.generate()[4:]

        expected = lambda t, c: (
            f"update omop.baz set {c} = {t}.{c} from mapping.baz, cerner.{t} "
            f"where (omop.baz.id = mapping.baz.id) and (cerner.{t}.id = mapping.baz.{t}_id);"
        )

        assert statements[0] == expected("foo", "alpha")
        assert statements[1] == expected("foo", "beta")
        assert statements[2] == expected("foo", "gamma")

        assert statements[3] == expected("bar", "alpha")
        assert statements[4] == expected("bar", "beta")
        assert statements[5] == expected("bar", "gamma")

    def test_translate_create_statement(self):
        statements, env = self.translate()
        actual = statements[0]
        expected = CreateTableStatement(
            "id",
            Table("baz", "mapping"),
            (
                ColumnDefinition("foo_id", "integer"),
                ColumnDefinition("bar_id", "integer"),
            ),
        )
        assert actual == expected

    def test_translate_insert_statements(self):
        statements, env = self.translate()
        statements = statements[1:4]
        expected = InsertFromStatement(
            ["foo_id"],
            Table("baz", "mapping"),
            SelectStatement(["foo.id as foo_id"], [Table("foo", "cerner")]),
        )
        assert expected == statements[1] or expected == statements[0]

        expected = InsertFromStatement(
            ["bar_id"],
            Table("baz", "mapping"),
            SelectStatement(["bar.id as bar_id"], [Table("bar", "cerner")]),
        )
        assert expected == statements[1] or expected == statements[0]

        expected = InsertFromStatement(
            ["id"],
            Table("baz", "omop"),
            SelectStatement(["mapping.baz.id"], [Table("baz", "mapping")]),
        )
        assert expected == statements[2]

    def test_translate_update_statements(self):
        statements, env = self.translate()
        statements = statements[4:]
        expected = lambda t, c: UpdateStatement(
            column=Column(c, Table("baz", "omop")),
            expression=f"{t}.{c}",
            criterion=Criterion(
                ["omop.baz.id = mapping.baz.id", f"cerner.{t}.id = mapping.baz.{t}_id"]
            ),
            source=[Table("baz", "mapping"), Table(t, "cerner")],
        )
        assert statements[0] == expected("foo", "alpha")
        assert statements[1] == expected("foo", "beta")
        assert statements[2] == expected("foo", "gamma")

        assert statements[3] == expected("bar", "alpha")
        assert statements[4] == expected("bar", "beta")
        assert statements[5] == expected("bar", "gamma")

    @skip_if_no_db
    def test_execute(self, postgresql):
        self.execute(postgresql)
        cur = postgresql.cursor()

        cur.execute("SELECT id, alpha, beta, gamma FROM omop.baz order by id")
        actual = cur.fetchall()
        expected = [
            (1, "a", 4, 2),
            (2, "c", 5, 5),
            (3, "d", 9, 7),
            (4, "x", 8, 3),
            (5, "a", 4, 4),
            (6, "c", 6, 5),
        ]

        assert expected == actual
