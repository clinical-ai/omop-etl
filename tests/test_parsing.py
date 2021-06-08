import os

import pytest
from pydantic import ValidationError
from schema import *


def load_rule(name):
    fn = os.path.join(".", "rules", f"cerner2omopv6_{name}.yaml")
    with open(fn) as f:
        return TargetTable.parse_string(f.read())


def test_parse_primary_key():
    yaml = """
    name: specimen_id
    sources:
      V500_SPECIMEN_PK:
        table: V500_SPECIMEN
        columns:
          specimen_id: bigint
    """

    expected = PrimaryKey(
        name="specimen_id",
        sources={
            "V500_SPECIMEN_PK": PrimaryKeySource(
                name="V500_SPECIMEN_PK",
                table="V500_SPECIMEN",
                columns={"specimen_id": "bigint"},
            )
        },
    )

    actual = PrimaryKey.parse_string(yaml)
    assert actual == expected


def test_parse_no_table_name():
    yml = """
primary_key:
  name: id
  sources:
    foo:
      foo:
        id: char
columns:
- column:
  source: foo
  name: foo_id
    """
    with pytest.raises(ValidationError):
        TargetTable.parse_string(yml)


def test_zero_columns():
    yml = """
name: baz
primary_key:
  name: id
  sources:
    foo:
      foo:
        id: char
columns:
    """
    with pytest.raises(ValidationError):
        TargetTable.parse_string(yml)


def test_no_columns():
    yml = """
name: baz
primary_key:
  name: id
  sources:
    foo:
      foo:
        id: char
    """
    with pytest.raises(ValidationError):
        TargetTable.parse_string(yml)


def test_simple_table_schema():
    yml = """
name: baz
primary_key:
  name: id
  sources:
    foo:
      table: foo
      columns:
        id: char
columns:
- column:
  tables: [foo]
  expression: foo.foo_id
  name: foo_id
  primary_key: foo
"""
    TargetTable.parse_string(yml)


def test_disjoint_table_schema():
    yml = """
name: baz
primary_key:
  name: id
  sources:
    foo:
      table: foo
      columns:
        id: char
    bar:
      table: bar
      columns:
        id: char
columns:
- column:
  tables: [foo]
  expression: foo.foo_id
  name: foo_id
  primary_key: foo
    """
    TargetTable.parse_string(yml)


def test_partial_pk_table_schema():
    yml = """
name: baz
primary_key:
  name: id
  sources:
    foo:
      table: foo
      columns:
        partial_a: char
        partial_b: char
columns:
- column:
  tables: [foo]
  expression: foo.foo_id
  name: foo_id
  primary_key: foo

    """
    TargetTable.parse_string(yml)


def test_disjoint_partial_table_schema():
    yml = """
name: baz
primary_key:
  name: id
  sources:
    foo:
      table: foo
      columns:
        partial_a: char
        partial_b: char
    bar:
      table: bar
      columns:
        id: char
columns:
- column:
  tables: [foo]
  expression: foo.foo_id
  name: foo_id
  primary_key: foo

    """
    TargetTable.parse_string(yml)


def test_copy_table_schema():
    yml = """
name: baz
primary_key:
  name: id
  sources:
    foo:
      table: foo
      columns:
        id: char
columns:
- column:
  tables: [foo]
  expression: foo.foo_id
  name: foo_id
  primary_key: foo

    """
    TargetTable.parse_string(yml)


def test_mapping_table_schema():
    yml = """
name: baz
primary_key:
  name: id
  sources:
    foo:
      table: foo
      columns:
        id: char
columns:
- tables: [foo]
  name: foo_id
  expression: COUNT(*)
  primary_key: foo

"""
    TargetTable.parse_string(yml)


def test_constant_table_schema():
    yml = """
name: baz
primary_key:
  name: id
  sources:
    foo:
      table: foo
      columns:
        id: char
columns:
  - name: alpha
    constant: alpha
  - name: beta
    constant: beta
  - name: gamma
    constant: gamma
"""
    TargetTable.parse_string(yml)


def test_disabled_column_schema():
    yml = """
name: baz
primary_key:
  name: id
  sources:
    foo:
      table: foo
      columns:
        id: char
columns:
  - name: alpha
    enabled: false
    tables: [table_not_exists]
    expression: table_not_exists.alpha
    primary_key: not_exists_pk
  - name: gamma
    constant: gamma
"""
    TargetTable.parse_string(yml)


def test_simple_copy_column():
    yml = """
tables: [foo]
expression: foo.foo_id
name: foo_id
primary_key: foo
"""
    TargetColumn.parse_string(yml)


def test_simple_multi_mapping_column():
    yml = """
column:
name: alpha
tables:
  - foo
  - bar

constraints:
- foo.alpha=bar.beta

expression: foo.alpha
primary_key: foo
"""
    TargetColumn.parse_string(yml)


def test_simple_mapping_column():
    yml = """
column:
name: alpha
tables:
    - foo
expression: foo.alpha || 'baz'
primary_key: foo
"""
    TargetColumn.parse_string(yml)


def test_copy_column_missing_name():
    yml = """
tables: [foo]
expression: das
    """
    with pytest.raises(ValidationError):
        TargetColumn.parse_string(yml)


def test_copy_column_missing_expression():
    yml = """
tables: [foo]
name: foo_id
    """
    with pytest.raises(ValidationError):
        TargetColumn.parse_string(yml)


def test_copy_column_missing_tables():
    yml = """
column:
name: foo_id
expression: asd
    """
    with pytest.raises(ValidationError):
        TargetColumn.parse_string(yml)
