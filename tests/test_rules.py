import os
from pathlib import Path

import pytest
from omop_etl import TargetTable
from pandas.api.types import is_numeric_dtype
from pytest_postgresql import factories
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from tests.utils import *


def load_rule(name):
    fn = os.path.join(".", "validation", f"{name}.yaml")
    with open(fn) as f:
        return TargetTable.parse_string(f.read())


RULE_DEPENDENCIES = {
    "person": [],
    "condition_occurrence": ["person"],
    "location": [],
    "visit_occurrence": ["person"],
}


postgresql = factories.postgresql(
    "postgresql_proc",
    load=[
        Path("schema", "cerner.sql"),
        Path("schema", "omop.sql"),
        Path("schema", "external.sql"),
    ],
)


def init_table(db, table):
    dependencies = RULE_DEPENDENCIES[table]
    with db.cursor() as cur:
        for rule in dependencies:
            sql = load_rule(rule).get_script()
            cur.execute(sql)
        db.commit()
        sql = load_rule(table).get_script()
        cur.execute(sql)
        db.commit()


@pytest.mark.parametrize("table", RULE_DEPENDENCIES)
def test_parse_table(table):
    table = load_rule(table)


@pytest.mark.parametrize("table", RULE_DEPENDENCIES)
def test_translate_table(table):
    table = load_rule(table)
    table.translate()


@pytest.mark.parametrize("table", RULE_DEPENDENCIES)
def test_generate_table(table):
    table = load_rule(table)
    table.get_script()


@skip_if_no_db
@pytest.mark.parametrize("table", RULE_DEPENDENCIES)
def test_execute_table(table, postgresql):
    init_table(postgresql, table)


def init_validation(engine):
    fn = Path("tests", "data", "mapping_test_data.xlsx")
    sheets: Dict[str, pd.DataFrame] = {
        t: df
        for t, df in pd.read_excel(fn, sheet_name=None).items()
        if t not in {"tests", "EXTERNAL.FACILITY_POSTCODE"}
    }
    source = {
        t: sheets[t]
        for t in [
            "PERSON",
            "ENCOUNTER",
            "ENCNTR_LOC_HIST",
            "DIAGNOSIS",
            "PROBLEM",
            "ADDRESS",
            "NOMENCLATURE",
            "CODE_VALUE",
            "OMOP.CONCEPT",
            "OMOP.CONCEPT_RELATIONSHIP",
        ]
    }

    targets = [
        "OMOP.PERSON",
        "OMOP.VISIT_OCCURRENCE",
        "OMOP.CONDITION_OCCURRENCE",
        "OMOP.LOCATION",
    ]
    target = dict()
    for t in targets:
        df = sheets[t]
        t = t.split(".")[-1].lower()
        cols = [c for c in df.columns if "Unnamed" not in c]
        df = df[cols]
        df = df.dropna(how="all")
        df.columns = [c.split(".")[-1] for c in df.columns]
        target[t] = df

    for t, df in source.items():
        t = t.lower()
        if "." in t:
            s, t = t.split(".")
        else:
            s = "cerner"
        cols = [c for c in df.columns if "Unnamed" not in c]
        df = df[cols]
        df = df.dropna(how="all")
        df.columns = [c.split(".")[-1] for c in df.columns]
        df.to_sql(t, engine, schema=s, index=False, if_exists="append")
    for t in ["FACILITY_POSTCODE", "PERSON_ETHNICITY_CONCEPT"]:
        df = pd.read_csv(f"external/{t}.csv")
        df.columns = [c.split(".")[-1].lower() for c in df.columns]
        df.to_sql(t.lower(), engine, schema="external", index=False, if_exists="append")

    return target


@skip_if_no_db
@pytest.mark.parametrize(
    "table,column",
    [
        ("condition_occurrence", "condition_concept_id"),
        ("condition_occurrence", "condition_occurrence_id"),
        ("condition_occurrence", "person_id"),
        ("location", "location_id"),
        ("location", "state"),
        ("location", "zip"),
        ("person", "death_datetime"),
        ("person", "gender_source_concept_id"),
        ("person", "gender_source_value"),
        ("person", "person_id"),
        ("person", "year_of_birth"),
        ("visit_occurrence", "person_id"),
        ("visit_occurrence", "visit_occurrence_id"),
    ],
)
def test_validation_tests(table, column, postgresql):
    cur = postgresql.cursor()
    cur.execute("SET search_path TO cerner;")

    postgresql.commit()

    connection = f"postgresql+psycopg2://{postgresql.info.user}:password@{postgresql.info.host}:{postgresql.info.port}/{postgresql.info.dbname}"
    engine = create_engine(connection, echo=False, poolclass=NullPool)
    expected_df = init_validation(engine)[table]
    init_table(postgresql, table)

    actual_df = pd.read_sql(
        f"select * from omop.{table} order by {expected_df.columns[0]}", engine
    )
    if is_numeric_dtype(expected_df[column]):
        actual = list(actual_df[column].astype(float))
        expected = list(expected_df[column].astype(float))
    else:
        actual = list(actual_df[column])
        expected = list(expected_df[column])

    assert actual == expected

