# OMOP-ETL

## Extract, Transform, Load Framework for the Conversion of Health Databases to OMOP
Quiroz, Juan C. and Chard, Tim and Sa, Zhisheng and Ritchie, Angus and Jorm, Louisa and Gallego, Blanca

Paper: https://doi.org/10.1101/2021.04.08.21255178

### Abstract
**Objective**: Develop an extract, transform, load (ETL) framework for the conversion of health databases to the Observational Medical Outcomes Partnership Common Data Model (OMOP CDM) that supports transparency of the mapping process, readability, refactoring, and maintainability.

**Materials and Methods**: We propose an ETL framework that is metadata-driven and generic across source datasets.  The ETL framework reads mapping logic for OMOP tables from YAML files, which organize SQL snippets in key-value pairs that define the extract and transform logic to populate OMOP columns. 

**Results**: We developed a data manipulation language (DML) for writing the mapping logic from health datasets to OMOP, which defines mapping operations on a column-by-column basis. A core ETL pipeline converts the DML into YAML files and generates an ETL script. We provide access to our ETL framework via a web application, allowing users to upload and edit YAML files and obtain an ETL SQL script that can be used in development environments.  

**Discussion**: The structure of the DML and the mapping operations defined in column-by-column operations maximizes readability, refactoring, and maintainability, while minimizing technical debt, and standardizes the writing of ETL operations for mapping to OMOP. Our web application allows institutions and teams to reuse the ETL pipeline by writing their own rules using our DML. 
**Conclusion**: The research community needs tools that reduce the cost and time effort needed to map datasets to OMOP. These tools must support transparency of the mapping process for mapping efforts to be reused by different institutions.

## Installation

The quickest way to get started is to use our web application which can be found at [www.omop.link](https://www.omop.link) and can be used without any installation.
However, if you would like to use OMOP-ETL in an environment that does not have access to the internet, there are two easy options: Docker and Conda.

In any case, the first step is to clone the repository:
```
git clone https://github.com/clinical-ai/omop-etl.git
cd omop-etl
```

### Conda

1. If not already installed, install either [Miniconda](https://docs.conda.io/en/latest/miniconda.html) or [Anaconda](https://www.anaconda.com/products/individual#Downloads):

2. Create a conda virtual environment:
    ```
    conda env create --file environment.lock.yml --name omop-etl
    ```

3. Activate the virtual environment:
    ```
    conda activate omop-etl
    ```

### Docker 

1. If not already installed, install [docker](https://docs.docker.com/install/) 

2. Build the docker image:
    ```
    docker build -t omop-etl .
    ```
### Testing

After completing the installation, you will be able to run the tests with the following command
```bash
python -m py.test 
```
However, some of the tests require a PostgreSQL database and these will be skipped if one is not present.
It is possible to start a postgres database with a single command using docker:
```bash
docker run --rm --name omop_etl_test_db -e POSTGRES_PASSWORD=password  -p 5432:5432 -d postgres
```

Alternatively, you can configure the test runner to use an existing database by editing the `_PG_CONNECTION` in the `./tests/utils.py` file.

## Getting started
There are two different ways to use the OMOP-ETL, a command-line interface and a web API.
Both of these will compile YAML files into a separate SQL script that you can run against your database.

The choice that you make will depend on how you would like to interact with the service.
If you are looking to include the framework in another language you might consider using the web API otherwise the command line interface might be better.

### Command Line Interface


To compile your YAML files run the following command after changing the paths for the rules and the output. 
 ```
 python main.py compile --rules ./validation --output ./output
 ```

If you have installed OMOP-ETL with docker then you will need to mount the folders such in the command below which mounts the validation and output folders in the current working directory.
```bash
docker run \
    -v $PWD/validation:/app/validation \
    -v $PWD/output:/app/sql \
    omop-etl python main.py compile --rules validation
```

### Web API

Unlike the command-line interface, the web API does not compile YAML files directly.
Instead, it accepts JSON objects with the same schema as we have defined below.

The web api provides one endpoint `http://127.0.0.1:8000/api/compile`.
It can be run with docker by executing the following:
```
docker run -p 8000:8000 omop-etl
```
or with the command-line interface:
```
uvicorn main:api
```

The web API provides a [Swagger-UI](https://swagger.io/) that can be accessed at http://127.0.0.1:8000/docs to test the API interactively.


## Language

The OMOP-ETL combines YAML with SQL allowing simple configuration without limiting the flexibility of mapping logic.
Each file defines the mapping process for a single OMOP table.
The file defines the source data, the target OMOP table and the transformation logic to map from source data to OMOP.
Each YAML file contains three top-level fields: (1) name of the OMOP table being mapped (`name`), (2) definition of primary keys used by the ETL framework to manage the load (insert) operations (`primary_key`), and (3) mapping rules for each column in the targeted OMOP table (`columns`).

While OMOP-ETL is designed to specifically convert to the OMOP CDM, it is possible to target arbitrary database schemas.
For instance, below we have a very simple `foo` table is generated from the `bar` source table.



``` yaml
name: foo

primary_key:
  name: id``
  sources:
    BAR_PK:
      table: bar
      columns:
        id: bigint

columns:
  - name: baz
    tables:
      - bar
    expression: bar.foo_bar

```

### Primary Keys

``` yaml
name: id
sources:
  BAR_PK:
    table: bar
    columns:
      id: bigint
    constrains:
      - TRUE
```

The first step in our ETL process is to map every row in the OMOP table to all of the relevant rows in the source tables.
We use the `primary_key` field to define how this process takes place.
The `primary_key` has only two fields: the `name` of the primary key in the OMOP table and the `sources` from which the primary key will be generated.
For simple datasets, there may be a one-to-one relationship between source and OMOP tables.
For instance, in the example above, we are the tables with a one-to-one relationship between the primary keys of the tables so that the `bar.id` is mapped onto the primary key to the OMOP table.

To handle composite keys and arbitrary data types, we generate an intermediate "mapping" table which is populated in the order that the primary keys are defined (if there is more than one primary key source).
The "mapping" table is defined in the `MAPPING` schema and has the same name as the OMOP table.
For instance, when mapping the PERSON table, the `MAPPING.PERSON` table will generated and will map the rows from each of the primary key `sources` to exactly one row in `OMOP.PERSON`.

The `sources` field is a collection of key-value pairs, where the key is the alias that is used in the `primary_key` field of the `column` and the value is made up of three different fields.
The `table` can either be the name of the source table or a Query Table (described shortly).
The `columns` defines all of the columns that are necessary to create a unique relationship between the source `table` and the target table.
Finally, the `constraints` is an optional field that can be used to only select a subset of the rows from the source table and the OMOP table will only contain the rows where all of the constraints are satisfied.
  
### Columns

``` yaml
name: foo
tables: [event]
primary_key: event_pk
constraints:
  - TRUE
references:
  table: person
  column: staff_id
expression: event.staff_id
```

The `columns` field is a sequence of "columns" and defines how the rest of the transformation takes places.
Each "column" in `columns` represents the logic that is needed to transform a column into an OMOP table.

A "column" has six different fields, `name`, `tables`, `primary_key`, `constraints`,`references` and `expression`.
The `name` is the name of the field in the OMOP table.
`tables` defines all of the tables that are required to map the column and is a sequence that only contains table names and Query Tables.
The `primary_key` defines which primary key is used to identify rows.
The `constraints` field is to allow each defined column to apply to a subset of the rows in the final database.
`references` will convert foreign key references from the source database to agree with the newly created primary keys in the OMOP database.
Finally, the `expression` is a SQL expression that will generate the desired output for the column.

   
### Query Table

In some cases, the existing language features may not be flexible enough.
For instance, it is possible to use a nested subquery in the expression but for some queries, the database may not be able to execute these efficiently.
In these cases, you can fall back to SQL with the Query Table.
The Query Table can be used in the table field of the `primary_key` as well as one of the `tables` on a Column.
It has two fields, an `alias` and a `query` and forms an aliased nested query.
Essentially, we convert these into a nested subquery of the form `(<QUERY>) AS <ALIAS>` and therefore the `query` can be any table like query.

In the example below, we have created the Query Table `foo` and used a [YAML anchor](https://yaml.org/spec/1.2/spec.html#id2765878) with the name `foo_table`.
The query table is then being used in both the `table` field of the `primary_key` and as part of the `tables` field in the alpha `column`.


``` yaml
name: baz

variables:
  foo_table: &foo_table
    alias: foo
    query: select * from (values (0, 'a1', 1), (2, 'b1', 3), (4, 'c1', 5)) x(id, alpha, beta)

primary_key:
  name: id
  sources:
    foo:
      name: foo
      table: *foo_table
      columns:
        id: integer

columns:
  - name: alpha
    tables: [*foo_table]
    expression: foo.alpha
    primary_key: foo
```

## Citing OMOP-ETL
```
@article {Quiroz2021.04.08.21255178,
  author = {Quiroz, Juan C. and Chard, Tim and Sa, Zhisheng and Ritchie, Angus and Jorm, Louisa and Gallego, Blanca},
  title = {Extract, Transform, Load Framework for the Conversion of Health Databases to OMOP},
  elocation-id = {2021.04.08.21255178},
  year = {2021},
  doi = {10.1101/2021.04.08.21255178},
  publisher = {Cold Spring Harbor Laboratory Press},
  URL = {https://www.medrxiv.org/content/early/2021/05/28/2021.04.08.21255178},
  eprint = {https://www.medrxiv.org/content/early/2021/05/28/2021.04.08.21255178.full.pdf},
  journal = {medRxiv}
}

