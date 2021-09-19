from setuptools import setup

setup(
    name="omop-etl",
    version="1.0.0",
    description="ETL Tool for converting datasets to OMOP CDM",
    url="https://github.com/clinical-ai/omop-etl",
    author="Tim Chard",
    author_email="t.chard@unsw.edu.au",
    license="GPL-3.0",
    packages=["omop_etl"],
    entry_points={"console_scripts": ["omop_etl = omop_etl.__main__:app"],},
    install_requires=[
        "fastapi",
        "jsonschema",
        "openpyxl",
        "pandas",
        "psycopg2-binary",
        "pydantic",
        "pyyaml",
        "sqlalchemy",
        "tqdm",
        "typer",
        "uvicorn[standard]",
        "xlrd",
    ],
    extras_require={"dev": ["pytest-postgresql >= 2.6.1", "pytest"]},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
)

