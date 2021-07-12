FROM continuumio/miniconda3

WORKDIR /app

COPY environment.lock.yml environment.yml

RUN conda env update -q -f environment.yml -n base

ADD . /app

EXPOSE 8000

CMD [ "uvicorn", "omop_etl.api:app", "--host", "0.0.0.0"]