FROM python:3.10.2-slim

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /usr/src/app

COPY requirements.txt .
RUN python -m pip install --upgrade pip && \
    pip install -r requirements.txt && \
    rm requirements.txt

COPY etl/etc etc/
COPY etl/*.py .

EXPOSE 5432:5432
EXPOSE 9200:9200
EXPOSE 9300:9300

ENTRYPOINT ["python", "./loader.py"]
