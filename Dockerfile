FROM python:3.11.0-alpine

COPY connector /app
COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip install --no-cache-dir -r /app/requirements.txt

ENTRYPOINT ["python", "main.py"]
