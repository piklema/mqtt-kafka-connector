FROM python:3.11-alpine as builder

ENV VIRTUAL_ENV=/usr/local

RUN apk update && apk add gcc \
                          libc-dev \
                          zlib-dev

ADD https://astral.sh/uv/install.sh /install.sh
RUN chmod -R 655 /install.sh && /install.sh && rm /install.sh

COPY requirements.txt .
RUN /root/.cargo/bin/uv pip install --system --no-cache -r requirements.txt

FROM python:3.11-alpine

ARG RELEASE_VERSION
ENV RELEASE_VERSION=${RELEASE_VERSION}

COPY --from=builder /usr/local /usr/local
COPY . /app
WORKDIR /app

RUN pip install -e .

ENTRYPOINT ["mqtt_kafka_connector"]
