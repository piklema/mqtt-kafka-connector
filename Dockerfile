FROM python:3.11.0-alpine as builder

COPY . /app
WORKDIR /app

RUN apk update && apk add gcc \
                          libc-dev \
                          zlib-dev

ADD --chmod=755 https://astral.sh/uv/install.sh /install.sh

RUN /install.sh && \
    rm /install.sh && \
    /root/.cargo/bin/uv venv && \
    /root/.cargo/bin/uv pip install --no-cache -r requirements.txt

FROM python:3.11.0-alpine

ARG RELEASE_VERSION
ENV RELEASE_VERSION=${RELEASE_VERSION}
ENV PIP_ROOT_USER_ACTION=ignore
ENV VIRTUAL_ENV=/app/.venv

COPY . /app
WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /root/.cargo/bin/uv /root/.cargo/bin/uv

RUN /root/.cargo/bin/uv pip install -e .

ENTRYPOINT ["mqtt_kafka_connector"]
