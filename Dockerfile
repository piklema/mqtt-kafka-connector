FROM python:3.11.0-alpine as builder

RUN apk update && apk add gcc \
                          libc-dev \
                          zlib-dev

ENV VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

ADD https://astral.sh/uv/install.sh /install.sh
RUN chmod -R 655 /install.sh && /install.sh && rm /install.sh

COPY . /app
WORKDIR /app

RUN /root/.cargo/bin/uv venv ${VIRTUAL_ENV} && \
    /root/.cargo/bin/uv pip install --no-cache -r requirements.txt && \
    /root/.cargo/bin/uv pip install -e .


FROM python:3.11.0-alpine

ARG RELEASE_VERSION
ENV RELEASE_VERSION=${RELEASE_VERSION}

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

ENTRYPOINT ["mqtt_kafka_connector"]
