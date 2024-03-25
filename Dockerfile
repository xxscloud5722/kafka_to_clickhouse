FROM rust:1.77.0-slim AS builder
RUN apt update && apt install cmake git -y && cd /opt && git clone https://github.com/xxscloud5722/kafka_to_clickhouse.git && cd kafka_to_clickhouse && cargo build --release

FROM alpine:3.19.1
COPY --from=builder /opt/kafka_to_clickhouse/release/log2click /usr/local/bin/log2click
WORKDIR /usr/local/bin
CMD ["log2click", "-c", "/etc/log2click.yaml"]