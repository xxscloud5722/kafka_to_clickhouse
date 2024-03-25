#FROM rust:1.77.0-slim AS builder
#RUN apt update && apt install cmake git pkg-config libssl-dev g++ -y && \
#    cd /opt && git clone https://github.com/xxscloud5722/kafka_to_clickhouse.git && \
#    cd kafka_to_clickhouse && \
#    cargo build --release

FROM debian:bookworm-slim
ADD target/release/log2click /usr/local/bin/log2click
RUN apt update && apt install libssl-dev -y
WORKDIR /usr/local/bin
CMD ["log2click", "-c", "/etc/log2click.yaml"]