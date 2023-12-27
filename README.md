# Kafka to Clickhouse

`New` generation logging solution.

Based on kafka's natural concurrency advantages and Go portability, the developed log synchronizer is simpler than Flink

- No messages lost
- Simple little configuration
- Quick deployment and operation

# Dependency requirements

- `github.com/ClickHouse/clickhouse-go`
- `github.com/fatih/color`
- `github.com/IBM/sarama`

Minimum Supported Golang Version is 1.20.


# Getting started

**Download package**
[latest version 2.0.1](https://github.com/xxscloud5722/kafka_to_clickhouse/releases)

**Program compilation**
- Goalng 1.20

```bash
# windows OR linux
go env -w GOOS=linux
go mod tidy
go build -o ./dist/log_sync src/main.go
```

# Configuration instructions
```

```

# Easy Demo
Confirm that the program has been downloaded or programming is completed.

```bash
# start 
log_sync
```

# Contributors

Thanks for your contributions!

- [@xiaoliuya](https://github.com/xxscloud5722/)


# Zen
Don't let what you cannot do interfere with what you can do.
