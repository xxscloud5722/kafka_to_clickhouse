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
[latest version 1.0.1](https://github.com/xxscloud5722/kafka_to_clickhouse/releases)

**Program compilation**
- Goalng 1.20

```bash
# windows OR linux
go env -w GOOS=linux
go mod tidy
go build -o ./dist/log_sync src/main.go
```

# Configuration instructions
```yaml
# Kafka Config
kafka:
  host: 10.10.1.5
  port: 9094
  consumer:
    group-id: default
# ClickHouse
clickhouse:
  host: 10.10.1.5
  port: 9000
  username: default
  password: *******
  database: logs
# Subscription Topic
topic:
  - test
# ClickHouse full table path
sink: logs.ding
# Table Field
fields: date, env, service_code, level, thread, class, message
# Golang Pattern Parse group
pattern: '(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+) (\w+) \[([^]]+)\] ([^:]+) : (.+[\s\S]*)'
# Parse Field mapping
pattern-index:
  - date
  - level
  - thread
  - class
  - message
debug: true
# Debug true, Log output format, Use ${} for variables
output:
  # ${env}/${service_code}
  format: '${env}/${service_code} ${date} ${level} [${thread}] ${class} : ${message}'

```

# LogSync Demo
Confirm that the program has been downloaded or programming is completed.

```bash
# start 
log_sync config.yaml
```

# fluent-bit Demo
```bash
# fluent-bit.conf
[SERVICE]
    Flush         1
    Log_Level     info
    Daemon        off
    Parsers_File  parser.conf


[INPUT]
    Name         tail
    Path         /opt/logs.log
    Tag          file_logs
    Refresh_Interval 1
    multiline.parser java_parser

[FILTER]
    Name record_modifier
    Match *
    # Add Value
    Record service_code [value]
    Record env [value]


# Debug
[OUTPUT]
    Name stdout
    Match *

# Kafka
[OUTPUT]
    Name kafka
    Match *
    Brokers [kafka address]
    Topics [kafka topic]
    Timestamp_Key @timestamp
    
    
# ================================

# parser.conf
[MULTILINE_PARSER]
    name          java_parser
    type          regex
    flush_timeout 1000
    #
    # Regex rules for multiline parsing
    # ---------------------------------
    #
    # configuration hints:
    #
    #  - first state always has the name: start_state
    #  - every field in the rule must be inside double quotes
    #
    # rules |   state name  | regex pattern                  | next state
    # ------|---------------|--------------------------------------------
    rule      "start_state"   "/(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}).*$/"             "cont"
    rule      "cont"          "/^(?!\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)[a-zA-Z\s\S].*/" "cont"
```

# Contributors

Thanks for your contributions!

- [@xiaoliuya](https://github.com/xxscloud5722/)


# Zen
Don't let what you cannot do interfere with what you can do.
