package main

import (
	"kafka-to-clickhouse/src/app"
	"kafka-to-clickhouse/src/pipeline"
)

func main() {
	app.SetConfig("config.yaml")
	pipeline.Connect()
}
