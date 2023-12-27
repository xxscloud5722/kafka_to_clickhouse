package main

import (
	"kafka-to-clickhouse/src/app"
	"kafka-to-clickhouse/src/pipeline"
	"os"
)

func main() {
	args := os.Args
	var fileName string
	if len(args) > 1 {
		fileName = args[1]
	} else {
		fileName = "config.yaml"
	}
	app.SetConfig(fileName)
	pipeline.Connect()
}
