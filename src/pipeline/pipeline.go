package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/IBM/sarama"
	"github.com/fatih/color"
	"github.com/jmoiron/sqlx"
	"kafka-to-clickhouse/src/app"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"
)

func Connect() {
	// Create GL context.Context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Kafka Config
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	brokerList := []string{fmt.Sprintf("%s:%d", app.Get().Kafka.Host, app.Get().Kafka.Port)}

	// ClickHouse Config
	clickhouseURL := fmt.Sprintf("tcp://%s:%d?username=%s&password=%s&database=%s",
		app.Get().Clickhouse.Host, app.Get().Clickhouse.Port,
		app.Get().Clickhouse.Username, url.QueryEscape(app.Get().Clickhouse.Password),
		app.Get().Clickhouse.Database)
	// ClickHouse Connect
	clickhouseConn, err := sqlx.Connect("clickhouse", clickhouseURL)
	if err != nil {
		color.Red("[Config] Error opening ClickHouse connection: %v\n", err)
		return
	}
	defer func(connect *sqlx.DB) {
		_ = connect.Close()
	}(clickhouseConn)
	color.Green("[Config] Connect to `ClickHouse` Success ...")

	consumer, err := sarama.NewConsumerGroup(brokerList, app.Get().Kafka.Consumer.GroupId, kafkaConfig)
	if err != nil {
		color.Red("[Config] Error creating Kafka consumer: %v\n", err)
		return
	}
	defer func() {
		_ = consumer.Close()
	}()
	color.Green("[Config] Connect to `Kafka` Success ...")

	// Subscribe to Kafka
	handler := &LogConsumerHandler{
		Conn: clickhouseConn,
	}
	err = consumer.Consume(ctx, app.Get().Topic, handler)
	if err != nil {
		color.Red("[Config] Error subscribing to topics: %v\n", err)
		return
	}
	color.Blue("[Config] Subscribing to Topics : " + strings.Join(app.Get().Topic, ","))

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	// Release resources
	if err = consumer.Close(); err != nil {
		color.Red("Error closing consumer: %v", err)
	}
}

func toSQL(messageList []*sarama.ConsumerMessage) (error, string, [][]any) {
	var sql = "INSERT INTO " + app.Get().Sink + " ( " + app.Get().Fields + " ) \n VALUES \n ("
	for range app.Get().Columns() {
		sql += "?, "
	}
	sql = sql[0:len(sql)-2] + ")"
	var params [][]any
	for _, item := range messageList {
		var data map[string]any
		err := json.Unmarshal(item.Value, &data)
		if err != nil {
			continue
		}
		logEntry, ok := data["log"]
		if ok {
			regexpPattern := app.Get().Regexp()
			matches := regexpPattern.FindStringSubmatch(logEntry.(string))
			if matches == nil {
				color.Red("Regexp Error")
				log.Fatal(logEntry)
			}
			if len(matches) > len(app.Get().PatternIndex) {
				for i, key := range app.Get().PatternIndex {
					data[key] = strings.Trim(matches[i+1], "\n")
				}
			} else {
				continue
			}
		} else {
			continue
		}

		// Print
		if app.Get().Debug {
			debugLog := app.Get().Output.Format
			for key, value := range data {
				valueString, valueOk := value.(string)
				if valueOk {
					debugLog = strings.ReplaceAll(debugLog, "${"+key+"}", valueString)
				}
			}
			var level = data["level"]
			switch level {
			case "INFO":
				color.Green(debugLog)
				break
			case "WARN":
				color.Yellow(debugLog)
				break
			case "ERROR":
				color.Red(debugLog)
				break
			default:
				grey := color.New(color.FgWhite)
				_, _ = grey.Println(debugLog)
				break
			}
		}

		// Inject
		var values []any
		for _, key := range app.Get().Columns() {
			value, dataOk := data[key]
			if dataOk {
				values = append(values, value)
			} else {
				values = append(values, "")
			}
		}
		params = append(params, values)
	}
	return nil, sql, params
}

func processBatch(batch []*sarama.ConsumerMessage, conn *sqlx.DB) error {
	err, sql, params := toSQL(batch)
	if err != nil {
		return err
	}
	tx, err := conn.Begin()
	if err != nil {
		return err
	}
	prepare, err := tx.Prepare(sql)
	if err != nil {
		return err
	}
	for _, param := range params {
		_, err = prepare.Exec(param...)
		if err != nil {
			return err
		}
	}
	err = tx.Commit()
	color.Green(fmt.Sprintf("[Sync] Kafka: %s (%d) => CK(%s)", app.Get().Topic, len(params), app.Get().Clickhouse.Host))
	if err != nil {
		return err
	}
	return nil
}

type LogConsumerHandler struct {
	Conn *sqlx.DB
}

func (h *LogConsumerHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *LogConsumerHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *LogConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	batchSize := 1000
	batch := make([]*sarama.ConsumerMessage, 0, batchSize)
	timeout := time.NewTimer(2 * time.Second)

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			batch = append(batch, message)
			// Check if batch size reached
			if len(batch) >= batchSize {
				err := processBatch(batch, h.Conn)
				if err != nil {
					color.Red("[Clickhouse] Fail: %v", err)
				} else {
					for _, item := range batch {
						session.MarkMessage(item, "")
					}
					batch = batch[:0]
					timeout.Reset(2 * time.Second)
				}
			}
		case <-timeout.C:
			// The timer triggers to process the currently collected messages
			if len(batch) > 0 {
				err := processBatch(batch, h.Conn)
				if err != nil {
					color.Red("[Clickhouse] Fail: %v", err)
				} else {
					for _, item := range batch {
						session.MarkMessage(item, "")
					}
					batch = batch[:0]
				}
			}
			timeout.Reset(2 * time.Second)
		}
	}
}
