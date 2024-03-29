package lumiAuditReporter

import (
	"context"
	"crypto/tls"
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
)

type Reporter interface {
	Report(actorId, actorType, resourceId, resourceType, action string)
	Close()
}

type reporter struct {
	MSK_BROKERS []string
	ERROR_TOPIC string
	AuditWriter *kafka.Writer
	Source      string
}

type AuditMessage struct {
	Source       string `json:"source"`
	ActorId      string `json:"actorId"`
	ResourceId   string `json:"resourceId"`
	ActorType    string `json:"actorType"`
	ResourceType string `json:"resourceType"`
	Action       string `json:"action"`
}

// Sends error messages to ohDear topic
func (r reporter) Report(actorId, actorType, resourceId, resourceType, action string) {
	am := AuditMessage{
		Source:       r.Source,
		ActorId:      actorId,
		ResourceId:   resourceId,
		ActorType:    actorType,
		ResourceType: resourceType,
		Action:       action,
	}

	messageJson, _ := json.Marshal(am)

	r.AuditWriter.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(uuid.New().String()),
			Value: messageJson,
		})
}

func (r reporter) Close() {
	r.AuditWriter.Close()
}

func CreateLumiAuditor(source string, brokers []string, auditTopic string, isLocal bool) (reporterToReturn Reporter, err error) {

	err = validateReporterRequest(source, brokers, auditTopic)
	if err != nil {
		return nil, err
	}

	aw := getKafkaWriter(brokers, auditTopic, isLocal)

	reporterToReturn = reporter{
		MSK_BROKERS: brokers,
		ERROR_TOPIC: auditTopic,
		AuditWriter: aw,
		Source:      source,
	}
	return
}

func getKafkaWriter(brokers []string, topic string, isLocal bool) *kafka.Writer {

	if isLocal {
		return &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		}
	}

	return &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		Transport: &kafka.Transport{
			TLS: &tls.Config{},
		},
	}
}

func validateReporterRequest(source string, brokers []string, auditTopic string) error {
	if source == "" {
		return errors.New("no error source provided")
	}

	if len(brokers) == 0 {
		return errors.New("MSK broker list empty")
	}

	if auditTopic == "" {
		return errors.New("error topic name not provided")
	}

	return nil
}
