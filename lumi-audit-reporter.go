package lumiAuditReporter

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
)

type Reporter interface {
	Report(actorId, actorType, resourceId, resourceType, action string)
	Close()
}

type reporter struct {
	MSK_BROKERS    []string
	ERROR_TOPIC    string
	AuditWriter    *kafka.Writer
	Source         string
	unsentMessages map[uuid.UUID]AuditMessage
	mut            *sync.Mutex
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

	id := uuid.New()
	r.mut.Lock()
	r.unsentMessages[id] = am
	r.mut.Unlock()

	go r.writeMessage(id, messageJson)

}

func (r reporter) writeMessage(id uuid.UUID, messageJson []byte) {
	r.AuditWriter.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(uuid.New().String()),
			Value: messageJson,
		})

	// remove message from unsentMessages
	r.mut.Lock()
	delete(r.unsentMessages, id)
	r.mut.Unlock()

}

func (r reporter) Close() {
	// if any unsent messages, wait for them to be sent
	for len(r.unsentMessages) > 0 {
		time.Sleep(1 * time.Second)
	}
	r.AuditWriter.Close()
}

func CreateLumiAuditor(source string, brokers []string, auditTopic string, isLocal bool) (reporterToReturn Reporter, err error) {

	err = validateReporterRequest(source, brokers, auditTopic)
	if err != nil {
		return nil, err
	}

	aw := getKafkaWriter(brokers, auditTopic, isLocal)

	reporterToReturn = reporter{
		MSK_BROKERS:    brokers,
		ERROR_TOPIC:    auditTopic,
		AuditWriter:    aw,
		Source:         source,
		unsentMessages: make(map[uuid.UUID]AuditMessage),
		mut:            &sync.Mutex{},
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
