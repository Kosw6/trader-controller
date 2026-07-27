package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kosw006/trader-controller/internal/domain"
	kafka "github.com/segmentio/kafka-go"
)

// PublishResult는 Kafka가 메시지를 정상 수락했을 때 확정되는 위치 정보다.
// 이 값을 pipeline_outbox에 저장하면 장애 분석 때 DB 이벤트와 Kafka 로그를 연결할 수 있다.
type PublishResult struct {
	Partition int
	Offset    int64
}

// Publisher는 outbox relay가 의존하는 Kafka 발행 인터페이스다.
// 인터페이스로 두면 나중에 테스트용 fake publisher나 다른 브로커 구현으로 바꾸기 쉽다.
type Publisher interface {
	Health(ctx context.Context) error
	Publish(ctx context.Context, event domain.OutboxEvent) (PublishResult, error)
	Close() error
}

// KafkaPublisher는 segmentio/kafka-go Writer를 감싼 실제 Kafka 발행 구현체다.
type KafkaPublisher struct {
	brokers       []string
	topic         string
	healthTimeout time.Duration
	writer        *kafka.Writer
}

// NewKafkaPublisher는 Kafka 토픽으로 메시지를 보내는 publisher를 생성한다.
// Completion 콜백을 사용해 WriteMessages 성공 시 partition/offset 값을 얻는다.
func NewKafkaPublisher(brokers []string, topic string, publishTimeout, healthTimeout time.Duration, allowAutoTopic bool) *KafkaPublisher {
	return &KafkaPublisher{
		brokers:       brokers,
		topic:         topic,
		healthTimeout: healthTimeout,
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Balancer:               &kafka.Hash{},
			RequiredAcks:           kafka.RequireAll,
			Async:                  false,
			BatchSize:              1,
			BatchTimeout:           10 * time.Millisecond,
			WriteTimeout:           publishTimeout,
			ReadTimeout:            publishTimeout,
			AllowAutoTopicCreation: allowAutoTopic,
		},
	}
}

// Health는 Kafka broker에 접속해 topic metadata를 읽을 수 있는지 확인한다.
// 이 단계가 실패하면 relay는 outbox row를 PUBLISHING으로 선점하지 않고 대기한다.
func (p *KafkaPublisher) Health(ctx context.Context) error {
	if len(p.brokers) == 0 {
		return errors.New("kafka brokers are empty")
	}
	healthCtx, cancel := context.WithTimeout(ctx, p.healthTimeout)
	defer cancel()

	conn, err := kafka.DialContext(healthCtx, "tcp", p.brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka broker: %w", err)
	}
	defer conn.Close()

	if deadline, ok := healthCtx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	partitions, err := conn.ReadPartitions(p.topic)
	if err != nil {
		return fmt.Errorf("read kafka topic metadata: %w", err)
	}
	if len(partitions) == 0 {
		return fmt.Errorf("kafka topic %s has no partitions", p.topic)
	}
	return nil
}

// Publish는 outbox event 하나를 Kafka로 보내고 Kafka가 확정한 partition/offset을 반환한다.
// WriteMessages는 동기 모드로 사용하므로 함수가 nil error를 반환하면 Kafka ack까지 끝난 상태다.
func (p *KafkaPublisher) Publish(ctx context.Context, event domain.OutboxEvent) (PublishResult, error) {
	if event.Topic == "" {
		return PublishResult{}, errors.New("outbox event topic is empty")
	}

	var result PublishResult
	var completionErr error
	p.writer.Completion = func(messages []kafka.Message, err error) {
		if err != nil {
			completionErr = err
			return
		}
		if len(messages) == 0 {
			completionErr = errors.New("kafka completion returned no messages")
			return
		}
		result = PublishResult{
			Partition: messages[0].Partition,
			Offset:    messages[0].Offset,
		}
	}

	message := kafka.Message{
		Topic: event.Topic,
		Key:   []byte(event.MessageKey),
		Value: []byte(event.Payload),
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(event.EventType)},
			{Key: "idempotency_key", Value: []byte(event.IdempotencyKey)},
			{Key: "outbox_id", Value: []byte(fmt.Sprintf("%d", event.ID))},
		},
	}
	if err := p.writer.WriteMessages(ctx, message); err != nil {
		return PublishResult{}, err
	}
	if completionErr != nil {
		return PublishResult{}, completionErr
	}
	return result, nil
}

// Close는 Kafka writer 내부 연결과 goroutine을 정리한다.
func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}
