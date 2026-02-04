package consumer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/zeromicro/go-queue/natsmq/common"
	"github.com/zeromicro/go-queue/natsmq/internal"
	"github.com/zeromicro/go-zero/core/contextx"
	"github.com/zeromicro/go-zero/core/queue"
	"go.opentelemetry.io/otel"
)

// ConsumerManager manages consumer operations including NATS connection, JetStream stream initialization,
// and consumer queue creation and subscription.
type ConsumerManager struct {
	// NATS connection configuration and instance.
	nc       *nats.Conn
	natsConf *common.NatsConfig

	// List of global JetStream configurations parsed from a config file.
	JetStreamConfigs []*common.JetStreamConfig

	// List of consumer queue configurations.
	queues []*ConsumerQueueConfig

	// Channel to signal stop of the consumer manager.
	stopCh chan struct{}

	// List of push-based subscription contexts.
	subscribers []jetstream.ConsumeContext

	// List of cancel functions for managing pull-based consumption contexts.
	cancelFuncs []context.CancelFunc
}

// NewConsumerManager creates a new ConsumerManager instance.
// Parameters:
//
//	natsConf         - pointer to NatsConfig defining the NATS connection settings
//	jetStreamConfigs - list of JetStreamConfig, used to initialize global streams
//	cq               - list of ConsumerQueueConfig defining individual consumer queue settings
//
// Returns:
//
//	queue.MessageQueue - the created consumer manager (implements MessageQueue)
//	error              - error if no consumer queues provided or if connection fails
func NewConsumerManager(natsConf *common.NatsConfig, jetStreamConfigs []*common.JetStreamConfig, cq []*ConsumerQueueConfig) (queue.MessageQueue, error) {
	if len(cq) == 0 {
		return nil, errors.New("no consumer queues provided")
	}
	cm := &ConsumerManager{
		natsConf:         natsConf,
		JetStreamConfigs: jetStreamConfigs,
		queues:           cq,
		stopCh:           make(chan struct{}),
		subscribers:      []jetstream.ConsumeContext{},
		cancelFuncs:      []context.CancelFunc{},
	}
	if err := cm.connectToNATS(); err != nil {
		return nil, err
	}
	return cm, nil
}

// connectToNATS establishes a connection to the NATS server.
// Returns:
//
//	error - non-nil error if connection fails
func (cm *ConsumerManager) connectToNATS() error {
	nc, err := nats.Connect(cm.natsConf.URL, cm.natsConf.Options...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}
	cm.nc = nc
	return nil
}

// Start initializes stream instances and creates consumers according to the provided consumer queue configurations.
// This function blocks until Stop() is invoked.
func (cm *ConsumerManager) Start() {
	// Initialize stream instances (register or update streams) based on the provided JetStream configurations.
	common.RegisterStreamInstances(cm.nc, cm.JetStreamConfigs)

	// Iterate over each consumer queue configuration to create the consumer on the corresponding stream.
	for _, cfg := range cm.queues {
		var stream jetstream.Stream
		if cfg.StreamName != "" {
			s, ok := common.GetStream(cfg.StreamName)
			if !ok {
				log.Printf("stream %s not found, skipping consumer: %s", cfg.StreamName, cfg.ConsumerConfig.Name)
				continue
			} else {
				stream = s
			}
		} else {
			s, ok := common.GetStream(common.DefaultStream)
			if !ok {
				log.Printf("default stream not found, skipping consumer: %s", cfg.ConsumerConfig.Name)
				continue
			}
			stream = s
		}

		ctx := context.Background()
		if err := cm.createConsumer(ctx, cfg, stream); err != nil {
			log.Printf("failed to create consumer %s: %v", cfg.ConsumerConfig.Name, err)
			continue
		}
	}
	<-cm.stopCh
}

// createConsumer creates a consumer for a given queue configuration and attaches it to the provided JetStream stream.
// Parameters:
//
//	ctx    - context to manage cancellation and timeout during consumer creation
//	cfg    - pointer to ConsumerQueueConfig containing consumer settings and delivery options
//	stream - JetStream stream instance to be used
//
// Returns:
//
//	error - non-nil error if creating the consumer or subscribing fails
func (cm *ConsumerManager) createConsumer(ctx context.Context, cfg *ConsumerQueueConfig, stream jetstream.Stream) error {
	var consumer jetstream.Consumer
	var err error

	if err := validateConsumerConfig(cfg); err != nil {
		return err
	}

	// Create an ordered consumer or a standard consumer based on the configuration.
	if cfg.ConsumerConfig.Ordered {
		opts := jetstream.OrderedConsumerConfig{
			FilterSubjects:    cfg.ConsumerConfig.FilterSubjects,
			DeliverPolicy:     jetstream.DeliverPolicy(cfg.ConsumerConfig.OrderedConsumerOptions.DeliverPolicy),
			OptStartSeq:       cfg.ConsumerConfig.OrderedConsumerOptions.OptStartSeq,
			OptStartTime:      cfg.ConsumerConfig.OrderedConsumerOptions.OptStartTime,
			ReplayPolicy:      jetstream.ReplayPolicy(cfg.ConsumerConfig.OrderedConsumerOptions.ReplayPolicy),
			InactiveThreshold: cfg.ConsumerConfig.OrderedConsumerOptions.InactiveThreshold,
			HeadersOnly:       cfg.ConsumerConfig.OrderedConsumerOptions.HeadersOnly,
			MaxResetAttempts:  cfg.ConsumerConfig.OrderedConsumerOptions.MaxResetAttempts,
			Metadata:          cfg.ConsumerConfig.OrderedConsumerOptions.Metadata,
			NamePrefix:        cfg.ConsumerConfig.OrderedConsumerOptions.NamePrefix,
		}
		consumer, err = stream.OrderedConsumer(ctx, opts)
		if err != nil {
			return fmt.Errorf("failed to create ordered consumer: %w", err)
		}
	} else {
		consumer, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			Name:               cfg.ConsumerConfig.Name,
			Durable:            cfg.ConsumerConfig.Durable,
			Description:        cfg.ConsumerConfig.Description,
			DeliverPolicy:      jetstream.DeliverPolicy(cfg.ConsumerConfig.DeliverPolicy),
			OptStartSeq:        cfg.ConsumerConfig.OptStartSeq,
			OptStartTime:       cfg.ConsumerConfig.OptStartTime,
			AckPolicy:          jetstream.AckPolicy(cfg.ConsumerConfig.AckPolicy),
			AckWait:            cfg.ConsumerConfig.AckWait,
			MaxDeliver:         cfg.ConsumerConfig.MaxDeliver,
			BackOff:            cfg.ConsumerConfig.BackOff,
			FilterSubject:      cfg.ConsumerConfig.FilterSubject,
			ReplayPolicy:       jetstream.ReplayPolicy(cfg.ConsumerConfig.ReplayPolicy),
			RateLimit:          cfg.ConsumerConfig.RateLimit,
			SampleFrequency:    cfg.ConsumerConfig.SampleFrequency,
			MaxWaiting:         cfg.ConsumerConfig.MaxWaiting,
			MaxAckPending:      cfg.ConsumerConfig.MaxAckPending,
			HeadersOnly:        cfg.ConsumerConfig.HeadersOnly,
			MaxRequestBatch:    cfg.ConsumerConfig.MaxRequestBatch,
			MaxRequestExpires:  cfg.ConsumerConfig.MaxRequestExpires,
			MaxRequestMaxBytes: cfg.ConsumerConfig.MaxRequestMaxBytes,
			InactiveThreshold:  cfg.ConsumerConfig.InactiveThreshold,
			Replicas:           cfg.ConsumerConfig.Replicas,
			MemoryStorage:      cfg.ConsumerConfig.MemoryStorage,
			FilterSubjects:     cfg.ConsumerConfig.FilterSubjects,
			Metadata:           cfg.ConsumerConfig.Metadata,
			PauseUntil:         cfg.ConsumerConfig.PauseUntil,
			PriorityPolicy:     jetstream.PriorityPolicy(cfg.ConsumerConfig.PriorityPolicy),
			PinnedTTL:          cfg.ConsumerConfig.PinnedTTL,
			PriorityGroups:     cfg.ConsumerConfig.PriorityGroups,
			DeliverSubject:     cfg.ConsumerConfig.DeliverSubject,
			DeliverGroup:       cfg.ConsumerConfig.DeliverGroup,
			FlowControl:        cfg.ConsumerConfig.FlowControl,
			IdleHeartbeat:      cfg.ConsumerConfig.IdleHeartbeat,
		})
		if err != nil {
			return fmt.Errorf("failed to create standard consumer: %w", err)
		}
	}

	// Create consumer instances based on the specified number for the consumer queue. Each instance
	// uses either push-based (subscription) or pull-based consumption.
	for i := 0; i < cfg.QueueConsumerCount; i++ {
		log.Printf("Consumer [%s] instance [%d] with filterSubjects %v created successfully", cfg.ConsumerConfig.Name, i, cfg.ConsumerConfig.FilterSubjects)
		switch cfg.Delivery.ConsumptionMethod {
		case Consumer:
			consumerCtx, err := cm.consumerSubscription(ctx, stream, consumer, cfg)
			if err != nil {
				return fmt.Errorf("failed to subscribe to push messages: %w", err)
			}
			cm.subscribers = append(cm.subscribers, consumerCtx)
		case Pull, PullNoWait:
			pullFn := func(num int) (jetstream.MessageBatch, error) {
				if cfg.Delivery.ConsumptionMethod == Pull {
					return consumer.Fetch(num)
				}
				return consumer.FetchNoWait(num)
			}
			pullCtx, cancel := context.WithCancel(ctx)
			cm.cancelFuncs = append(cm.cancelFuncs, cancel)
			go cm.runPullMessages(pullCtx, cfg, pullFn)
		default:
			return fmt.Errorf("unsupported consumption method: %v", cfg.Delivery.ConsumptionMethod)
		}
	}
	return nil
}

// consumerSubscription sets up a push-based subscription using the provided JetStream consumer.
// Parameters:
//
//	consumer - JetStream consumer instance to be used for subscription
//	cfg      - pointer to ConsumerQueueConfig containing consumer settings and the message handler
//
// Returns:
//
//	jetstream.ConsumeContext - context to manage the subscription lifecycle
//	error                  - non-nil error if subscription fails
func (cm *ConsumerManager) consumerSubscription(ctx context.Context, stream jetstream.Stream, consumer jetstream.Consumer, cfg *ConsumerQueueConfig) (jetstream.ConsumeContext, error) {
	managed := &managedConsume{
		stopCh: make(chan struct{}),
	}
	errCh := make(chan error, 1)

	go func() {
		backoff := time.Second
		maxBackoff := 30 * time.Second
		currentConsumer := consumer
		for {
			select {
			case <-managed.stopCh:
				return
			default:
			}

			consumerCtx, err := currentConsumer.Consume(func(msg jetstream.Msg) {
				cm.ackMessage(cfg, msg)
			}, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
				select {
				case errCh <- err:
				default:
				}
			}))
			if err != nil {
				log.Printf("failed to subscribe to messages: %v", err)
				select {
				case <-managed.stopCh:
					return
				case <-time.After(backoff):
				}
				if backoff < maxBackoff {
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
				}
				continue
			}

			managed.setCurrent(consumerCtx)
			backoff = time.Second

			select {
			case <-managed.stopCh:
				managed.stopCurrent()
				return
			case err := <-errCh:
				log.Printf("consume error, restarting consumer %s: %v", cfg.ConsumerConfig.Name, err)
				managed.stopCurrent()
				if isRecoverableConsumerErr(err) {
					rebuildCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					newConsumer, rebuildErr := cm.ensureConsumer(rebuildCtx, cfg, stream)
					cancel()
					if rebuildErr != nil {
						log.Printf("rebuild consumer failed %s: %v", cfg.ConsumerConfig.Name, rebuildErr)
					} else {
						currentConsumer = newConsumer
					}
				}
				select {
				case <-managed.stopCh:
					return
				case <-time.After(backoff):
				}
				if backoff < maxBackoff {
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
				}
			}
		}
	}()

	return managed, nil
}

func (cm *ConsumerManager) ensureConsumer(ctx context.Context, cfg *ConsumerQueueConfig, stream jetstream.Stream) (jetstream.Consumer, error) {
	// Create an ordered consumer or a standard consumer based on the configuration.
	if cfg.ConsumerConfig.Ordered {
		opts := jetstream.OrderedConsumerConfig{
			FilterSubjects: cfg.ConsumerConfig.FilterSubjects,
			DeliverPolicy:  jetstream.DeliverPolicy(cfg.ConsumerConfig.OrderedConsumerOptions.DeliverPolicy),
			OptStartSeq:    cfg.ConsumerConfig.OrderedConsumerOptions.OptStartSeq,
			OptStartTime:   cfg.ConsumerConfig.OrderedConsumerOptions.OptStartTime,
			ReplayPolicy:   jetstream.ReplayPolicy(cfg.ConsumerConfig.OrderedConsumerOptions.ReplayPolicy),
		}
		consumer, err := stream.OrderedConsumer(ctx, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create ordered consumer: %w", err)
		}
		return consumer, nil
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:           cfg.ConsumerConfig.Name,
		Durable:        cfg.ConsumerConfig.Durable,
		Description:    cfg.ConsumerConfig.Description,
		FilterSubjects: cfg.ConsumerConfig.FilterSubjects,
		AckPolicy:      jetstream.AckPolicy(cfg.ConsumerConfig.AckPolicy),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create standard consumer: %w", err)
	}
	return consumer, nil
}

func isRecoverableConsumerErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, nats.ErrNoResponders) {
		return true
	}
	if errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrConsumerNotFound) {
		return true
	}
	return false
}

type managedConsume struct {
	mu      sync.Mutex
	current jetstream.ConsumeContext
	stopCh  chan struct{}
}

func (m *managedConsume) Stop() {
	m.close()
	m.stopCurrent()
}

func (m *managedConsume) Drain() {
	m.close()
	m.mu.Lock()
	if m.current != nil {
		m.current.Drain()
	}
	m.mu.Unlock()
}

func (m *managedConsume) Closed() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopCh
}

func (m *managedConsume) close() {
	m.mu.Lock()
	if m.stopCh != nil {
		select {
		case <-m.stopCh:
		default:
			close(m.stopCh)
		}
	}
	m.mu.Unlock()
}

func (m *managedConsume) setCurrent(ctx jetstream.ConsumeContext) {
	m.mu.Lock()
	m.current = ctx
	m.mu.Unlock()
}

func (m *managedConsume) stopCurrent() {
	m.mu.Lock()
	if m.current != nil {
		m.current.Stop()
	}
	m.mu.Unlock()
}

func validateConsumerConfig(cfg *ConsumerQueueConfig) error {
	if cfg.ConsumerConfig.FilterSubject != "" && len(cfg.ConsumerConfig.FilterSubjects) > 0 {
		return errors.New("filterSubject and filterSubjects are mutually exclusive")
	}
	if cfg.Delivery.ConsumptionMethod == Pull || cfg.Delivery.ConsumptionMethod == PullNoWait {
		if cfg.ConsumerConfig.DeliverSubject != "" || cfg.ConsumerConfig.DeliverGroup != "" || cfg.ConsumerConfig.FlowControl || cfg.ConsumerConfig.IdleHeartbeat > 0 {
			return errors.New("push consumer fields are not allowed for pull consumption")
		}
	}
	if cfg.ConsumerConfig.MaxDeliver > 0 && len(cfg.ConsumerConfig.BackOff) > cfg.ConsumerConfig.MaxDeliver {
		return errors.New("backOff length must be less than or equal to maxDeliver")
	}
	return nil
}

// ackMessage processes a message using the user-provided handler and acknowledges the message if required.
// Parameters:
//
//	cfg - pointer to ConsumerQueueConfig containing the message handler and acknowledgement settings
//	msg - the JetStream message to process
func (cm *ConsumerManager) ackMessage(cfg *ConsumerQueueConfig, msg jetstream.Msg) {
	headers := msg.Headers()
	carrier := internal.NewHeaderCarrier(&headers)
	// extract trace context from message
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)
	// remove deadline and error control
	ctx = contextx.ValueOnlyFrom(ctx)

	if err := cfg.Handler.Consume(ctx, msg); err != nil {
		log.Printf("message processing error: %v", err)
		return
	}

	// Acknowledge the message unless using AckNonePolicy or in an ordered consumer scenario.
	if jetstream.AckPolicy(cfg.ConsumerConfig.AckPolicy) != jetstream.AckNonePolicy && !cfg.ConsumerConfig.Ordered {
		if err := msg.Ack(); err != nil {
			log.Printf("failed to acknowledge message: %v", err)
		}
	}
}

// runPullMessages continuously pulls messages in batches using the provided fetch function.
// Parameters:
//
//	ctx     - context to control the pull loop (supports cancellation)
//	cfg     - pointer to ConsumerQueueConfig with pull configuration, including the fetch count
//	fetchFn - function that fetches a batch of messages; takes an integer defining the number of messages
func (cm *ConsumerManager) runPullMessages(ctx context.Context, cfg *ConsumerQueueConfig, fetchFn func(num int) (jetstream.MessageBatch, error)) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msgs, err := fetchFn(cfg.Delivery.FetchCount)
		if err != nil {
			log.Printf("error fetching messages: %v", err)
			continue
		}
		for msg := range msgs.Messages() {
			cm.ackMessage(cfg, msg)
		}
		if fetchErr := msgs.Error(); fetchErr != nil {
			log.Printf("error after fetching messages: %v", fetchErr)
		}
	}
}

// Stop terminates all active subscriptions and pull routines,
// closes the underlying NATS connection, and signals exit via stopCh.
func (cm *ConsumerManager) Stop() {
	for _, consumerCtx := range cm.subscribers {
		consumerCtx.Stop()
	}
	for _, cancel := range cm.cancelFuncs {
		cancel()
	}
	if cm.nc != nil {
		cm.nc.Close()
	}
	close(cm.stopCh)
}
