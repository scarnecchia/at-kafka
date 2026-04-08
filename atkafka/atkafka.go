package atkafka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/gorilla/websocket"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Server struct {
	relayHost        string
	tapHost          string
	tapWorkers       int
	disableAcks      bool
	bootstrapServers []string
	outputTopic      string
	ospreyCompat     bool

	watchedServices []string
	ignoredServices []string

	watchedCollections []string
	ignoredCollections []string

	producer  *Producer
	plcClient *PlcClient
	apiClient *ApiClient
	logger    *slog.Logger
	ws        *websocket.Conn
	ackQueue  chan uint
}

type ServerArgs struct {
	// network params
	RelayHost   string
	TapHost     string
	TapWorkers  int
	DisableAcks bool
	PlcHost     string
	ApiHost     string

	// for watched and ignoed services or collections, only one list may be supplied
	// for both services and collections, wildcards are acceptable. for example:
	// app.bsky.* will watch/ignore any collection that falls under the app.bsky namespace.
	// *.bsky.network will watch/ignore any event that falls under the bsky.network list of PDSes

	// list of services that are events will be emitted for
	WatchedServices []string
	// list of services that events are ignored for
	IgnoredServices []string

	// list of collections that events are emitted for
	WatchedCollections []string
	// list of collections that events are ignored for
	IgnoredCollections []string

	// kafka params
	BootstrapServers []string
	OutputTopic      string

	// osprey-specific params
	OspreyCompat bool

	// other
	Logger *slog.Logger
}

func NewServer(args *ServerArgs) (*Server, error) {
	if args.Logger == nil {
		args.Logger = slog.Default()
	}

	if len(args.WatchedServices) > 0 && len(args.IgnoredServices) > 0 {
		return nil, fmt.Errorf("you may only specify a list of watched services _or_ ignored services, not both")
	}

	if (len(args.WatchedServices) > 0 || len(args.IgnoredServices) > 0) && args.PlcHost == "" {
		return nil, fmt.Errorf("unable to support watched/ignored services without specifying a PLC host")
	}

	if len(args.WatchedCollections) > 0 && len(args.IgnoredCollections) > 0 {
		return nil, fmt.Errorf("you may only specify a list of watched collections _or_ ignored collections, not both")
	}

	var plcClient *PlcClient
	if args.PlcHost != "" {
		plcClient = NewPlcClient(&PlcClientArgs{
			PlcHost: args.PlcHost,
		})
	}

	var apiClient *ApiClient
	if args.ApiHost != "" {
		var err error
		apiClient, err = NewApiClient(&ApiClientArgs{
			ApiHost: args.ApiHost,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create new api client: %w", err)
		}
	}

	s := &Server{
		relayHost:        args.RelayHost,
		tapHost:          args.TapHost,
		tapWorkers:       args.TapWorkers,
		disableAcks:      args.DisableAcks,
		plcClient:        plcClient,
		apiClient:        apiClient,
		bootstrapServers: args.BootstrapServers,
		outputTopic:      args.OutputTopic,
		ospreyCompat:     args.OspreyCompat,
		logger:           args.Logger,
	}

	if len(args.WatchedServices) > 0 {
		watchedServices := make([]string, 0, len(args.WatchedServices))
		for _, service := range args.WatchedServices {
			watchedServices = append(watchedServices, strings.TrimPrefix(strings.TrimPrefix(service, "*."), "."))
		}
		s.watchedServices = watchedServices
	} else if len(args.IgnoredServices) > 0 {
		ignoredServices := make([]string, 0, len(args.IgnoredServices))
		for _, service := range args.IgnoredServices {
			ignoredServices = append(ignoredServices, strings.TrimPrefix(strings.TrimPrefix(service, "*."), "."))
		}
		s.ignoredServices = ignoredServices
	}

	if len(args.WatchedCollections) > 0 {
		watchedCollections := make([]string, 0, len(args.WatchedCollections))
		for _, collection := range args.WatchedCollections {
			watchedCollections = append(watchedCollections, strings.TrimSuffix(strings.TrimSuffix(collection, ".*"), "."))
		}
		s.watchedCollections = watchedCollections
	} else if len(args.IgnoredCollections) > 0 {
		ignoredCollections := make([]string, 0, len(args.IgnoredCollections))
		for _, collection := range args.IgnoredCollections {
			ignoredCollections = append(ignoredCollections, strings.TrimSuffix(strings.TrimSuffix(collection, ".*"), "."))
		}
		s.ignoredCollections = ignoredCollections
	}

	return s, nil
}

func (s *Server) Run(ctx context.Context) error {
	s.logger.Info("starting consumer", "relay-host", s.relayHost, "bootstrap-servers", s.bootstrapServers, "output-topic", s.outputTopic)

	createCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	producerLogger := s.logger.With("component", "producer")
	kafProducer, err := NewProducer(createCtx, producerLogger, s.bootstrapServers, s.outputTopic,
		WithEnsureTopic(true),
		WithTopicPartitions(200),
	)
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer kafProducer.Close()
	s.producer = kafProducer
	s.logger.Info("created producer")

	wsDialer := websocket.DefaultDialer
	u, err := url.Parse(s.relayHost)
	if err != nil {
		return fmt.Errorf("invalid relayHost: %w", err)
	}
	u.Path = "/xrpc/com.atproto.sync.subscribeRepos"
	s.logger.Info("created dialer")

	wsErr := make(chan error, 1)
	shutdownWs := make(chan struct{}, 1)
	go func() {
		logger := s.logger.With("component", "websocket")

		logger.Info("subscribing to repo event stream", "upstream", s.relayHost)

		conn, _, err := wsDialer.Dial(u.String(), http.Header{
			"User-Agent": []string{"at-kafka/0.0.0"},
		})
		if err != nil {
			wsErr <- err
			return
		}

		parallelism := 400
		scheduler := parallel.NewScheduler(parallelism, 1000, s.relayHost, s.handleEvent)
		defer scheduler.Shutdown()

		logger.Info("firehose scheduler configured", "parallelism", parallelism)

		go func() {
			if err := events.HandleRepoStream(ctx, conn, scheduler, logger); err != nil {
				wsErr <- err
				return
			}
		}()

		<-shutdownWs

		wsErr <- nil
	}()
	s.logger.Info("created relay consumer")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-signals:
		s.logger.Info("shutting down on signal", "signal", sig)
	case err := <-wsErr:
		if err != nil {
			s.logger.Error("websocket error", "err", err)
		} else {
			s.logger.Info("websocket shutdown unexpectedly")
		}
	}

	close(shutdownWs)

	return nil
}

func (s *Server) FetchEventMetadata(ctx context.Context, did syntax.DID) (*EventMetadata, *identity.Identity, error) {
	var ident *identity.Identity
	var didDocument identity.DIDDocument
	var pdsHost string
	var handle string
	var didCreatedAt string
	accountAge := int64(-1)
	var profile *bsky.ActorDefs_ProfileViewDetailed

	var wg sync.WaitGroup

	if s.plcClient != nil {
		wg.Go(func() {
			logger := s.logger.With("component", "didDoc")
			var err error
			ident, err = s.plcClient.GetIdentity(ctx, did)
			if err != nil {
				logger.Error("error fetching did doc", "did", did, "err", err)
				return
			}
			didDocument = ident.DIDDocument()
			pdsHost = ident.PDSEndpoint()
			handle = ident.Handle.String()
		})

		// We can only fetch the audit log from did:plc
		if did.Method() == "plc" {
			wg.Go(func() {
				logger := s.logger.With("component", "auditLog")
				auditLog, err := s.plcClient.GetDIDAuditLog(ctx, did)
				if err != nil {
					logger.Error("error fetching did audit log", "did", did, "err", err)
					return
				}

				didCreatedAt = auditLog.CreatedAt

				createdAt, err := time.Parse(time.RFC3339Nano, auditLog.CreatedAt)
				if err != nil {
					logger.Error("error parsing timestamp in audit log", "did", did, "timestamp", auditLog.CreatedAt, "err", err)
					return
				}

				accountAge = int64(time.Since(createdAt).Seconds())
			})
		}
	}

	if s.apiClient != nil {
		wg.Go(func() {
			logger := s.logger.With("component", "profile")
			var err error
			profile, err = s.apiClient.GetProfile(ctx, did.String())
			if err != nil {
				logger.Error("error getting actor profile", "did", did, "err", err)
				return
			}
		})
	}

	wg.Wait()

	return &EventMetadata{
		DidDocument:  didDocument,
		PdsHost:      pdsHost,
		Handle:       handle,
		DidCreatedAt: didCreatedAt,
		AccountAge:   accountAge,
		Profile:      profile,
	}, ident, nil
}

func (s *Server) handleEvent(ctx context.Context, evt *events.XRPCStreamEvent) error {
	dispatchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	logger := s.logger.With("component", "handleEvent")
	logger.Debug("event", "seq", evt.Sequence())

	var collection string
	var actionName string

	var evtKey string
	var evtsToProduce [][]byte

	if evt.RepoCommit != nil {
		// key events by DID
		evtKey = evt.RepoCommit.Repo

		// read the repo
		rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.RepoCommit.Blocks))
		if err != nil {
			logger.Error("failed to read repo from car", "error", err)
			return nil
		}

		did, err := syntax.ParseDID(evt.RepoCommit.Repo)
		if err != nil {
			s.logger.Error("received invalid DID from repo car", "did", evt.RepoCommit.Repo, "err", err)
			return nil
		}

		eventMetadata, ident, err := s.FetchEventMetadata(dispatchCtx, did)
		if err != nil {
			logger.Error("error fetching event metadata", "err", err)
		} else if ident != nil {
			skip := false
			pdsEndpoint := ident.PDSEndpoint()
			u, err := url.Parse(pdsEndpoint)
			if err != nil {
				return fmt.Errorf("failed to parse pds host: %w", err)
			}
			pdsHost := u.Hostname()

			if pdsHost != "" {
				if len(s.watchedServices) > 0 {
					skip = true
					for _, watchedService := range s.watchedServices {
						if watchedService == pdsHost || strings.HasSuffix(pdsHost, "."+watchedService) {
							skip = false
							break
						}
					}
				} else if len(s.ignoredServices) > 0 {
					for _, ignoredService := range s.ignoredServices {
						if ignoredService == pdsHost || strings.HasSuffix(pdsHost, "."+ignoredService) {
							skip = true
							break
						}
					}
				}
			}

			if skip {
				logger.Debug("skipping event based on pds host", "pdsHost", pdsHost)
				return nil
			}
		}

		for _, op := range evt.RepoCommit.Ops {
			kind := repomgr.EventKind(op.Action)
			collection = strings.Split(op.Path, "/")[0]
			rkey := strings.Split(op.Path, "/")[1]
			did := evt.RepoCommit.Repo
			atUri := fmt.Sprintf("at://%s/%s/%s", did, collection, rkey)

			// bust the profile cache whenever it gets updated
			// we won't worry about the counts of i.e. followers, since we have carveouts for some of these already anyway
			if collection == "app.bsky.actor.profile" {
				if s.apiClient != nil {
					s.apiClient.BustProfileCache(did)
				}
			}

			skip := false
			if len(s.watchedCollections) > 0 {
				skip = true
				for _, watchedCollection := range s.watchedCollections {
					if watchedCollection == collection || strings.HasPrefix(collection, watchedCollection+".") {
						skip = false
						break
					}
				}
			} else if len(s.ignoredCollections) > 0 {
				for _, ignoredCollection := range s.ignoredCollections {
					if ignoredCollection == collection || strings.HasPrefix(collection, ignoredCollection+".") {
						skip = true
						break
					}
				}
			}

			if skip {
				logger.Debug("skipping event based on collection", "collection", collection)
				continue
			}

			kindStr := "create"
			switch kind {
			case repomgr.EvtKindUpdateRecord:
				kindStr = "update"
			case repomgr.EvtKindDeleteRecord:
				kindStr = "delete"
			}
			actionName = "operation#" + kindStr

			handledEvents.WithLabelValues(actionName, collection).Inc()

			var rec map[string]any
			var recCid string
			if kind == repomgr.EvtKindCreateRecord || kind == repomgr.EvtKindUpdateRecord {
				rcid, recB, err := rr.GetRecordBytes(ctx, op.Path)
				if err != nil {
					logger.Error("failed to get record bytes", "error", err)
					continue
				}

				// verify the cids match
				recCid = rcid.String()
				if recCid != op.Cid.String() {
					logger.Error("record cid mismatch", "expected", *op.Cid, "actual", rcid)
					continue
				}

				// unmarshal the cbor into a map[string]any
				maybeRec, err := atdata.UnmarshalCBOR(*recB)
				if err != nil {
					logger.Error("failed to unmarshal record", "error", err)
					continue
				}
				rec = maybeRec
			}

			// create the formatted operation
			atkOp := AtKafkaOp{
				Action:     op.Action,
				Collection: collection,
				Rkey:       rkey,
				Uri:        atUri,
				Cid:        recCid,
				Path:       op.Path,
				Record:     rec,
			}

			// create the evt to put on kafka, regardless of if we are using osprey or not
			kafkaEvt := AtKafkaEvent{
				Did:       did,
				Timestamp: evt.RepoCommit.Time,
				Operation: &atkOp,
			}

			if eventMetadata != nil {
				kafkaEvt.Metadata = eventMetadata
			}

			var evtBytes []byte
			if s.ospreyCompat {
				// create the wrapper event for osprey
				ospreyKafkaEvent := OspreyAtKafkaEvent{
					Data: OspreyEventData{
						ActionName: actionName,
						ActionId:   time.Now().UnixNano(), // TODO: this should be a snowflake
						Data:       kafkaEvt,
						Timestamp:  evt.RepoCommit.Time,
						SecretData: map[string]string{},
						Encoding:   "UTF8",
					},
					SendTime: time.Now().Format(time.RFC3339),
				}

				evtBytes, err = json.Marshal(&ospreyKafkaEvent)
			} else {
				evtBytes, err = json.Marshal(&kafkaEvt)
			}
			if err != nil {
				return fmt.Errorf("failed to marshal kafka event: %w", err)
			}

			evtsToProduce = append(evtsToProduce, evtBytes)
		}
	} else {
		defer func() {
			handledEvents.WithLabelValues(actionName, "").Inc()
		}()

		// start with a kafka event and an action name
		var kafkaEvt AtKafkaEvent
		var timestamp string
		var did string

		if evt.RepoAccount != nil {
			actionName = "account"
			timestamp = evt.RepoAccount.Time
			did = evt.RepoAccount.Did

			kafkaEvt = AtKafkaEvent{
				Did:       evt.RepoAccount.Did,
				Timestamp: evt.RepoAccount.Time,
				Account: &AtKafkaAccount{
					Active: evt.RepoAccount.Active,
					Seq:    evt.RepoAccount.Seq,
					Status: evt.RepoAccount.Status,
				},
			}
		} else if evt.RepoIdentity != nil {
			actionName = "identity"
			timestamp = evt.RepoIdentity.Time
			did = evt.RepoIdentity.Did

			var handle string
			if evt.RepoIdentity.Handle != nil {
				handle = *evt.RepoIdentity.Handle
			}

			kafkaEvt = AtKafkaEvent{
				Did:       evt.RepoIdentity.Did,
				Timestamp: evt.RepoIdentity.Time,
				Identity: &AtKafkaIdentity{
					Seq:    evt.RepoIdentity.Seq,
					Handle: handle,
				},
			}
		} else if evt.RepoInfo != nil {
			actionName = "info"
			timestamp = time.Now().Format(time.RFC3339Nano)

			kafkaEvt = AtKafkaEvent{
				Info: &AtKafkaInfo{
					Name:    evt.RepoInfo.Name,
					Message: evt.RepoInfo.Message,
				},
			}
		} else {
			return fmt.Errorf("unhandled event received")
		}

		if did != "" {
			// key events by DID
			evtKey = did
			parsedDid, err := syntax.ParseDID(did)
			if err != nil {
				s.logger.Error("received invalid DID from repo event", "did", did, "err", err)
				return nil
			}

			eventMetadata, ident, err := s.FetchEventMetadata(dispatchCtx, parsedDid)
			if err != nil {
				logger.Error("error fetching event metadata", "err", err)
			} else if ident != nil {
				skip := false
				pdsEndpoint := ident.PDSEndpoint()
				u, err := url.Parse(pdsEndpoint)
				if err != nil {
					return fmt.Errorf("failed to parse pds host: %w", err)
				}
				pdsHost := u.Hostname()

				if pdsHost != "" {
					if len(s.watchedServices) > 0 {
						skip = true
						for _, watchedService := range s.watchedServices {
							if watchedService == pdsHost || strings.HasSuffix(pdsHost, "."+watchedService) {
								skip = false
								break
							}
						}
					} else if len(s.ignoredServices) > 0 {
						for _, ignoredService := range s.ignoredServices {
							if ignoredService == pdsHost || strings.HasSuffix(pdsHost, "."+ignoredService) {
								skip = true
								break
							}
						}
					}
				}

				if skip {
					logger.Debug("skipping event based on pds host", "pdsHost", pdsHost)
					return nil
				}

				kafkaEvt.Metadata = eventMetadata
			}
		} else {
			// key events without a DID by "unknown"
			evtKey = "<unknown>"
		}

		// create the kafka event bytes
		var evtBytes []byte
		var err error

		if s.ospreyCompat {
			// wrap the event in an osprey event
			ospreyKafkaEvent := OspreyAtKafkaEvent{
				Data: OspreyEventData{
					ActionName: actionName,
					ActionId:   time.Now().UnixNano(), // TODO: this should be a snowflake
					Data:       kafkaEvt,
					Timestamp:  timestamp,
					SecretData: map[string]string{},
					Encoding:   "UTF8",
				},
				SendTime: time.Now().Format(time.RFC3339),
			}

			evtBytes, err = json.Marshal(&ospreyKafkaEvent)
		} else {
			evtBytes, err = json.Marshal(&kafkaEvt)
		}
		if err != nil {
			return fmt.Errorf("failed to marshal kafka event: %w", err)
		}

		evtsToProduce = append(evtsToProduce, evtBytes)
	}

	for _, evtBytes := range evtsToProduce {
		if err := s.produceAsync(ctx, evtKey, evtBytes); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) produceAsync(ctx context.Context, key string, msg []byte) error {
	callback := func(r *kgo.Record, err error) {
		status := "ok"
		if err != nil {
			status = "error"
			s.logger.Error("error producing message", "err", err)
		}
		producedEvents.WithLabelValues(status).Inc()
	}

	if err := s.producer.ProduceAsync(ctx, key, msg, callback); err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	return nil
}
