package nrpc_extra

import (
	"log"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/nats-io/go-nats"
	"github.com/rapidloop/nrpc"
)

func CallMany(req proto.Message, n int, newRep func() proto.Message, nc nrpc.NatsConn, subject string, encoding string, timeout time.Duration) ([]proto.Message, error) {
	// encode request
	rawRequest, err := nrpc.Marshal(encoding, req)
	if err != nil {
		log.Printf("nrpc: inner request marshal failed: %v", err)
		return nil, err
	}

	if encoding != "protobuf" {
		subject += "." + encoding
	}

	// call
	inbox := nats.NewInbox()
	ch := make(chan *nats.Msg, n)
	s, err := nc.ChanSubscribe(inbox, ch)
	if err != nil {
		return nil, err
	}
	s.AutoUnsubscribe(n)
	defer s.Unsubscribe()
	err = nc.PublishRequest(subject, inbox, rawRequest)
	if err != nil {
		return nil, err
	}
	rep := []proto.Message{}
	for i := 0; i < n; i++ {
		msg, err := s.NextMsg(timeout)
		if i != 0 && err == nats.ErrTimeout {
			return rep, nil
		}
		if err != nil {
			log.Printf("nrpc: nats request failed: %v", err)
			return rep, err
		}
		data := msg.Data
		pm := newRep()
		if err := nrpc.UnmarshalResponse(encoding, data, pm); err != nil {
			if _, isError := err.(*nrpc.Error); !isError {
				log.Printf("nrpc: response unmarshal failed: %v", err)
			}
			return rep, err
		}
		rep = append(rep, pm)
	}
	// unreachable
	return rep, nil
}
