package broadcast

import (
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v3/plugins/pubsub"
	websocketsProto "go.buf.build/protocolbuffers/go/roadrunner-server/api/websockets/v1"
	"go.uber.org/zap"
)

// rpc collectors struct
type rpc struct {
	plugin *Plugin
	log    *zap.Logger
}

// Publish ... msg is a protobuf decoded payload
// see: root/proto
func (r *rpc) Publish(in *websocketsProto.Request, out *websocketsProto.Response) error {
	const op = errors.Op("broadcast_publish")

	// just return in case of nil message
	if in == nil {
		out.Ok = false
		return nil
	}

	r.log.Debug("message was published", zap.String("msg", in.String()))
	msgLen := len(in.GetMessages())

	for i := 0; i < msgLen; i++ {
		for j := 0; j < len(in.GetMessages()[i].GetTopics()); j++ {
			if in.GetMessages()[i].GetTopics()[j] == "" {
				r.log.Warn("message with empty topic, skipping")
				// skip empty topics
				continue
			}

			tmp := &pubsub.Msg{
				Topic_:   in.GetMessages()[i].GetTopics()[j],
				Payload_: in.GetMessages()[i].GetPayload(),
			}

			err := r.plugin.Publish(tmp)
			if err != nil {
				out.Ok = false
				return errors.E(op, err)
			}
		}
	}

	out.Ok = true
	return nil
}

// PublishAsync ...
// see: root/proto
func (r *rpc) PublishAsync(in *websocketsProto.Request, out *websocketsProto.Response) error {
	// just return in case of nil message
	if in == nil {
		out.Ok = false
		return nil
	}

	r.log.Debug("message was published", zap.Any("msg", in.GetMessages()))

	msgLen := len(in.GetMessages())

	for i := 0; i < msgLen; i++ {
		for j := 0; j < len(in.GetMessages()[i].GetTopics()); j++ {
			if in.GetMessages()[i].GetTopics()[j] == "" {
				r.log.Warn("message with empty topic, skipping")
				// skip empty topics
				continue
			}

			tmp := &pubsub.Msg{
				Topic_:   in.GetMessages()[i].GetTopics()[j],
				Payload_: in.GetMessages()[i].GetPayload(),
			}

			r.plugin.PublishAsync(tmp)
		}
	}

	out.Ok = true
	return nil
}
