package kafka

// services consuming from Kafka should meet this contract
type Handler interface {
	Handle(*Message) error
}

var NoOpHandler = &noOpHandler{}

// no-op message handler impl
type noOpHandler struct{}

func (h *noOpHandler) Handle(kmsg *Message) error {
	return nil
}
