package pb

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	uuid "github.com/satori/go.uuid"
)

// Generate a new UUID
func GenerateId() string {
	return fmt.Sprint(uuid.NewV4())
}

func CreateSignMessage(msgType HandlerMessage_Type, data []byte) ([]byte, string, error) {
	return CreateMessageWithCorrelationId(msgType, data, GenerateId())
}

func CreateMessageWithCorrelationId(msgType HandlerMessage_Type, data []byte, corrId string) ([]byte, string, error) {
	b, err := proto.Marshal(&HandlerMessage{
		Type:          msgType,
		CorrelationId: corrId,
		Content:       data,
	})
	return b, corrId, err
}

func CreateHandlerMessageWithCorrelationId(msgType HandlerMessage_Type, data proto.Message, corrId string, handlerAddr string) (*HandlerMessage, string, error) {
	bytes, err := proto.Marshal(data)

	if err != nil {
		return nil, "", err
	}

	hd := HandlerMessage{
		Type:          msgType,
		CorrelationId: corrId,
		Content:       bytes,
		HandlerAddr:     handlerAddr,
	}
	return &hd, corrId, nil
}

func CreateHandlerMessageWithBytesToBytes(msgType HandlerMessage_Type, data []byte, handlerAddr string,handlerId string) ([]byte, string, error) {
	corrId := GenerateId()
	b, err := proto.Marshal(&HandlerMessage{
		Type:          msgType,
		CorrelationId: corrId,
		Content:       data,
		HandlerAddr:     handlerAddr,
		HandlerId: handlerId,
	})
	return b, corrId, err
}

func CreateHandlerMessage(msgType HandlerMessage_Type, data proto.Message, handlerId string) (*HandlerMessage, string, error) {
	return CreateHandlerMessageWithCorrelationId(msgType, data, GenerateId(), handlerId)
}

func UnmarshallHandlerMessage(data []byte) (*HandlerMessage, error) {
	msg := HandlerMessage{}

	err := proto.Unmarshal(data, &msg)

	return &msg, err
}
