package handlerClient

import (
	"errors"
	"fmt"
	"github.com/jffp113/go-util/messaging"
	"github.com/jffp113/go-util/messaging/routerdealerhandlers/pb"
	zmq "github.com/pebbe/zmq4"
	"io"
)

type context struct {
	handlerId string
	client    *HandlerClient
	worker    *messaging.ZmqConnection
}

type Invoker interface {
	Invoke(request []byte,msgType int32) (content []byte,responseType int32, err error)
}

//GetContext gets a context similar to a factory
//it returns a Invoker to to remote handler invocations
//and a io.Closer to close the resources used afterwards
func (c *HandlerClient) GetContext(handlerId string) (Invoker, io.Closer) {
	worker, err := messaging.NewConnection(c.context, zmq.DEALER, "inproc://workers", false)

	if err != nil {
		panic(err)
	}

	r := context{handlerId, c, worker}
	return &r, &r
}

//Invoke sends a message to a distributed component
//msgType should not use 1000-1999 because they are reserved to this protocol
//returns response bytes, msg type response and and error if it is the case
func (c *context) Invoke(request []byte,msgType int32) ([]byte, int32, error) {
	if msgType >= 1000 && msgType <= 1999 {
		return nil,0,errors.New(fmt.Sprintf("msgtype %v reserved to the protocol",msgType))
	}


	logger.Debugf("Invoking Handler %v",c.handlerId)

	handlerAddress := c.client.handlers[c.handlerId]

	data, _, err := pb.CreateHandlerMessageWithBytesToBytes(pb.HandlerMessage_Type(msgType),
		request, handlerAddress,c.handlerId)

	err = c.worker.SendData("", data)

	if err != nil {
		return nil,0,err
	}

	_, recvData, err := c.worker.RecvData()

	msg,err := pb.UnmarshallHandlerMessage(recvData)

	if err != nil {
		return nil,0,err
	}

	return msg.Content,int32(msg.Type),err
}

//Close closes the Context.
//Consequent calls to Invoke will produce and error.
func (c *context) Close() error {
	c.worker.Close()
	return nil
}