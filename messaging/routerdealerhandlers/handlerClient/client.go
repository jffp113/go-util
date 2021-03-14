package handlerClient

import (
	"github.com/golang/protobuf/proto"
	"github.com/ipfs/go-log"
	"github.com/jffp113/go-util/messaging"
	"github.com/jffp113/go-util/messaging/routerdealerhandlers/pb"
	zmq "github.com/pebbe/zmq4"
)

var logger = log.Logger("handler_client")

const RegisterChanSize = 5

type handlerClient struct {
	requests            map[string]string
	handlers            map[string]string
	registerHandlerChan chan *pb.HandlerMessage
	conn                *messaging.ZmqConnection
	clients             *messaging.ZmqConnection
	context             *zmq.Context
}

func NewHandlerFactory(uri string) (*handlerClient, error) {
	context, _ := zmq.NewContext()

	conn, err := messaging.NewConnection(context, zmq.ROUTER, uri, true)

	if err != nil {
		return nil, err
	}
	clients, err := messaging.NewConnection(context, zmq.ROUTER, "inproc://workers", true)
	if err != nil {
		return nil, err
	}

	c := handlerClient{
		requests:            make(map[string]string),
		handlers:            make(map[string]string),
		registerHandlerChan: make(chan *pb.HandlerMessage, RegisterChanSize),
		conn:                conn,
		clients:             clients,
		context:             context,
	}

	go c.receive()
	go c.processNewHandlers()

	return &c, nil
}

func (c *handlerClient) Close() error {
	c.conn.Close()
	return nil
}

func (c *handlerClient) receive() {
	poller := zmq.NewPoller()

	poller.Add(c.conn.Socket(), zmq.POLLIN)
	poller.Add(c.clients.Socket(), zmq.POLLIN)

	for {
		polled, err := poller.Poll(-1)
		if err != nil {
			logger.Error("Error Polling messages from socket")
			return
		}
		for _, ready := range polled {
			switch socket := ready.Socket; socket {
			case c.conn.Socket():
				c.handleConnSocket()
			case c.clients.Socket():
				c.handleClientSocket()
			}
		}
	}

}

func (c *handlerClient) processNewHandlers() {
	worker, err := messaging.NewConnection(c.context, zmq.DEALER, "inproc://workers", false)

	if err != nil {
		panic(err)
	}

	for newMsg := range c.registerHandlerChan {
		req := pb.HandlerRegisterRequest{}

		err := proto.Unmarshal(newMsg.Content, &req)

		if err != nil {
			logger.Warnf("Error Ignoring register handler MSG: %v", err)
			continue
		}
		logger.Debugf("Registering %v from %v", req.HandlerId, newMsg.HandlerAddr)

		c.handlers[req.HandlerId] = newMsg.HandlerAddr

		rep := pb.HandlerRegisterResponse{Status: pb.HandlerRegisterResponse_OK}

		handlerMsg, _, err := pb.CreateHandlerMessageWithCorrelationId(pb.HandlerMessage_HANDLER_REGISTER_RESPONSE,
			&rep, newMsg.CorrelationId, newMsg.HandlerAddr)

		if err != nil {
			logger.Warnf("Error Ignoring register handler MSG: %v", err)
			continue
		}

		data, err := proto.Marshal(handlerMsg)

		worker.SendData("", data)

	}
}

func (c *handlerClient) handleConnSocket() {
	handlerAddr, data, err := c.conn.RecvData()

	if err != nil {
		logger.Warnf("Error Ignoring MSG: %v", err)
		return
	}

	msg := pb.HandlerMessage{}

	err = proto.Unmarshal(data, &msg)

	if err != nil {
		logger.Warnf("Error Ignoring MSG: %v", err)
		return
	}

	if msg.Type == pb.HandlerMessage_HANDLER_REGISTER_REQUEST {
		logger.Debug("Register Handler MSG received")
		msg.HandlerAddr = handlerAddr
		c.registerHandlerChan <- &msg
	} else {
		logger.Debug("Received response from a handler")
		v, present := c.requests[msg.CorrelationId]
		if !present {
			logger.Warnf("MSG not expected, ignoring MSG")
			return
		}

		err := c.clients.SendData(v, data)
		if err != nil {
			logger.Error("Error Sending the message")
			return
		}

		delete(c.requests, msg.CorrelationId)
	}
}

func (c *handlerClient) handleClientSocket() {
	logger.Debug("Received data")

	clientId, data, err := c.clients.RecvData()

	handlerMsg := pb.HandlerMessage{}
	err = proto.Unmarshal(data, &handlerMsg)

	if err != nil {
		logger.Error("Error sending out: %v", err)
		return
	}

	c.requests[handlerMsg.CorrelationId] = clientId

	err = c.conn.SendData(handlerMsg.HandlerAddr, data)
	logger.Debug("Sent msg out to signer")

	if err != nil {
		logger.Warnf("Error retransmitting message", err)
	}

}
