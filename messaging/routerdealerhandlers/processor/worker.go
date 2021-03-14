package processor

import (
	"github.com/jffp113/go-util/messaging"
	"github.com/jffp113/go-util/messaging/routerdealerhandlers/pb"
	zmq "github.com/pebbe/zmq4"
)

func worker(workerPoolURL string, context *zmq.Context, workerChan <-chan *pb.HandlerMessage, handlers []Handler) {
	connection, err := messaging.NewConnection(context, zmq.DEALER, workerPoolURL, false)
	defer connection.Close()

	if err != nil {
		logger.Error("Worker connection is nill")
		panic(err)
		return
	}

	for msg := range workerChan {

		handler := findHandler(handlers, msg.HandlerId) //todo error

		resp,respType := handler.Handle(msg.Content,int32(msg.Type))

		b,_,_ := pb.CreateMessageWithCorrelationId(pb.HandlerMessage_Type(respType), //todo handle the error
			resp, msg.CorrelationId)

		err = connection.SendData("", b)
	}

	if err != nil {
		logger.Error("Error sending message")
	}
}

func findHandler(handlers []Handler, scheme string) Handler {

	for _, handler := range handlers {
		if handler.Name() == scheme {
			return handler
		}
	}

	return nil
}
