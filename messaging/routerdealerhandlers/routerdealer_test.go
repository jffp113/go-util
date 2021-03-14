package messaging

import (
	"github.com/jffp113/go-util/messaging/routerdealerhandlers/handlerClient"
	"github.com/jffp113/go-util/messaging/routerdealerhandlers/processor"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)


type stubHandler1 struct {

}

func (s stubHandler1) Handle(msg []byte, msgType int32) (response []byte, responseType int32) {
	if msgType == 1 {
		return []byte("world"), 2
	}
	return []byte("Not World"),2
}

func (s stubHandler1) Name() string {
	return "world"
}

type stubHandler2 struct {

}

func (s stubHandler2) Handle(msg []byte, msgType int32) (response []byte, responseType int32) {
	if msgType == 1 {
		return []byte("world2"), 3
	}
	return []byte("Not World2"),3
}

func (s stubHandler2) Name() string {
	return "world2"
}

func TestRouterDealerCommunication(t *testing.T){

	handlerClient,err := handlerClient.NewHandlerFactory("tcp://127.0.0.1:9000")

	assert.Nil(t,err)


	go func() {
		server := processor.NewHandlerProcessor("tcp://127.0.0.1:9000")

		server.AddHandler(stubHandler1{})
		server.AddHandler(stubHandler2{})

		server.Start()
	}()

	time.Sleep(1*time.Second)

	w,c := handlerClient.GetContext("world")
	defer c.Close()

	content,responseType,err := w.Invoke([]byte("hello"),1)

	assert.Nil(t,err)

	assert.Equal(t,[]byte("world"),content)
	assert.Equal(t,int32(2),responseType)


	w,c = handlerClient.GetContext("world2")
	defer c.Close()
	content,responseType,err = w.Invoke([]byte("hello"),1)

	assert.Equal(t,[]byte("world2"),content)
	assert.Equal(t,int32(3),responseType)
}