/**
 * Copyriclient.go
context.goght 2017 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ------------------------------------------------------------------------------
 */

// Package messaging handles lower-level communication between a transaction
// processor and validator.
package messaging

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	uuid "github.com/satori/go.uuid"
)

// Generate a new UUID
func GenerateId() string {
	return fmt.Sprint(uuid.NewV4())
}

type Connection interface {
	SendData(id string, data []byte) error
	RecvData() (string, []byte, error)
	Close()
	Socket() *zmq.Socket
	Monitor(zmq.Event) (*zmq.Socket, error)
	Identity() string
}

// Connection wraps a ZMQ DEALER socket or ROUTER socket and provides some
// utility methods for sending and receiving messages.
type ZmqConnection struct {
	identity string
	uri      string
	socket   *zmq.Socket
	context  *zmq.Context
}

// NewConnection establishes a new connection using the given ZMQ context and
// socket type to the given URI.
func NewConnection(context *zmq.Context, t zmq.Type, uri string, bind bool) (*ZmqConnection, error) {
	socket, err := context.NewSocket(t)
	if err != nil {
		return nil, fmt.Errorf("Failed to create ZMQ socket: %v", err)
	}

	identity := GenerateId()
	socket.SetIdentity(identity)

	if bind {
		err = socket.Bind(uri)
	} else {
		err = socket.Connect(uri)
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to establish connection to %v: %v", uri, err)
	}

	return &ZmqConnection{
		identity: identity,
		uri:      uri,
		socket:   socket,
		context:  context,
	}, nil
}

// SendData sends the byte array.
//
// If id is not "", the id is included as the first part of the message. This
// is useful for passing messages to a ROUTER socket so it can route them.
func (self *ZmqConnection) SendData(id string, data []byte) error {
	if id != "" {
		_, err := self.socket.SendMessage(id, [][]byte{data})
		if err != nil {
			return err
		}
	} else {
		_, err := self.socket.SendMessage([][]byte{data})
		if err != nil {
			return err
		}
	}

	return nil
}

// RecvData receives a ZMQ message from the wrapped socket and returns the
// identity of the sender and the data sent. If ZmqConnection does not wrap a
// ROUTER socket, the identity returned will be "".
func (self *ZmqConnection) RecvData() (string, []byte, error) {
	msg, err := self.socket.RecvMessage(0)

	if err != nil {
		return "", nil, err
	}
	switch len(msg) {
	case 1:
		data := []byte(msg[0])
		return "", data, nil
	case 2:
		id := msg[0]
		data := []byte(msg[1])
		return id, data, nil
	default:
		return "", nil, fmt.Errorf(
			"Receive message with unexpected length: %v", len(msg),
		)
	}
}

// Close closes the wrapped socket. This should be called with defer() after opening the socket.
func (self *ZmqConnection) Close() {
	self.socket.Close()
}

// Socket returns the wrapped socket.
func (self *ZmqConnection) Socket() *zmq.Socket {
	return self.socket
}

// Create a new monitor socket pair and return the socket for listening
func (self *ZmqConnection) Monitor(events zmq.Event) (*zmq.Socket, error) {
	endpoint := fmt.Sprintf("inproc://monitor.%v", self.identity)
	err := self.socket.Monitor(endpoint, events)
	if err != nil {
		return nil, err
	}
	monitor, err := self.context.NewSocket(zmq.PAIR)
	err = monitor.Connect(endpoint)
	if err != nil {
		return nil, err
	}

	return monitor, nil
}

// Identity returns the identity assigned to the wrapped socket.
func (self *ZmqConnection) Identity() string {
	return self.identity
}
