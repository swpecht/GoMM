package GoMM

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type MockMessageHandler struct {
	t        *testing.T
	expected Message
}

func (handler MockMessageHandler) HandleMessage(msg Message) {
	assert := assert.New(handler.t)
	assert.Equal(handler.expected, msg)
}

func TestMessaging_TCPMessenger(t *testing.T) {
	assert := assert.New(t)
	messenger0, err := GetTCPMessenger("Messenger0", "localhost:5000")
	if err != nil {
		t.Errorf("Failed to create messenger 0: %s", err.Error())
	}
	messenger1, err := GetTCPMessenger("Messenger1", "localhost:5001")
	if err != nil {
		t.Errorf("Failed to create messenger 1: %s", err.Error())
	}

	msgTo1 := Message{
		Target:     "localhost:5001",
		StringData: []string{"Message to 1"},
	}

	recvrChannel := make(chan Message)
	go messenger1.Recv(recvrChannel)
	// Give time for tcp listener to start
	time.Sleep(time.Millisecond * 1000)
	err = messenger0.Send(msgTo1)
	if err != nil {
		t.Errorf("Failed to send message %s", err.Error())
	}

	msgRecvd := <-recvrChannel

	assert.Equal(msgTo1, msgRecvd)

}

func TestMessaging_ChannelMessenger(t *testing.T) {
	assert := assert.New(t)
	resolverMap := make(map[string]chan Message)
	messengers := GetChannelMessengers([]string{"Messenger0", "Messenger1"}, resolverMap)
	messenger0 := messengers[0]
	messenger1 := messengers[1]

	msgTo1 := Message{
		Target:     "Messenger1",
		StringData: []string{"Message to 1"},
	}
	recvrChannel := make(chan Message)

	timer := time.AfterFunc(500*time.Millisecond, func() {
		panic("Hung sending message!")
	})
	defer timer.Stop()

	go messenger1.Recv(recvrChannel)
	err := messenger0.Send(msgTo1)
	if err != nil {
		t.Errorf("Failed to send message", err.Error())
	}
	msgRecvd := <-recvrChannel

	assert.Equal(msgTo1, msgRecvd)

	go messenger0.Send(msgTo1)
	go messenger1.Recv(recvrChannel)
	msgRecvd = <-recvrChannel
	assert.Equal(msgTo1, msgRecvd)

	// Check invalid send
	invalidMessage := Message{
		Target: "Fake Messenger",
	}
	err = messenger0.Send(invalidMessage)
	if err == nil {
		t.Errorf("Failed to handle invalid address on send")
	}
}

func TestMessaging_Listener(t *testing.T) {
	timeout := time.AfterFunc(500*time.Millisecond, func() {
		panic("Failed to stop listener!")
	})
	defer timeout.Stop()

	assert := assert.New(t)
	resolverMap := make(map[string]chan Message)
	messengers := GetChannelMessengers([]string{"Messenger0", "Messenger1"}, resolverMap)
	messenger0 := messengers[0]
	messenger1 := messengers[1]

	msgTo1 := Message{
		Target:     "Messenger1",
		StringData: []string{"Listener test"},
	}

	msgHandler := MockMessageHandler{
		t:        t,
		expected: msgTo1,
	}

	l := NewListener(msgHandler)
	// Test early stop
	err := l.Stop()
	if err == nil {
		t.Error("Failed to detect early stop")
	}
	go l.Listen(messenger1)

	// Should immediately allow sending both messages
	messenger0.Send(msgTo1)
	messenger0.Send(msgTo1)

	// Test starting listener twice
	err = l.Listen(messenger1)
	if err == nil {
		t.Error("Allowed listener to be started twice")
	}

	// Test stopping
	l.Stop()
	assert.False(l.isRunning)
	time.AfterFunc(100*time.Millisecond, func() {
		l.Stop()
	})

	// Is blocking, if stop is not called, it will time out
	l.Listen(messenger1)
}
