package GoMM


import (
	"github.com/stretchr/testify/assert"
	"net"
	"sync"
	"testing"
	"time"
)

// Mock messenger for client tests. Collects all sent messages into a channel
// Does no resolving. Has a capacity for 50 sent messages
type MockMessenger struct {
	sentMessages chan Message
}

func NewMockMessenger() (Messenger, chan Message) {
	msgChan := make(chan Message, 50)
	return MockMessenger{
		sentMessages: msgChan,
	}, msgChan
}

// Add the message to the sent queue
func (messenger MockMessenger) Send(msg Message) error {
	messenger.sentMessages <- msg
	return nil
}

func (messenger MockMessenger) Recv(channel chan Message) error {
	return nil
}

// Does nothing
func (messenger MockMessenger) resolve(addr string) (interface{}, error) {
	return nil, nil
}

var nodeNumLock sync.Mutex
var nodeNum int = 0

func GetNode(t *testing.T) Node {
	nodeNumLock.Lock()
	addr := net.ParseIP("0.0.0.0")

	node := Node{
		Name: string(nodeNum),
		Port: nodeNum,
		Addr: addr,
	}
	nodeNum++
	nodeNumLock.Unlock()
	return node
}

func GetClient_DataOnly(t *testing.T) *client {
	c := new(client)
	f := ClientFactory{}
	f.initializeData(c)

	c.ActiveMembers[c.node.Name] = c.node
	return c
}

func GetActivateMessage(t *testing.T, nodes []Node) Message {
	msg, err := createActivateMsg(nodes)
	if err != nil {
		t.Errorf("Failed to create activate message")
	}

	return msg
}

func GetBarrierMessage(t *testing.T, source string) Message {
	msg := createBarrierMsg(source)
	return msg
}

func TestClient_IsActive(t *testing.T) {
	assert := assert.New(t)
	c := GetClient_DataOnly(t)

	assert.True(c.IsActive())

	c.updateActiveMemberList([]Node{})
	assert.False(c.IsActive())

}

func TestClient_Barrier(t *testing.T) {
	assert := assert.New(t)
	timer := time.AfterFunc(500*time.Millisecond, func() {
		panic("Hung during barrier test!")
	})
	defer timer.Stop()

	c := GetClient_DataOnly(t)

	// Test single client case, only active node, should return immediately
	c.Barrier()

	// Test with multiple active nodes
	messenger, sent := NewMockMessenger()
	c.messenger = messenger
	activeNodes := []Node{GetNode(t), GetNode(t), c.node}
	c.updateActiveMemberList(activeNodes)

	blocked := false
	go func() {
		c.Barrier()
		if !blocked {
			t.Error("Barrier didn't block")
		}

	}()
	// Should send 3 messages
	for i := 0; i < len(activeNodes); i++ {
		msg := <-sent
		assert.Equal(barrierMsg, msg.Type)
	}

	c.HandleMessage(GetBarrierMessage(t, activeNodes[0].Name))
	c.HandleMessage(GetBarrierMessage(t, activeNodes[1].Name))
	blocked = true
	c.HandleMessage(GetBarrierMessage(t, activeNodes[2].Name))

}

func TestClient_HandleActivate(t *testing.T) {
	assert := assert.New(t)
	c := GetClient_DataOnly(t)
	emptyActivate := GetActivateMessage(t, []Node{})
	c.handleActivateMessage(emptyActivate)
	assert.Equal(0, c.NumActiveMembers())

	node := GetNode(t)
	sameActivate := GetActivateMessage(t, []Node{node, node})
	c.handleActivateMessage(sameActivate)
	assert.Equal(1, c.NumActiveMembers())

	twoActivate := GetActivateMessage(t, []Node{GetNode(t), GetNode(t)})
	c.handleActivateMessage(twoActivate)
	assert.Equal(2, c.NumActiveMembers())

}

func TestClient_WaitActive(t *testing.T) {
	assert := assert.New(t)
	// Create a client that is not active
	c := GetClient_DataOnly(t)
	c.updateActiveMemberList([]Node{})
	assert.False(c.IsActive())

	timeout := time.AfterFunc(500*time.Millisecond, func() {
		panic("TestClient_WaitActive timed out!")
	})
	defer timeout.Stop()

	// Update the active list with its self
	go c.updateActiveMemberList([]Node{c.node})
	c.WaitActive()

}

func TestClient_UpdateActiveMembers(t *testing.T) {
	assert := assert.New(t)
	timer := time.AfterFunc(500*time.Millisecond, func() {
		panic("Hung during UpdateActiveMembers test!")
	})
	defer timer.Stop()

	c := GetClient_DataOnly(t)

	messenger, sent := NewMockMessenger()
	c.messenger = messenger
	activeNodes := []Node{c.node}
	pendingNodes := []Node{GetNode(t), GetNode(t)}

	c.updateActiveMemberList(activeNodes)
	for i := range pendingNodes {
		c.pendingMembers[pendingNodes[i].Name] = pendingNodes[i]
	}
	go c.UpdateActiveMembers()

	// Should send messages to both pending and active
	for i := 0; i < len(pendingNodes); i++ {
		msg := <-sent
		node := pendingNodes[i]
		tcpAddr := node.GetTCPAddr()
		assert.Equal(tcpAddr.String(), msg.Target)
		assert.Equal(activateMsg, msg.Type)

	}

}

func TestClient_Broadcast(t *testing.T) {
	assert := assert.New(t)
	c := GetClient_DataOnly(t)

	messenger, sent := NewMockMessenger()
	c.messenger = messenger
	activeNodes := []Node{GetNode(t), GetNode(t), c.node}
	c.updateActiveMemberList(activeNodes)

	stringData := []string{"Hello", "World"}
	floatData := []float64{2.0, 48182.2}
	go c.Broadcast(stringData, floatData)

	// Should send 3 messages
	for i := 0; i < len(activeNodes); i++ {
		msg := <-sent
		assert.Equal(broadcastMsg, msg.Type)
		assert.Equal(stringData, msg.StringData)
		assert.Equal(floatData, msg.FloatData)
	}

}
