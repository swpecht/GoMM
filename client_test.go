package GoMM

import (
	"github.com/stretchr/testify/assert"
	"net"
	"sync"
	"testing"
	"time"
)

// Mock messenger for Client tests. Collects all sent messages into a channel
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

func GetClient_DataOnly(t *testing.T) *Client {
	c := new(Client)
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
	// timer := time.AfterFunc(1000*time.Millisecond, func() {
	// 	panic("Hung during barrier test!")
	// })
	// defer timer.Stop()

	c, _ := getMessagingClient(t)

	// Test single Client case, only active node, should return immediately
	c.Barrier()

	// Test with multiple active nodes
	activeNodes := []Node{GetNode(t), GetNode(t), c.node}
	activeNodes[0].Name = "Node0"
	activeNodes[1].Name = "Node1"
	c.updateActiveMemberList(activeNodes)

	blocked := false
	go func() {
		c.Barrier()
		if !blocked {
			t.Error("Barrier didn't block")
		}

	}()

	c.HandleMessage(GetBarrierMessage(t, activeNodes[0].Name))
	c.HandleMessage(GetBarrierMessage(t, activeNodes[2].Name))
	blocked = true
	c.HandleMessage(GetBarrierMessage(t, activeNodes[1].Name))

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
	// Create a Client that is not active
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
		matchANode := false
		for j := 0; j < len(pendingNodes); j++ {
			node := pendingNodes[j]
			tcpAddr := node.GetTCPAddr()
			if tcpAddr.String() == msg.Target {
				matchANode = true
			}
		}

		if matchANode == false {
			t.Errorf("TestClient_UpdateActiveMembers error: The message was sent to a non-existent node.")
		}
		assert.Equal(activateMsg, msg.Type)

	}

}

// Get a data only client with an attached messenger, and the
// channel that all sent messages will go to.
func getMessagingClient(t *testing.T) (*Client, chan Message) {
	c := GetClient_DataOnly(t)
	messenger, sent := NewMockMessenger()
	c.messenger = messenger
	return c, sent
}

func TestClient_Broadcast(t *testing.T) {
	// Need to make broadcast not send to itself
	// Then can implement a tree based broadcast
	// Can test the entire tree manually by calling broadcast
	// and then feeding in messages to the next clients handle message.
	// That way all the listeners and such won't have to be started.

	assert := assert.New(t)
	numClients := 3
	clients := make([]*Client, numClients)
	sendChans := make([]chan Message, numClients)
	activeNodes := make([]Node, numClients)
	for i := 0; i < numClients; i++ {
		clients[i], sendChans[i] = getMessagingClient(t)
		clients[i].node = GetNode(t)
		activeNodes[i] = clients[i].node
	}

	for i := 0; i < numClients; i++ {
		clients[i].updateActiveMemberList(activeNodes)
	}

	stringData := []string{"Hello", "World"}
	floatData := []float64{2.0, 48182.2}
	go clients[0].Broadcast(stringData, floatData)

	// Get the intial message to node0
	msg := <-sendChans[0]
	assert.Equal(broadcastMsg, msg.Type)
	assert.Equal(stringData, msg.StringData)
	assert.Equal(floatData, msg.FloatData)
	assert.Equal(0, msg.Origin)

	// Feed the message into client 0
	clients[0].HandleMessage(msg)

	// Should send 2 messages
	for i := 1; i < len(activeNodes); i++ {
		msg := <-sendChans[0]
		assert.Equal(broadcastMsg, msg.Type)
		assert.Equal(stringData, msg.StringData)
		assert.Equal(floatData, msg.FloatData)
		assert.Equal(activeNodes[i].GetStringAddr(), msg.Target)
	}

	// Shouldn't be any more messages
	for i := range activeNodes {
		close(sendChans[i])
	}

	for i := 1; i < len(activeNodes); i++ {
		// Will cause panic if try to send since chan is closed
		clients[i].HandleMessage(msg)
		<-clients[i].BroadcastChannel
	}

}

func TestClient_SendMsg(t *testing.T) {
	assert := assert.New(t)
	c, sent := getMessagingClient(t)
	activeNodes := []Node{GetNode(t), GetNode(t), c.node}
	c.updateActiveMemberList(activeNodes)

	sentMsg := Message{
		StringData: []string{"Sending test"},
		FloatData:  []float64{2.0, 48182.2},
	}

	go c.sendMsg(sentMsg, 0)
	msg := <-sent
	id, _ := c.ResolveId(0)
	assert.Equal(msg.Target, id)
	assert.Equal(sentMsg.StringData, msg.StringData)
	assert.Equal(sentMsg.FloatData, msg.FloatData)

	go c.sendMsg(sentMsg, 1)
	msg = <-sent
	id, _ = c.ResolveId(1)
	assert.Equal(msg.Target, id)
	assert.Equal(sentMsg.StringData, msg.StringData)
	assert.Equal(sentMsg.FloatData, msg.FloatData)

}

func TestClient_ResolveId(t *testing.T) {
	assert := assert.New(t)
	// Reset node num to avoidd previous test interfecernce
	nodeNumLock.Lock()
	nodeNum = 0
	nodeNumLock.Unlock()

	c := GetClient_DataOnly(t)
	activeNodes := []Node{GetNode(t), GetNode(t), c.node}
	c.updateActiveMemberList(activeNodes)

	node0, err := c.ResolveId(0)
	assert.Equal(activeNodes[0].GetStringAddr(), node0)
	assert.Nil(err)

	node1, err := c.ResolveId(1)
	assert.Equal(activeNodes[1].GetStringAddr(), node1)
	assert.Nil(err)

	node2, err := c.ResolveId(2)
	assert.Equal(activeNodes[2].GetStringAddr(), node2)
	assert.Nil(err)

	_, err = c.ResolveId(3)
	if err == nil {
		t.Error("Failed to handle id out of bounds")
	}

}

func TestClient_GetAddrId(t *testing.T) {
	assert := assert.New(t)
	// Reset node num to avoidd previous test interfecernce
	nodeNumLock.Lock()
	nodeNum = 0
	nodeNumLock.Unlock()

	c := GetClient_DataOnly(t)
	activeNodes := []Node{GetNode(t), GetNode(t), c.node}
	c.updateActiveMemberList(activeNodes)

	id, err := c.GetAddrId(activeNodes[0].GetStringAddr())
	assert.Equal(0, id)
	assert.Nil(err)

	id, err = c.GetAddrId(activeNodes[1].GetStringAddr())
	assert.Equal(1, id)
	assert.Nil(err)

	id, err = c.GetAddrId(activeNodes[2].GetStringAddr())
	assert.Equal(2, id)
	assert.Nil(err)

	_, err = c.GetAddrId("Junk")
	if err == nil {
		t.Error("Failed to handle addr not found")
	}

}

var childTests = []struct {
	id         int
	totalNodes int
	left       int
	right      int
}{
	{0, 0, -1, -1},

	{0, 7, 1, 2},
	{1, 7, 3, 4},
	{2, 7, 5, 6},
	{3, 7, -1, -1},

	{3, 15, 7, 8},
	{4, 15, 9, 10},
	{5, 15, 11, 12},
	{6, 15, 13, 14},
	{6, 14, 13, -1},
}

func TestClient_GetChildren(t *testing.T) {
	c := Client{}

	for _, tt := range childTests {
		left, right := c.getChildren(tt.id, tt.totalNodes)
		if left != tt.left {
			t.Errorf("TestClient_GetChildren failed left for id %d total %d. Expected %d, got %d", tt.id, tt.totalNodes, tt.left, left)
		}

		if right != tt.right {
			t.Errorf("TestClient_GetChildren failed right for id %d total %d. Expected %d, got %d", tt.id, tt.totalNodes, tt.right, right)
		}
	}

}
