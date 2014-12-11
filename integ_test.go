package GoMM


import (
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
	"time"
)

// Get clients for the test
func GetClients(t *testing.T, num int, headName string) []client {
	factory := ClientFactory{}

	// Create clients
	clients := make([]client, num)
	clientNames := make([]string, num)
	for i := 0; i < num; i++ {
		clients[i] = factory.NewClient()

		tcpAddr := clients[i].node.GetTCPAddr()
		clientNames[i] = tcpAddr.String()
		log.Println("[DEBUG] Created client", tcpAddr.String())
	}

	// Create channel messengers
	resolverMap := make(map[string]chan Message)
	messengers := GetChannelMessengers(clientNames, resolverMap)

	// Attach chennel messengers to clients
	for i := 0; i < num; i++ {
		clients[i].messenger = messengers[i]
	}

	return clients

}

func TestInteg_ChannelMessenger(t *testing.T) {
	assert := assert.New(t)

	timeout := time.AfterFunc(2000*time.Millisecond, func() {
		panic("TestInteg_ChannelMessenger timed out!")
	})
	defer timeout.Stop()

	headName := "0.0.0.0:7946"
	clients := GetClients(t, 3, headName)

	for i := range clients {
		clients[i].Start()
	}

	clients[1].Join(headName)
	clients[2].Join(headName)
	num_clients := clients[0].NumMembers()
	assert.Equal(num_clients, 3, "Incorrect num of initial clients")

	// Test tracking of active nodes
	assert.Equal(1, clients[0].NumActiveMembers(), "bad initial active members")
	assert.Equal(0, clients[1].NumActiveMembers(), "Not purging active after join")

	// Test tracking of pending nodes
	assert.Equal(2, len(clients[0].pendingMembers), "Not tracking pending members")

	num_active := clients[0].UpdateActiveMembers()
	assert.Equal(3, num_active, "invlaid new number of active members.")

	clients[1].WaitActive()
	clients[2].WaitActive()
	assert.Equal(3, clients[1].NumActiveMembers(), "invlaid new number of active members.")
	assert.Equal(3, clients[2].NumActiveMembers(), "invlaid new number of active members.")

	assert.True(clients[1].IsActive())
	assert.True(clients[2].IsActive())

	clients[1].Close()
	time.Sleep(time.Millisecond * 50)
	assert.Equal(2, clients[0].NumActiveMembers(), "Didn't handle client leaving")
	assert.Equal(2, clients[2].NumActiveMembers(), "Didn't handle client leaving")

}
