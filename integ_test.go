package GoMM

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestInteg_TCPMessenger(t *testing.T) {
	timeout := time.AfterFunc(2000*time.Millisecond, func() {
		panic("TestInteg_TCPMessenger timed out!")
	})
	defer timeout.Stop()

	clients, err := GetTCPClients(3)
	if err != nil {
		t.Errorf("Failed to create clients %s", err.Error())
		return
	}
	testClients(t, clients)
}

func TestInteg_ChannelMessenger(t *testing.T) {

	timeout := time.AfterFunc(2000*time.Millisecond, func() {
		panic("TestInteg_ChannelMessenger timed out!")
	})
	defer timeout.Stop()

	clients := GetLocalClients(3)

	testClients(t, clients)

}

func testClients(t *testing.T, clients []Client) {
	assert := assert.New(t)

	for i := range clients {
		clients[i].Start()
	}

	headName := clients[0].JoinAddr()
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
	time.Sleep(time.Millisecond * 500)
	assert.Equal(2, clients[0].NumActiveMembers(), "Didn't handle client leaving")
	assert.Equal(2, clients[2].NumActiveMembers(), "Didn't handle client leaving")

	clients[0].Close()
	clients[2].Close()
}
