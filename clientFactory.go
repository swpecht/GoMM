package GoMM


import (
	"github.com/hashicorp/memberlist"
	"net"
	"strconv"
)

const (
	memberlist_starting_port int = 7946
	tcp_offset               int = 100
)

type ClientFactory struct {
	num_created int
}

func (f *ClientFactory) NewClient() (c client) {
	c = client{}
	f.initializeData(&c)

	f.num_created += 1

	return
}

func (f *ClientFactory) initializeData(c *client) {
	// Initialize variables
	c.ActiveMembers = make(map[string]Node)
	c.pendingMembers = make(map[string]Node)
	c.barrierChannel = make(chan string)

	var config *memberlist.Config = memberlist.DefaultLocalConfig()
	c.Name = config.Name + ":" + strconv.Itoa(memberlist_starting_port) + "-" + strconv.Itoa(f.num_created)

	// Configure the local Node data
	c.node = Node{
		Name: c.Name,
		Addr: net.ParseIP("10.0.2.15"),
		Port: config.BindPort + tcp_offset + f.num_created,
	}
}
