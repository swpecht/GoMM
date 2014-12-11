package GoMM

import (
	"github.com/hashicorp/memberlist"
	"log"
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

func (f *ClientFactory) initializeData(c *client) error {
	// Initialize variables
	c.ActiveMembers = make(map[string]Node)
	c.pendingMembers = make(map[string]Node)
	c.barrierChannel = make(chan string)

	var config *memberlist.Config = memberlist.DefaultLocalConfig()
	c.Name = config.Name + ":" + strconv.Itoa(memberlist_starting_port) + "-" + strconv.Itoa(f.num_created)

	// Configure the local Node data
	address, err := f.getNonLoopBackAddress()

	c.node = Node{
		Name: c.Name,
		Addr: address,
		Port: config.BindPort + tcp_offset + f.num_created,
	}

	return err
}

func (f *ClientFactory) getNonLoopBackAddress() (net.IP, error) {
	// https://www.socketloop.com/tutorials/golang-how-do-I-get-the-local-ip-non-loopback-address

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return net.IP{}, err
	}

	for _, address := range addrs {

		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP, err
			}

		}
	}

	return net.IP{}, err
}

// Get clients for the test
func GetLocalClients(num int, headName string) []client {
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
