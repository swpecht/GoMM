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

func (f *ClientFactory) NewClient() (c Client) {
	c = Client{}
	f.initializeData(&c)

	f.num_created += 1

	return
}

func (f *ClientFactory) initializeData(c *Client) error {
	// Initialize variables
	c.ActiveMembers = make(map[string]Node)
	c.pendingMembers = make(map[string]Node)
	c.barrierChannel = make(chan string)
	c.BroadcastChannel = make(chan Message, 10)

	var config *memberlist.Config = memberlist.DefaultLocalConfig()
	c.Name = config.Name + ":" + strconv.Itoa(memberlist_starting_port) + "-" + strconv.Itoa(f.num_created)

	// Configure the local Node data
	address, err := f.getNonLoopBackAddress()

	c.node = Node{
		Name:           c.Name,
		Addr:           address,
		Port:           config.BindPort + tcp_offset + f.num_created,
		MemberlistPort: config.BindPort + f.num_created,
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

// Get Clients for the test
func GetLocalClients(num int) []Client {
	factory := ClientFactory{}

	// Create clients
	clients := make([]Client, num)
	ClientNames := make([]string, num)
	for i := 0; i < num; i++ {
		clients[i] = factory.NewClient()

		tcpAddr := clients[i].node.GetTCPAddr()
		ClientNames[i] = tcpAddr.String()
		log.Println("[DEBUG] Created Client", tcpAddr.String())
	}

	// Create channel messengers
	resolverMap := make(map[string]chan Message)
	messengers := GetChannelMessengers(ClientNames, resolverMap)

	// Attach chennel messengers to clients
	for i := 0; i < num; i++ {
		clients[i].messenger = messengers[i]
	}

	return clients
}

// Get TCP clients
func GetTCPClients(num int) ([]Client, error) {
	factory := ClientFactory{}

	clients := make([]Client, num)
	for i := 0; i < num; i++ {
		clients[i] = factory.NewClient()
		tcpAddr := clients[i].node.GetTCPAddr()
		messenger, err := GetTCPMessenger(tcpAddr.String(), tcpAddr.String())
		for err != nil {
			// If already in use, try a different port
			log.Printf("[ERROR] Failed to create client: %s. Incrementing port and trying again", tcpAddr.String())
			clients[i].node.Port += 1
			clients[i].node.MemberlistPort += 1
			tcpAddr = clients[i].node.GetTCPAddr()
			messenger, err = GetTCPMessenger(tcpAddr.String(), tcpAddr.String())
		}
		clients[i].messenger = messenger
	}

	return clients, nil
}
