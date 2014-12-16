package GoMM

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"strconv"
)

// messageType is an integer ID of a type of message that can be received
// on network channels from other members.
type messageType uint8

// The list of available message types.
const (
	activateMsg messageType = iota + 1
	ackMsg
	broadcastMsg
	barrierMsg
)

func CreateBroadcastMsg(stringData []string, floatData []float64) Message {
	msg := Message{
		Type:       broadcastMsg,
		StringData: stringData,
		FloatData:  floatData,
	}

	return msg
}

// Creates an activate message, where the first member of the string array
// contains an array of nodes
func createActivateMsg(activeMembers []Node) (Message, error) {
	nodesBytes, err := json.Marshal(activeMembers)
	if err != nil {
		log.Println("[ERROR] Failed to marshal nodes")
	}
	nodesString := string(nodesBytes)
	msg := Message{
		Type:       activateMsg,
		StringData: []string{nodesString},
	}

	return msg, err
}

func createBarrierMsg(source string) Message {
	return Message{
		Type:       barrierMsg,
		StringData: []string{source},
	}
}

func decodeActivateMsg(msg Message) ([]Node, error) {
	log.Println("[DEBUG] Decoding activate message", msg)
	var err error
	if msg.Type != activateMsg {
		log.Println("[ERROR] Tried to decodeActivateMsg on non-Activate type message")
		err = errors.New("Failed incorrect message type")
		return make([]Node, 0), err
	}

	nodesString := msg.StringData[0]
	var nodes []Node
	err = json.Unmarshal([]byte(nodesString), &nodes)
	if err != nil {
		log.Println("[ERROR] Failed to unmarshal node list: " + err.Error())
	}

	return nodes, err
}

// Activates all pending members
func (c *Client) activatePendingMembers() {
	// Create the appended list of active members
	c.ActiveMembersLock.Lock()
	activeMembers := make([]Node, len(c.ActiveMembers))
	var i int = 0
	for _, value := range c.ActiveMembers {
		activeMembers[i] = value
		i++
	}
	c.ActiveMembersLock.Unlock()

	c.pendingMembersLock.Lock()
	pendingMembers := make([]Node, len(c.pendingMembers))
	i = 0
	for _, value := range c.pendingMembers {
		pendingMembers[i] = value
		i++
	}
	// Clear pending members
	c.pendingMembers = make(map[string]Node)
	c.pendingMembersLock.Unlock()

	activeMembers = append(activeMembers, pendingMembers...)

	msg, _ := createActivateMsg(activeMembers)

	// Only send the activate message if node 0
	if c.GetId() == 0 {
		for i := 0; i < len(pendingMembers); i++ {
			tcpAddr := pendingMembers[i].GetTCPAddr()
			msg.Target = tcpAddr.String()
			msg.Origin = c.GetId()
			err := c.messenger.Send(msg)
			if err == nil {
				log.Println("[DEBUG] Activate message sent to: ", tcpAddr.String())
			}
		}
	}
	// TODO: Race condition here where is node 0 is just activated and a
	// node joined between the time this pending list is built, but before
	// before the new node 0 is activated, the node would essentially be lost.

	// Update the active members on the local node
	log.Println("[DEBUG] Total active nodes: " + strconv.Itoa(len(activeMembers)))
	c.updateActiveMemberList(activeMembers)
}

// Returns a connection to the specified node
// TODO use a connection pool for speed
func (c *Client) getTCPConection(node Node) (*net.TCPConn, error) {

	tcpAddr := node.GetTCPAddr()
	tcp_conn, err := net.DialTCP("tcp", nil, &tcpAddr)
	if err != nil {
		log.Println("[ERROR] Failed to get tcp connection to ", node.GetTCPAddr())
	}

	return tcp_conn, err
}

// Checks if a join address refers to a node joining
func isSelfAddr(rAddr string, memberlistPort int) bool {

	tcpAddr, err := net.ResolveTCPAddr("tcp", rAddr)
	if err != nil {
		// Invalid addr so cant be a match
		return false
	}
	// First check the ports
	if tcpAddr.Port != memberlistPort {
		return false
	}

	// Error occurs, can't tell, so assume false
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return false
	}

	// Check against all addresses
	for _, address := range addrs {
		ipnet, ok := address.(*net.IPNet)
		if !ok {
			continue
		}

		ip4 := ipnet.IP.To4()
		if ip4 == nil {
			continue
		}
		log.Println("[DEBUG]", ip4.String(), tcpAddr.IP.String())
		if ip4.String() == tcpAddr.IP.String() {
			return true
		}
	}

	return false
}
