package GoMM

import (
	"errors"
	"github.com/hashicorp/memberlist"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Client struct {
	memberTracker *memberlist.Memberlist // Underlying tracker to

	pendingMembersLock sync.Mutex
	pendingMembers     map[string]Node // Members that are online, but not active
	ActiveMembersLock  sync.Mutex
	ActiveMembers      map[string]Node // Members that are online and active, mapped by the memberlist.Node.Name
	Name               string          // Unique name of the Client
	node               Node            // Used for TCP communications

	messenger Messenger
	listener  Listener

	barrierChannel chan string // The channel that handles barrier message, will be the name of the node that sent the barrier
	// Channel for recieve broadcast messages
	BroadcastChannel chan Message
}

func (c Client) NumMembers() int {
	return c.memberTracker.NumMembers()
}

func (c *Client) NumActiveMembers() int {
	c.ActiveMembersLock.Lock()
	num := len(c.ActiveMembers)
	c.ActiveMembersLock.Unlock()
	return num
}

// Cause a node to join another memberlist group. This function removes this
// node from the active list. Further more, this should only be called
// when a node is alone in it's undelying memberlist. Therefore, a group
// of nodes cannot merge with another group, but the sub group must all join
// individually. Should this be blocking until the node is made active?
func (c *Client) Join(address string) {
	c.memberTracker.Join([]string{address})
	c.updateActiveMemberList([]Node{})
	return
}

func (c Client) HandleMessage(msg Message) {
	// log.Println("[DEBUG]", c.node.Name, " Message received", msg)
	switch msg.Type {
	case activateMsg:
		c.handleActivateMessage(msg)
		break
	case barrierMsg:
		c.barrierChannel <- msg.StringData[0] // Pass on the name, will be handled on the calling thread
		break
	case broadcastMsg:
		c.BroadcastChannel <- msg
	default:
		log.Println("[ERROR] Unknown message type")
	}
}

func (c *Client) handleActivateMessage(msg Message) {

	activeNodes, err := decodeActivateMsg(msg)
	if err != nil {
		log.Println("[ERROR] Received malformed activate message")
		return
	}
	c.updateActiveMemberList(activeNodes)
	log.Println("[DEBUG]", c.Name, "IsActive", c.IsActive(), "total active nodes: "+strconv.Itoa(len(activeNodes)))
}

func (c *Client) Start() error {
	// Start event processing
	c.listener = NewListener(c)
	go c.listener.Listen(c.messenger)

	var config *memberlist.Config = memberlist.DefaultLocalConfig()
	config.BindPort = c.node.Port - 100 // off set for tcp
	config.Name = c.Name
	config.AdvertisePort = c.node.Port - 100 // off set for tcp
	config.Events = c

	list, err := memberlist.Create(config)
	if err != nil {
		log.Println("[ERROR] Failed to create member list for", c.Name, "Error: ", err.Error())
	}
	log.Println("[DEBUG] Started memberlist for", c.Name)
	c.memberTracker = list

	log.Println("[DEBUG] Started Client", c.Name)

	return nil
}

func (c *Client) Close() {
	c.memberTracker.Leave(time.Millisecond * 500)
	log.Println("[DEBUG]", c.Name, "left memberTracker")
	c.listener.Stop()
	// Not totally sure how closing channels works TODO
	close(c.barrierChannel)
	log.Println("[DEBUG]", c.Name, "shut down")
}

// Wait until the Client is active
func (c *Client) WaitActive() {
	for {
		if c.IsActive() == true {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
}

// Allows members currently waiting to become active to become active,
// this method blocks and requires that all current active members
// have also called this method.
func (c *Client) UpdateActiveMembers() int {
	// Need to ensure all active members have decided to do this
	c.Barrier()
	c.activatePendingMembers()
	// Need to send go ahead message to new members to be made active
	return c.NumActiveMembers()
}

func (c *Client) updateActiveMemberList(members []Node) {

	c.ActiveMembersLock.Lock()

	// Delete everything in the map, can't just make a new one, otherwise
	// the references can be broken across threads
	for k := range c.ActiveMembers {
		delete(c.ActiveMembers, k)
	}

	for i := range members {
		c.ActiveMembers[members[i].Name] = members[i]
	}

	log.Println("[DEBUG] Updateing active member list with:", c.ActiveMembers)

	c.ActiveMembersLock.Unlock()

}

// Determine if the given Client is in the active pool
func (c *Client) IsActive() bool {
	c.ActiveMembersLock.Lock()
	_, ok := c.ActiveMembers[c.Name]
	c.ActiveMembersLock.Unlock()
	return ok
}

// Barrier that blacks for all active nodes
func (c *Client) Barrier() {
	if !c.IsActive() {
		panic("Client is not active!")
	}
	if c.NumActiveMembers() == 1 {
		// This is the only member so can return instantly
		return
	}

	// Need to broadcast the barrier message
	msg := createBarrierMsg(c.Name)

	log.Println("[DEBUG] Broadcasting barrier from", c.Name)
	c.broadCastMsg(msg)

	// Wait for each node to respond
	//Get messages from channel

	responded := make(map[string]bool)
PollingLoop:
	for {
		select {
		case name := <-c.barrierChannel:
			responded[name] = true
			log.Println("[DEBUG]", c.Name, "Received barrier from", name, len(responded), "of", c.NumActiveMembers())
		default:
			if len(responded) == c.NumActiveMembers() {
				log.Println("[DEBUG] Barrier completed by", c.Name)
				break PollingLoop // everyone is at the barrier
			}
		}
	}
}

// Send message to all nodes
// TODO implement a tree rather than naive send to all
func (c *Client) Broadcast(stringData []string, floatData []float64) {
	msg := CreateBroadcastMsg(stringData, floatData)
	c.broadCastMsg(msg)
	// Need to implement a less naive broadcast using a tree structure
	// Will involvde adding a broadcast handler to echo the messageType
	// The most difficult part will be determining who to send to

	// Make broadcast off of ids - make a send message function that
	// sends to a given id
}

// Sends a message to a node with the supplied id
func (c *Client) sendMsg(msg Message, targetId int) error {
	target, err := c.ResolveId(targetId)
	if err != nil {
		return err
	}
	msg.Target = target
	err = c.messenger.Send(msg)

	return err
}

// Resolve the id to a client address. The id is currently based on
// the sorted string order of the nodes address.
func (c *Client) ResolveId(id int) (string, error) {
	c.ActiveMembersLock.Lock()
	defer c.ActiveMembersLock.Unlock()

	// Check valid id
	if id < 0 || id > len(c.ActiveMembers)-1 {
		return "", errors.New("Id out of bounds")
	}

	// Generate a list of addresses
	memberAddresses := make([]string, len(c.ActiveMembers))
	i := 0
	for _, v := range c.ActiveMembers {
		addr := v.GetTCPAddr()
		memberAddresses[i] = addr.String()
		i++
	}

	sort.Strings(memberAddresses)

	return memberAddresses[id], nil
}

func (c *Client) broadCastMsg(msg Message) {
	c.ActiveMembersLock.Lock()
	numMembers := len(c.ActiveMembers)
	c.ActiveMembersLock.Unlock()

	log.Println("[DEBUG] Broadcasting message to", numMembers, "nodes")
	for i := 0; i < numMembers; i++ {
		err := c.sendMsg(msg, i)
		if err != nil {
			log.Println("[ERROR] Failed to broadcast message to node", i)
		}
	}

}

func (c Client) NotifyJoin(n *memberlist.Node) {
	new_node := Node{
		Name: n.Name,
		Addr: n.Addr,
		Port: int(n.Port) + 100, // Add 100 for the port offset
	}

	if n.Name == c.Name {
		// The initial self notification
		c.ActiveMembersLock.Lock()
		c.ActiveMembers[c.Name] = new_node
		c.ActiveMembersLock.Unlock()
		return
	}

	c.pendingMembersLock.Lock()
	c.pendingMembers[n.Name] = new_node
	c.pendingMembersLock.Unlock()
}

func (c Client) NotifyLeave(n *memberlist.Node) {
	log.Println("[DEBUG]", n.Name, "left")
	c.ActiveMembersLock.Lock()
	// Delete the node from active members
	delete(c.ActiveMembers, n.Name)
	c.ActiveMembersLock.Unlock()

	// Delete the node from pending members
	c.pendingMembersLock.Lock()
	delete(c.pendingMembers, n.Name)
	c.pendingMembersLock.Unlock()
}

func (c Client) NotifyUpdate(n *memberlist.Node) {

}
