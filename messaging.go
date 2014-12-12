package GoMM

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"sync"
)

// Interface for sending peer to peer messages
type Messenger interface {
	// Blocking call to send a message
	Send(msg Message) error
	// Blocking call to receive a message
	Recv(channel chan Message) error
	// Resolve an address to a given interface for sending
	// The returned type will depend on the implementation
	resolve(addr string) (interface{}, error)
}

// Messages that can be sent by messengers
type Message struct {
	Type       messageType
	Target     string
	Origin     int // Origin of the message
	StringData []string
	FloatData  []float64
}

// Implements the messenger interface over channels. Useful for local
// testing and debugging.
type ChannelMessenger struct {
	ResolverMap map[string]chan Message // Used by the resolver function to send messages
	Incoming    chan Message
}

func (messenger ChannelMessenger) Send(msg Message) error {
	var resolved interface{}
	resolved, err := messenger.resolve(msg.Target)
	if err != nil {
		return err
	}
	targetChan, ok := resolved.(chan Message)
	if !ok || targetChan == nil {
		errorMsg := "Failed to convert the resolved value to a channel"
		err = errors.New(errorMsg)
		log.Println("[ERROR]", errorMsg, resolved)
		return err
	}

	log.Println("[DEBUG] Sending message", msg, "over", targetChan)
	targetChan <- msg
	return nil
}

func (messenger ChannelMessenger) Recv(channel chan Message) error {
	log.Println("[DEBUG] Waiting on receive on", messenger.Incoming)
	incomingMessage := <-messenger.Incoming
	log.Println("[DEBUG] Message received", incomingMessage)
	channel <- incomingMessage
	return nil
}

func (messenger ChannelMessenger) resolve(addr string) (interface{}, error) {
	channel, ok := messenger.ResolverMap[addr]
	var err error
	if !ok {
		log.Println("[ERROR] Failed to resolve", addr)
		for k, _ := range messenger.ResolverMap {
			log.Println("[ERROR] Valid choices are", k)
		}
		err = errors.New("Address not found!")
	}
	log.Println("[DEBUG] Resolved", addr, "to", channel)
	return channel, err
}

// Listener for messengers
type Listener struct {
	sync.Mutex // Embedded lock
	// Channel to determe if the listener was stopped
	stop      chan bool
	isRunning bool
	handler   MessageHandler
}

type MessageHandler interface {
	HandleMessage(msg Message)
}

func NewListener(msgHandler MessageHandler) Listener {
	return Listener{
		isRunning: false,
		stop:      make(chan bool),
		handler:   msgHandler,
	}
}

// Blocking call that polls for call
func (l *Listener) Listen(messenger Messenger) error {
	l.Lock()
	if l.isRunning == true {
		log.Println("[ERROR] Attempt to start listener while running")
		l.Unlock()
		return errors.New("Alreading running!")
	}
	l.isRunning = true
	l.Unlock()
	log.Println("[DEBUG] Starting listener")

	recvChan := make(chan Message)
	for {
		go messenger.Recv(recvChan)
		stopped := l.waitForRecvOrStop(recvChan)
		if stopped == true {
			log.Println("[DEBUG] Stop received for listener")
			close(recvChan)
			return nil
		}
	}
	return nil
}

// Waits for a message ot be recieved or the stop signal to be sent
func (l *Listener) waitForRecvOrStop(recv chan Message) bool {
	for {
		select {
		case <-l.stop:
			return true
		case msg := <-recv:
			go func() { l.handler.HandleMessage(msg) }()
			return false
		}
	}
}

func (l *Listener) Stop() error {
	l.Lock()
	defer l.Unlock()
	if l.isRunning == false {
		log.Println("[ERROR] Attempt to stop listener while not running")
		return errors.New("Attempt to stop non running listener")
	}
	l.stop <- true
	l.isRunning = false
	return nil
}

// Messenger over TCP
// http://stackoverflow.com/questions/19167970/mock-functions-in-golang
// Good thoughts on how to mock out the messageing
// https://eclipse.org/paho/clients/golang/
// http://golang.org/pkg/net/textproto/
// Good messaging library
type TCPMessenger struct {
	// connectionPool map[string]*net.TCPConn
	listenAddr *net.TCPAddr // Local address to listen on
	Name       string
	listener   *net.TCPListener
}

// Encodes a messafe for sending over a tcp connection. Format is:
// {len in}\n{msgbody}
func (messenger *TCPMessenger) Encode(msg Message) (outputMsg string, err error) {
	msgBody, err := json.Marshal(msg)
	if err != nil {
		log.Println("[ERROR] Failed to encode message: " + err.Error())
	}
	outputMsg += string(msgBody) + string('\n')
	return
}

func (messenger *TCPMessenger) getConnection(address string) (*net.TCPConn, error) {
	remoteAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialTCP("tcp", nil, remoteAddr)

	return conn, err
}

func (messenger *TCPMessenger) getListener() (*net.TCPListener, error) {
	if messenger.listener != nil {
		// Already created
		return messenger.listener, nil
	}

	listener, err := net.ListenTCP("tcp", messenger.listenAddr)
	if err != nil {
		log.Println("[ERROR] Error starting listener %s", err.Error())
		return nil, err
	}
	log.Printf("[DEBUG] Listener started for %s", messenger.listenAddr.String())
	messenger.listener = listener
	return messenger.listener, nil
}

func (messenger *TCPMessenger) Decode(b []byte) (Message, error) {
	var msg Message
	err := json.Unmarshal(b, &msg)
	if err != nil {
		log.Println("[ERROR] Failed to unmarshal message")
	}

	return msg, err
}

// handleConn handles a single incoming TCP connection
func (messenger *TCPMessenger) handleConn(c *net.TCPConn, channel chan Message) {
	msg, err := messenger.recvMessage(c)
	if err != nil {
		log.Println("[ERROR] Failed to rcvmessage: " + err.Error())
	}
	if err == io.EOF {
		log.Println("[DEBUG] Closing connection.")
		c.Close()
		return
	}
	// log.Println("[DEBUGMessage recieved ", msg)
	// Quesues messages for processing in the channel
	channel <- msg

}

func (messenger *TCPMessenger) Recv(channel chan Message) error {
	listener, err := messenger.getListener()
	if err != nil {
		return err
	}

	log.Printf("[DEBUG] Awaiting connection on %s", messenger.listenAddr.String())
	conn, err := listener.AcceptTCP()
	if err != nil {
		return err
	}

	log.Printf("[DEBUG] Connection received on %s from %s", messenger.listenAddr.String(), conn.RemoteAddr().String())
	msg, err := messenger.recvMessage(conn)
	if err != nil {
		return err
	}

	channel <- msg
	return nil
}

func (messenger *TCPMessenger) Send(msg Message) error {
	conn, err := messenger.getConnection(msg.Target)
	if err != nil {
		return err
	}

	err = messenger.sendMessage(conn, msg)
	return err
}

func (messenger *TCPMessenger) resolve(addr string) (interface{}, error) {
	return net.ResolveTCPAddr("tcp", addr)
}

// Receive a message over a tcp connections, and unmarshal it from JSON
func (messenger *TCPMessenger) recvMessage(conn *net.TCPConn) (Message, error) {

	reader := bufio.NewReader(conn)
	b, err := reader.ReadBytes('\n')
	if err != nil {
		log.Println("[ERROR] Failed to read message")
		return Message{}, err
	}
	log.Printf("[DEBUG] Message recieved: %s", b)
	msg, err := messenger.Decode(b)
	return msg, err
}

// Marshal the message and send it over a given TCP connection
func (messenger *TCPMessenger) sendMessage(conn *net.TCPConn, msg Message) error {
	// Serialize the message
	msgString, err := messenger.Encode(msg)
	if err != nil {
		return err
	}
	log.Println("[DEBUG] Sending message: " + msgString)
	io.Copy(conn, bytes.NewBufferString(msgString))
	//This is probably timing out, may beed to use a thread ppol
	//_, err = conn.Write([]byte(msgString))
	return err
}

func (messenger *TCPMessenger) Close() error {
	if messenger.listener != nil {
		return messenger.listener.Close()
	}

	return nil
}
