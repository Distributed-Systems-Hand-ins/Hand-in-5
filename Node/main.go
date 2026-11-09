
package main

import (
	"fmt"
	"bufio"
	"math/rand"
	"sort"
	"net"
	"strconv"
	"strings"
	"time"
)

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Print(err)
		return
	}

	defer listener.Close()
	// Start hosting the auction
	go hostAuction()

	// Continously accept incoming connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Print("Error accepting connection: ", err)
		}

		// Handle the connection in a new goroutine
		go handleConnection(conn)
	}

}

// Define a struct to represent a client
type client struct {
	addr string
	conn net.Conn
}

// Define a struct to represent a bidder
type bidder struct {
	client *client
	bid int
}

// Maintain a map of connected clients
var clients = make(map[string]*client)

// Maintain a map of bids from clients
var bids = make(map[*client]int)

// Define constants for auction timing
const AuctionBaseWaitingTime = 5
const BidWaitingTime = 5

// Function to host the auction
func hostAuction() {
	for {
		if len(clients) == 0 {
			continue
	}
	<-time.After(time.Second * time.Duration(AuctionBaseWaitingTime+rand.Intn(1)))
	fmt.Println("Broadcasting auctions: ")
    
	// Notify all clients that the auction is starting
	for _, c := range clients {
		fmt.Fprintf(c.conn, "Auction is starting! Place your bids now!\n", BidWaitingTime)
		go getInput(c)
	}
    // Wait for the bid submission time to elapse
    <- time.After(time.Second * BidWaitingTime)
	winner, winningBid := determineWinner(bids)

	// Notify the winner and all clients about the auction result
	fmt.Fprintf(winner.conn, "You won! \nPaying Price: %d", winningBid)
	for c := range bids {
		// inform all clients about the winner and their bid
		if c == winner {
			continue
		}
		fmt.Fprintf(c.conn, "You lost! Winner: %s, Winning Bid: %d", winner.addr, winningBid)
	}
	// Clear the bids map for the next auction round
	for k := range bids {
		delete (bids, k)
	}
}
}
// Function to determine the winner of the auction
func getInput(c *client) {
	err := c.conn.SetReadDeadline(time.Now().Add(time.Second * BidWaitingTime))
	if err != nil {
		fmt.Println("Error setting read deadline: ", err)
		return
	}
    // Read the bid input from the client
	b, _ := bufio.NewReader(c.conn).ReadString('\n')
	if err != nil {
		fmt.Println("Error reading input: ", err)
		return
	}
	// Skip empty bids
	if b == "" {
		return
	}
	// Convert the bid input to an integer
	bid, err := strconv.Atoi(strings.TrimSpace(b))
	if err != nil {
		fmt.Println("Error converting bid to integer: ", err)
		return
	}
	bids[c] = bid
}

// handleConnection function manages incoming client connections
func handleConnection(conn net.Conn) {
	// Create a new client with connection details
	c := client {
		addr: conn.RemoteAddr().String(),
		conn: conn,
	}
	
	// Store the new client in the clients map
	clients	[c.addr] = &c
	fmt.Println("New client connected: ", c.addr)
}
// determineWinner function identifies the client with the highest bid
func determineWinner(bids map[*client]int) (*client, int) {
	if len(bids) == 0 {
		return nil, 0
	}
	// Create a slice to hold bidders for sorting
	bs := make([]bidder, 0, len(bids))
	for c, bid := range bids {
		bs = append(bs, bidder{client: c, bid: bid})
	}
    // Return the single bidder if there's only one
	if len(bs) == 1 {
		return bs[0].client, bs[0].bid
	}
    // Sort the bidders slice in descending order based on bid amounts
	sort.Slice(bs, func(i, j int) bool {
		return bs[i].bid > bs[j].bid
	})
    // Return the client with the highest bid and the winning bid amount
    return bs[0].client, bs[1].bid
}
