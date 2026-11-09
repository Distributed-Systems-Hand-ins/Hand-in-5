package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "auction/proto" // generated from proto/auction.proto (see go_package)
)

// callTimeout is the per-node time limit for one call. It must be longer than
// the "known time limit" of the network, because a call to a backup is
// forwarded to the primary, which in turn replicates to the other backups
// before it answers.
const callTimeout = 5 * time.Second

// auctionClient holds one connection per known node and remembers which node
// answered last, so we don't retry a dead node first on every request.
type auctionClient struct {
	id      string
	addrs   []string
	conns   []*grpc.ClientConn
	stubs   []pb.AuctionClient
	current int
}

func newAuctionClient(id string, addrs []string) (*auctionClient, error) {
	c := &auctionClient{id: id, addrs: addrs}
	for _, addr := range addrs {
		// NewClient does not connect yet: a dead node only shows up when we
		// make an RPC, which is exactly where call() handles it.
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			c.close()
			return nil, fmt.Errorf("creating client for %s: %w", addr, err)
		}
		c.conns = append(c.conns, conn)
		c.stubs = append(c.stubs, pb.NewAuctionClient(conn))
	}
	return c, nil
}

func (c *auctionClient) close() {
	for _, conn := range c.conns {
		conn.Close()
	}
}

// call runs do against the current node and fails over to the next node when
// the node is unreachable or too slow (Unavailable / DeadlineExceeded).
// Any other error is a real answer from a live node and is returned as is.
// It returns the address of the node that answered.
//
// Retrying the same request on another node is only safe because the server
// treats a repeated bid (same bidder, same amount) as success, not as a fail.
func (c *auctionClient) call(do func(ctx context.Context, stub pb.AuctionClient) error) (string, error) {
	n := len(c.stubs)
	for i := 0; i < n; i++ {
		idx := (c.current + i) % n
		ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
		err := do(ctx, c.stubs[idx])
		cancel()
		if err == nil {
			c.current = idx
			return c.addrs[idx], nil
		}
		switch status.Code(err) {
		case codes.Unavailable, codes.DeadlineExceeded:
			log.Printf("node %s did not answer (%v), trying next node", c.addrs[idx], status.Code(err))
		default:
			return c.addrs[idx], err
		}
	}
	return "", errors.New("all nodes unreachable")
}

func (c *auctionClient) bid(amount int32) {
	var resp *pb.BidResponse
	node, err := c.call(func(ctx context.Context, stub pb.AuctionClient) error {
		var err error
		resp, err = stub.Bid(ctx, &pb.BidRequest{Bidder: c.id, Amount: amount})
		return err
	})
	if err != nil {
		fmt.Printf("bid could not be completed: %v\n", err)
		return
	}
	switch resp.GetOutcome() {
	case pb.Outcome_SUCCESS:
		fmt.Printf("[%s] SUCCESS: your bid of %d was accepted\n", node, amount)
	case pb.Outcome_FAIL:
		fmt.Printf("[%s] FAIL: your bid of %d was too low\n", node, amount)
	case pb.Outcome_EXCEPTION:
		fmt.Printf("[%s] EXCEPTION: bid rejected (auction over or invalid bid)\n", node)
	default:
		fmt.Printf("[%s] unexpected outcome: %v\n", node, resp.GetOutcome())
	}
}

func (c *auctionClient) result() {
	var resp *pb.ResultResponse
	node, err := c.call(func(ctx context.Context, stub pb.AuctionClient) error {
		var err error
		resp, err = stub.Result(ctx, &pb.ResultRequest{})
		return err
	})
	if err != nil {
		fmt.Printf("result could not be fetched: %v\n", err)
		return
	}
	switch {
	case resp.GetOver() && resp.GetWinner() == "":
		fmt.Printf("[%s] Auction is over. There were no bids.\n", node)
	case resp.GetOver():
		fmt.Printf("[%s] Auction is over. Winner: %s with a bid of %d\n", node, resp.GetWinner(), resp.GetHighestBid())
	default:
		fmt.Printf("[%s] Auction is running. Highest bid so far: %d\n", node, resp.GetHighestBid())
	}
}

// repl reads commands from stdin until "quit" or end of input.
func (c *auctionClient) repl() {
	fmt.Printf("Bidder %q connected to %d node(s). Type 'help' for commands.\n", c.id, len(c.addrs))
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			return
		}
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		switch strings.ToLower(fields[0]) {
		case "bid":
			if len(fields) != 2 {
				fmt.Println("usage: bid <amount>")
				continue
			}
			amount, err := strconv.ParseInt(fields[1], 10, 32)
			if err != nil {
				fmt.Printf("%q is not a valid amount\n", fields[1])
				continue
			}
			c.bid(int32(amount))
		case "result":
			c.result()
		case "help":
			fmt.Println("commands: bid <amount> | result | quit")
		case "quit", "exit":
			return
		default:
			fmt.Printf("unknown command %q (try 'help')\n", fields[0])
		}
	}
}

func main() {
	id := flag.String("id", "", "bidder id (required)")
	nodes := flag.String("nodes", "localhost:5001,localhost:5002,localhost:5003",
		"comma-separated addresses of all known nodes")
	flag.Parse()

	if *id == "" {
		fmt.Fprintln(os.Stderr, "missing required flag -id")
		flag.Usage()
		os.Exit(2)
	}

	var addrs []string
	for _, a := range strings.Split(*nodes, ",") {
		if a = strings.TrimSpace(a); a != "" {
			addrs = append(addrs, a)
		}
	}
	if len(addrs) == 0 {
		fmt.Fprintln(os.Stderr, "no node addresses given in -nodes")
		os.Exit(2)
	}

	c, err := newAuctionClient(*id, addrs)
	if err != nil {
		log.Fatal(err)
	}
	defer c.close()
	c.repl()
}