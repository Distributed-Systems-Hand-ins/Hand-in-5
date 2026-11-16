package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	pb "auction/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var addresses = []string{"localhost:5001", "localhost:5002", "localhost:5003"}

const clientTimeout = 10 * time.Second

func main() {
	name := flag.String("name", "", "your bidder name")
	start := flag.Int("node", 1, "which node to talk to first (1-3)")
	flag.Parse()
	if *name == "" {
		fmt.Println("usage: go run ./client -name alice [-node 2]")
		return
	}

	var nodes []pb.AuctionClient
	for _, addr := range addresses {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			panic(err)
		}
		nodes = append(nodes, pb.NewAuctionClient(conn))
	}
	current := *start - 1 // index into nodes

	call := func(f func(c pb.AuctionClient, ctx context.Context) error) bool {
		for attempt := 0; attempt < len(nodes); attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), clientTimeout) 
			err := f(nodes[current], ctx)
			cancel()
			if err == nil {
				return true
			}
			fmt.Printf(" (node %d did not answer, trying the next node)\n", current+1)
			current = (current + 1) % len(nodes)
		}
		return false
	}
	

	requestID := time.Now().UnixNano() // unique and increasing, even if you restart the client
	fmt.Println("Commands: bid <amount> | result | quit")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "bid":
			if len(fields) != 2 {
				fmt.Println("usage: bid <amount>")
				continue
			}
			amount, err := strconv.ParseInt(fields[1], 10, 32)
			if err != nil {
				fmt.Println("amount must be a number")
				continue
			}
			requestID++
			req := &pb.BidRequest{Bidder: *name, Amount: int32(amount), RequestId: requestID}
			ok := call(func(c pb.AuctionClient, ctx context.Context) error {
				reply, err := c.Bid(ctx, req)
				if err == nil {
					fmt.Printf("%v: %s\n", reply.Outcome, reply.Message)
				}
				return err
			})
			if !ok {
				fmt.Println("EXCEPTION: no node answered")
			}
		case "result":
			ok := call(func(c pb.AuctionClient, ctx context.Context) error {
				r, err := c.Result(ctx, &pb.ResultRequest{})
				if err == nil {
					switch {
					case !r.Started:
						fmt.Println("No bids yet.")
					case r.Over:
						fmt.Printf("Auction over. Winner: %s with %d\n", r.HighestBidder, r.HighestBid)
					default:
						fmt.Printf("Highest bid: %s with %d (%ds left)\n", r.HighestBidder, r.HighestBid, r.SecondsLeft)
					}
				}
				return err
			})
			if !ok {
				fmt.Println("EXCEPTION: no node answered")
			}
		case "quit":
			return
		default:
			fmt.Println("Commands: bid <amount> | result | quit")
		}
	}
    
}