package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"
)

   pb "auction/proto" // generated from proto/auction.proto (see go_package)
"google.golang.org/grpc"
"google.golang.org/grpc/credentials/insecure"
"google.golang.org/protobuf/proto"

var adresses = map[int]string{
	1: "localhost:5001",
	2: "localhost:5002",
	3: "localhost:5003",
}

const callTimeout = 1 * time.Second
const forwardTimeout = 3 * time.Second

var auctionDuration time.Duration

type Node struct {
	pb.UnimplementedAuctionServer
	id int
	peers map[int]pb.AuctionClient

	mu sync.Mutex
	state pb.AuctionState
	dead map[int]bool
}

func main() {
	id := flag.Int("id", 1, "node id (1-3)")
	flag.DurationVar(&auctionDuration, "duration", 100*time.Second, "auction duration")
	flag.Parse()

	file, err := os.Create(fmt.Sprintf("node%d.log", *id))
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(io.MultiWriter(os.Stdout, file))
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	n := &Node {
		id: *id,
		peers: map[int]pb.AuctionClient{},
		dead: map[int]bool{},
		state: pb.State{Bidders: map[string]*pb.BidderInfo{}},
	}

	for otherID, addr := range adresses {
		if otherID == *id {
			continue
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatal(err)
		}
		n.peers[otherID] = pb.NewAuctionClient(conn)
	}

	lis, err := net.Listen("tcp", adressess[n.id])
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterAuctionServer(server, n)
	log.Printf("Node %d listening on %s", n.id, adresses[n.id])
	log.Fatal(server.Serve(lis))

	func (n * Node) Bid(ctx context.Context, req *pb.BidRequest) (*pb.BidResponse, error) {
		return n.handleBid(req), nil
	}
    func (n *Node) Result(ctx context.Context, req *pb.ResultRequest) (*pb.ResultResponse, error) {
		return n.handleResult(req), nil
	}

	func (n *Node) handleBid(req *pb.BidRequest) *pb.BidReply {
		n.mu.Lock()
		defer n.mu.Unlock()
	}

	newState := proto.Clone(n.state).(*pb.State)
	reply := applyBid(newState, req, time.Now())
	newState.Seq++

	n.state = newState
	log.Printf("Bid %d from %s -> %v (seq %d)", req.Amount, req.Bidder, reply.Outcome, newState.Seq)
	return reply

	func applyBid(state *pb.State, req *pb.BidRequest, now time.Time) *pb.BidReply {
		var reply *pb.BidReply 
		switch {
		case s.EndUnixMs != 0 && now.UnixMilli() >= s.EndUnixMs:
			reply = &pb.BidReply{Outcome: pb.Outcome_EXCEPTION, Message: "the auction is over"}
		case req.Amount <= 0:
			reply = &pb.BidReply{Outcome: pb.Outcome_EXCEPTION, Message: "amount must be positive"}
		case req.Amount <= s.HighestBid:
			reply = &pb.BidReply{Outcome: pb.Outcome_FAIL, Message: fmt.Sprintf("bid must be higher than %d", s.HighestBid)}
		default:
			if s.EndUnixMs == 0 {
				s.EndUnixMs = now.Add(auctionDuration).UnixMilli()
			}
			s.HighestBid = req.Amount
			s.HighestBidder = req.Bidder
			reply = &pb.BidReply{Outcome: pb_Outcome_SUCCESS, Message: "you are the highest bidder"}
		}
		if s.Bidders == nil {
			s.Bidders = map[string]*pb.BidderInfo{}
		}
		s.Bidders[req.Bidder] = &pb.BidderInfo{LastRequestId: req.RequestId, LastReply: reply}
		return reply

}

func (n *Node) handleResult() *pb.ResultReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	s := n.state
	r := &pb.ResultReply{Started: s.EndUnixMs != 0, HighestBidder: s.HighestBidder, HighestBid: s.HighestBid}
	if r.Started {
		left := time.Until(time.UnixMilli(s.EndUnixMs))
		if left <= 0 {
			r.Over = true
		} else {
			r.SecondsLeft = int64(left.Seconds())
		}
	}
	return r
}
}
