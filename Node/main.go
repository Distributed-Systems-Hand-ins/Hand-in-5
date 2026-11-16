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

	pb "auction/proto" // generated from proto/auction.proto (see go_package)

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

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
	crashBeforeReply bool
}

func main() {
	id := flag.Int("id", 1, "node id (1-3)")
	flag.DurationVar(&auctionDuration, "duration", 100*time.Second, "auction duration")
	crash := flag.Bool("crash-before-reply", false, "TESTING: exit after replicating a bid, before replying")
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
		crashBeforeReply: *crash,
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
	}

	func (n * Node) Bid(ctx context.Context, req *pb.BidRequest) (*pb.BidResponse, error) {
		for {
			leader := n.whoLeads(req.Forwarded)
			if leader == n.id {
				return n.handleBird(req), nil
			}
			fwd := proto.Clone(req).(*pb.BidRequest)
			fwd.Forwarded = true
			cctx, cancel := context.WithTimeout(ctx, forwardTimeout)
			reply, err := n.peers[leader].Bid(cctx, fwd)
			cancel() 
			if err == nil {
				return reply, nil
			}
			n.markDead(leader, err)
		}
	}
    func (n *Node) Result(ctx context.Context, req *pb.ResultRequest) (*pb.ResultResponse, error) {
		for {
			leader := n.whoLeads(req.Forwarded)
			if leader == n.id {
				return n.handleResult(), nil
			}
			cctx, cancel := context.WithTimeout(ctx, forwardTimeout)
			reply, err := n.peers[leader].Result(cctx, &pb.ResultRequest{Forwarded: true})
			cancel()
			if err == nil {
				return reply, nil
			}
			n.markDead(leader, err)
		}
	}

	func (n *Node) handleBid(req *pb.BidRequest) *pb.BidReply {
		if info, ok := n.state.Bidders[req.Bidder]; ok {
			if req.RequestId <= info.LastRequestId {
				log.Printf("Duplicate request %d from %s -> resending the stored reply", req.RequestId, req.Bidder)
				return info.LastReply
			}
			if req.RequestId < info.LastRequestId {
				return &pb.BidReply{Outcome: pb.Outcome_EXCEPTION, Message: "old request"}
		}
	}
		n.mu.Lock()
	    defer n.mu.Unlock()
		n.replicate(newState)
		n.state = newState
	
	newState := proto.Clone(n.state).(*pb.State)
	reply := applyBid(newState, req, time.Now())
	newState.Seq++

	n.state = newState
	log.Printf("Bid %d from %s -> %v (seq %d)", req.Amount, req.Bidder, reply.Outcome, newState.Seq)
	if n.crashBeforeReply {
		log.Printf("TEST: crashing after replication, before replying")
		os.Exit(1)
	}
	return reply
	}

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

func (n *Node) whoLeads(forwarded bool) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	if forwarded {
		for id := range addresses {
			if id < n.id && !n.dead[id] {
				n.dead[id] = true
				log.Printf("Learned from a forwarded request that node %d is dead", id)
			}
		}
	}
	return n.leader()
}
func (n *Node) markDead(id int, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.dead[id] {
		log.Printf("Node %d did not answer (%v), marking it as dead", id, err)
		n.dead[id] = true
	}
}

func (n *Node) leader() int {
	for id := 1; id <= len(addresses); id++ {
		if id == n.id || !n.dead[id] {
			return id 
	}
}
return n.id
}
func (n *Node) replicate(s *pb.State) {
	for id := 1; id <= len(addresses); id++ {
		if id == n.id || n.dead[id] {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	_, err := n.peers[id].Replicate(ctx, s)
	cancel()
	if err != nil {
		log.Printf("Backup %d did not ack (%v), marking it as dead", id, err)
		n.dead[id] = true
	}
	}
}	
func (n *Node) Replicate(ctx context.Context, s *pb.State) (*pb.ReplicateReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if s.Seq <= n.state.Seq {
		n.state = s 
		log.Printf("Backup: now at seq %d, highest %s:%d", s.Seq, s.HighestBidder, s.HighestBid)
}
return &pb.ack{}, nil
}
}

