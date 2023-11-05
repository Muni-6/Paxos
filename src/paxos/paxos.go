package paxos

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
)

type Fate int

const (
	Decided Fate = iota + 1
	Pending
	Forgotten
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]
	instances  map[int]*Instance
	done       []int
	roundNum   int
	majority   int
}

type Instance struct {
	status          Fate
	acceptedPropNum float64
	acceptedValue   interface{}
	propNum         float64
	decidedV        interface{}
}

type PrepareArgs struct {
	Seq     int
	PropNum float64
}

type PrepareReply struct {
	Ok              bool
	Seq             int
	PropNum         float64
	AcceptedPropNum float64
	AcceptedVal     interface{}
}

type AcceptArgs struct {
	Seq     int
	PropNum float64
	Value   interface{}
}

type AcceptReply struct {
	Seq     int
	PropNum float64
	Ok      bool
}

type DecidedArgs struct {
	Seq     int
	Value   interface{}
	PropNum float64
	Me      int
	Done    int
}

type DecidedReply struct {
	Ok bool
}

func (px *Paxos) getInstance(seq int) *Instance {
	ins, exists := px.instances[seq]
	if !exists {
		ins = &Instance{acceptedPropNum: -100, propNum: -100, acceptedValue: nil, status: Pending, decidedV: nil}
		px.instances[seq] = ins
	}
	return ins
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	ins := px.getInstance(args.Seq)
	// fmt.Println("PREPARE", args.N, ins.acceptedN)
	if args.PropNum > ins.propNum {
		reply.Ok = true
		reply.PropNum = args.PropNum
		reply.AcceptedPropNum = px.instances[args.Seq].acceptedPropNum
		reply.AcceptedVal = px.instances[args.Seq].acceptedValue
		px.instances[args.Seq].propNum = args.PropNum
	} else {
		reply.Ok = false
	}
	return nil
}

func call(srv string, name string, args interface{}, reply interface{}) bool {
	// fmt.Println(srv, name, args, reply)
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			// fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}

func (px *Paxos) Propose(seq int, v interface{}) {
	// fmt.Println(seq, v, "PROPOSE")
	// ins := px.getInstance(seq)
	if seq < px.Min() {
		return
	}
	for {
		// Proposer's logic based on the given pseudocode
		if state, _ := px.Status(seq); state == Decided {
			break
		}
		// fmt.Println("IMt at start")
		n := px.chooseN()
		args := &PrepareArgs{Seq: seq, PropNum: n}
		prepareReplies := make([]*PrepareReply, len(px.peers))
		promisedCount := 0
		highestN := -1.0
		// fmt.Println(v, "PROPOSED VAL")
		val := v
		for i := range px.peers {
			reply := &PrepareReply{}
			if i == px.me {
				px.Prepare(args, reply)
			} else {

				call(px.peers[i], "Paxos.Prepare", args, reply)
			}
			prepareReplies[i] = reply
			if reply.Ok {
				// fmt.Println(reply.AcceptedN, highestN, "MAINNNNNNNNN")
				if reply.AcceptedPropNum > highestN && reply.AcceptedVal != nil {
					// fmt.Println(reply)
					highestN = reply.AcceptedPropNum
					val = reply.AcceptedVal
				}
				promisedCount++
			}
		}
		// fmt.Println(val, n, "BEFORE ACCEPT")
		// fmt.Println(promisedCount)
		// fmt.Println(len(px.peers))
		if promisedCount >= len(px.peers)/2+1 {
			acceptedCount := 0
			acceptArgs := &AcceptArgs{Seq: seq, PropNum: n, Value: val}
			for i := range px.peers {
				acceptReply := &AcceptReply{}
				if i == px.me {
					px.Accept(acceptArgs, acceptReply)
				} else {
					call(px.peers[i], "Paxos.Accept", acceptArgs, acceptReply)
				}
				if acceptReply.Ok {
					acceptedCount++
				}
			}
			if acceptedCount >= len(px.peers)/2+1 {
				decidedArgs := &DecidedArgs{Seq: seq, Value: val, PropNum: n, Me: px.me, Done: px.done[px.me]}
				for i := range px.peers {
					decidedReply := &DecidedReply{}
					if i == px.me {
						px.Decided(decidedArgs, decidedReply)
					} else {
						call(px.peers[i], "Paxos.Decided", decidedArgs, decidedReply)
					}
				}
			}
		}
	}
}

// start agreement on new instance
func (px *Paxos) Start(seq int, v interface{}) {
	go func() {
		if seq < px.Min() {
			return
		}
		px.Propose(seq, v)
	}()

}
func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	ins := px.getInstance(args.Seq)
	ins.propNum = args.PropNum
	ins.acceptedPropNum = args.PropNum
	ins.acceptedValue = args.Value
	ins.decidedV = args.Value
	ins.status = Decided
	px.done[args.Me] = args.Done
	// println("Decided Val = ")
	// println(ins.decidedV)
	return nil
}
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	// fmt.Println("ACCEPT")
	ins := px.getInstance(args.Seq)
	if args.PropNum >= ins.propNum {
		ins.acceptedValue = args.Value
		ins.acceptedPropNum = args.PropNum
		ins.propNum = args.PropNum
		reply.Ok = true
	} else {
		reply.Ok = false
	}
	return nil
}

func (px *Paxos) chooseN() float64 {
	px.roundNum++
	string_propNum := fmt.Sprintf("%d.%d", px.roundNum, px.me)
	// println(string_propNum)
	f, err := strconv.ParseFloat(string_propNum, 64)
	if err != nil {
		// fmt.Println("Error:", err)
		return -1
	} else {
		// fmt.Println("Float:", f)
		return f
	}
}

// ok to forget all instances <= seq
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()
	// fmt.Println("DONE", seq)
	if seq > px.done[px.me] {
		px.done[px.me] = seq
	}
}

// highest instance seq known, or -1
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	max := -1
	for seq := range px.instances {
		if seq > max {
			max = seq
		}
	}
	return max
}

// instances before this have been forgotten
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	min := px.done[0]
	// fmt.Println("BEFORE", px.done, px.instances)
	for _, doneVal := range px.done {
		if doneVal < min {
			min = doneVal
		}
	}
	for seq := range px.instances {
		if seq <= min && px.instances[seq].status == Decided {
			delete(px.instances, seq)
		}
	}
	// fmt.Println("AFTERRRRRRR", px.done, px.instances)
	return min + 1
}

// get info about an instance
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	min := px.Min()

	px.mu.Lock()
	defer px.mu.Unlock()

	if seq < min {
		return Forgotten, nil
	}

	if instance, ok := px.instances[seq]; ok {
		return instance.status, instance.acceptedValue
	} else {
		return Pending, nil
	}
}

func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	px.instances = make(map[int]*Instance)

	px.done = make([]int, len(peers))
	px.roundNum = 0
	for i := range px.done {
		px.done[i] = -1

	}
	px.majority = len(peers)/2 + 1

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}