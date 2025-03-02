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
	"time"
)

<<<<<<< HEAD
=======
import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"strconv"
	"time"
)

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
>>>>>>> 6809e0e (Commited the basic code)
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
<<<<<<< HEAD
	instances  map[int]*Instance
	done       []int
	roundNum   int
	majority   int
=======

	// Your data here.
	instances map[int]*Instance
	done      []int
	roundNum  int
	majority  int
>>>>>>> 6809e0e (Commited the basic code)
}

type Instance struct {
	status         Fate
<<<<<<< HEAD
	acceptedN      float64
	acceptedV      interface{}
	highestPrepare float64
	decidedV       interface{}
=======
	acceptedN      int
	acceptedV      interface{}
	highestPrepare int
	decidedV       interface{}
	proposing      bool
>>>>>>> 6809e0e (Commited the basic code)
}

type PrepareArgs struct {
	Seq int
<<<<<<< HEAD
	N   float64
=======
	N   int
>>>>>>> 6809e0e (Commited the basic code)
}

type PrepareReply struct {
	Ok          bool
	Seq         int
<<<<<<< HEAD
	N           float64
	AcceptedN   float64
=======
	N           int
	AcceptedN   int
>>>>>>> 6809e0e (Commited the basic code)
	AcceptedVal interface{}
}

type AcceptArgs struct {
	Seq int
<<<<<<< HEAD
	N   float64
=======
	N   int
>>>>>>> 6809e0e (Commited the basic code)
	V   interface{}
}

type AcceptReply struct {
	Seq int
<<<<<<< HEAD
	N   float64
=======
	N   int
>>>>>>> 6809e0e (Commited the basic code)
	Ok  bool
}

type DecidedArgs struct {
<<<<<<< HEAD
	Seq     int
	Value   interface{}
	propNum float64
	Me      int
	Done    int
=======
	Seq int
	V   interface{}
>>>>>>> 6809e0e (Commited the basic code)
}

type DecidedReply struct {
	Ok bool
}

func (px *Paxos) getInstance(seq int) *Instance {
<<<<<<< HEAD
	// px.mu.Lock()
	// defer px.mu.Unlock()
	ins, exists := px.instances[seq]
	if !exists {
		ins = &Instance{acceptedN: -100, highestPrepare: -100, acceptedV: nil, status: Pending, decidedV: nil}
		px.instances[seq] = ins
	}
	return ins
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	ins := px.getInstance(args.Seq)
	// fmt.Println("PREPARE", args.N, ins.acceptedN)
	if args.N > ins.highestPrepare {
		reply.Ok = true
		reply.N = args.N
		reply.AcceptedN = ins.acceptedN
		reply.AcceptedVal = ins.acceptedV
		ins.highestPrepare = args.N
		ins.acceptedN = args.N
	} else {
		reply.Ok = false
	}
	return nil
}

func call(srv string, name string, args interface{}, reply interface{}) bool {
	// fmt.Println(srv, name, args, reply)
=======
	ins, exists := px.instances[seq]
	if !exists {
		ins = &Instance{}
		px.instances[seq] = ins
	}
	return ins
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	// px.mu.Lock()
	// defer px.mu.Unlock()
	ins := px.getInstance(args.Seq)
	fmt.Println(args.N, ins.acceptedN)
	if args.N > ins.acceptedN {
		ins.highestPrepare = args.N
		ins.acceptedN = args.N
		reply.Ok = true
		reply.N = args.N
		reply.AcceptedN = ins.acceptedN
		reply.AcceptedVal = ins.acceptedV
	} else {
		reply.Ok = false
	}
	return nil
}

func call(srv string, name string, args interface{}, reply interface{}) bool {
	fmt.Println(srv, name, args, reply)
>>>>>>> 6809e0e (Commited the basic code)
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			//fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}

<<<<<<< HEAD
func (px *Paxos) Propose(seq int, v interface{}) {
	// fmt.Println(seq, v, "PROPOSE")
	// ins := px.getInstance(seq)
	if seq < px.Min() {
		return
	}
	for {
=======
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Propose(seq int, v interface{}) {
	fmt.Println(seq, v, "PROPOSE")
	ins := px.getInstance(seq)
	for ins.status != Decided && !px.isdead() {
>>>>>>> 6809e0e (Commited the basic code)
		// Proposer's logic based on the given pseudocode
		if state, _ := px.Status(seq); state == Decided {
			break
		}
<<<<<<< HEAD
		// fmt.Println("IMt at start")
		n := px.chooseN()
		args := &PrepareArgs{Seq: seq, N: n}
		prepareReplies := make([]*PrepareReply, len(px.peers))
		promisedCount := 0
		highestN := -1.0
		// fmt.Println(v, "PROPOSED VAL")
		val := v
		for i := range px.peers {
			reply := &PrepareReply{}
=======
		fmt.Println("IMt at start")
		n := px.chooseN(seq)
		args := &PrepareArgs{Seq: seq, N: n}
		prepareReplies := make([]*PrepareReply, len(px.peers))
		reply := &PrepareReply{}
		promisedCount := 0
		highestN := 0
		val := v
		for i := range px.peers {
>>>>>>> 6809e0e (Commited the basic code)
			if i == px.me {
				px.Prepare(args, reply)
			} else {

				call(px.peers[i], "Paxos.Prepare", args, reply)
			}
			prepareReplies[i] = reply
			if reply.Ok {
<<<<<<< HEAD
				// fmt.Println(reply.AcceptedN, highestN, "MAINNNNNNNNN")
				if reply.AcceptedN > highestN && reply.AcceptedVal != nil {
					// fmt.Println(reply)
					highestN = reply.AcceptedN
					val = reply.AcceptedVal
				}
				promisedCount++
			}
		}
		// fmt.Println(val, n, "BEFORE ACCEPT")
		// fmt.Println(promisedCount)
		// fmt.Println(len(px.peers))
		if promisedCount >= len(px.peers)/2+1 {
=======
				promisedCount++
				fmt.Println(reply.AcceptedN, highestN)
				if reply.AcceptedN > highestN {
					highestN = reply.AcceptedN
					val = reply.AcceptedVal
				}
			}
		}
		fmt.Println(val, n, "BEFORE ACCEPT")
		fmt.Println(promisedCount)
		fmt.Println(len(px.peers))
		if promisedCount > len(px.peers)/2 {
>>>>>>> 6809e0e (Commited the basic code)
			acceptedCount := 0
			acceptArgs := &AcceptArgs{Seq: seq, N: n, V: val}
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
<<<<<<< HEAD
			if acceptedCount >= len(px.peers)/2+1 {
				decidedArgs := &DecidedArgs{Seq: seq, Value: val, propNum: n, Me: px.me, Done: px.done[px.me]}
=======
			if acceptedCount > len(px.peers)/2 {
				decidedArgs := &DecidedArgs{Seq: seq, V: val}

>>>>>>> 6809e0e (Commited the basic code)
				for i := range px.peers {
					decidedReply := &DecidedReply{}
					if i == px.me {
						px.Decided(decidedArgs, decidedReply)
					} else {
						call(px.peers[i], "Paxos.Decided", decidedArgs, decidedReply)
					}
				}
<<<<<<< HEAD
				time.Sleep(time.Duration(rand.Int63n(10)) * time.Millisecond)
				break
			}
		}
	}
}
func (px *Paxos) Start(seq int, v interface{}) {
=======
				// px.mu.Lock()
				break

			}
		}
		// time.Sleep(time.Duration(rand.Int63n(10)) * time.Millisecond)
		break
	}
}
func (px *Paxos) Start(seq int, v interface{}) {

>>>>>>> 6809e0e (Commited the basic code)
	go func() {
		if seq < px.Min() {
			return
		}
		px.Propose(seq, v)
	}()

}
func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
<<<<<<< HEAD
	px.mu.Lock()
	defer px.mu.Unlock()
	// println("Decided")
	ins := px.getInstance(args.Seq)
	ins.highestPrepare = args.propNum
	ins.acceptedN = args.propNum
	ins.acceptedV = args.Value
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
	if args.N >= ins.acceptedN {
		ins.acceptedV = args.V
		ins.acceptedN = args.N
		ins.highestPrepare = args.N
		reply.Ok = true
=======
	// px.mu.Lock()
	// defer px.mu.Unlock()
	ins := px.getInstance(args.Seq)
	ins.status = Decided
	ins.acceptedV = args.V
	return nil
}
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	// px.mu.Lock()
	// defer px.mu.Unlock()
	fmt.Println("ACCEPT")
	ins := px.getInstance(args.Seq)
	if args.N >= ins.acceptedN {
		ins.highestPrepare = args.N
		ins.acceptedN = args.N
		ins.acceptedV = args.V
		reply.Ok = true
		reply.N = args.N
>>>>>>> 6809e0e (Commited the basic code)
	} else {
		reply.Ok = false
	}
	return nil
}

<<<<<<< HEAD
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

func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()
	// fmt.Println("DONE", seq)
	if seq > px.done[px.me] {
		px.done[px.me] = seq
	}
	// min := px.Min()
	// px.ForgetMin(min)
=======
func (px *Paxos) chooseN(seq int) int {
	pnum := strconv.FormatInt(time.Now().UnixNano(), 10) + "#" + strconv.Itoa(px.me)
	num, err := strconv.Atoi(pnum)
	if err != nil {
		fmt.Println("Conversion Error:", err)
		return -1
	}
	return num
}

// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	// px.mu.Lock()
	// defer px.mu.Unlock()
	px.done[px.me] = seq
	min := px.Min()
	px.ForgetMin(min)
>>>>>>> 6809e0e (Commited the basic code)
}

func (px *Paxos) ForgetMin(min int) {
	// px.mu.Lock()
	// defer px.mu.Unlock()
<<<<<<< HEAD

}

func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()
=======
	for seq := range px.instances {
		if seq < min {
			delete(px.instances, seq)
		}
	}

}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	// px.mu.Lock()
	// defer px.mu.Unlock()
>>>>>>> 6809e0e (Commited the basic code)
	max := -1
	for seq := range px.instances {
		if seq > max {
			max = seq
		}
	}
	return max
}

<<<<<<< HEAD
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	min := px.done[0]
	// fmt.Println("BEFORE", px.done, px.instances)
=======
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
func (px *Paxos) Min() int {
	//px.mu.Lock()
	//defer px.mu.Unlock()
	min := px.done[0]
>>>>>>> 6809e0e (Commited the basic code)
	for _, doneVal := range px.done {
		if doneVal < min {
			min = doneVal
		}
	}
<<<<<<< HEAD
	for seq := range px.instances {
		if seq <= min {
			delete(px.instances, seq)
		}
	}
	// px.ForgetMin(min)

	// fmt.Println("AFTERRRRRRR", px.done, px.instances)

	return min + 1
}

func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// min := px.Min()
	// px.mu.Lock()
	// defer px.mu.Unlock()
	// fmt.Println(px.instances)

	// ins := px.getInstance(seq)

	// if seq < min {
	// 	return Forgotten, nil
	// }
	// if ins.status == Decided {
	// 	return Decided, ins.decidedV
	// } else {
	// 	return Pending, nil
	// }
	min := px.Min()

	px.mu.Lock()
	defer px.mu.Unlock()
=======
	return min + 1
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	min := px.Min()
	//px.mu.Lock()
	//defer px.mu.Unlock()
	ins := px.getInstance(seq)
>>>>>>> 6809e0e (Commited the basic code)

	if seq < min {
		return Forgotten, nil
	}
<<<<<<< HEAD

	if instance, ok := px.instances[seq]; ok {
		return instance.status, instance.acceptedV
=======
	if ins.status == Decided {
		return Decided, ins.decidedV
>>>>>>> 6809e0e (Commited the basic code)
	} else {
		return Pending, nil
	}
}

<<<<<<< HEAD
=======
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
>>>>>>> 6809e0e (Commited the basic code)
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

<<<<<<< HEAD
=======
// has this peer been asked to shut down?
>>>>>>> 6809e0e (Commited the basic code)
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

<<<<<<< HEAD
=======
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
>>>>>>> 6809e0e (Commited the basic code)
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
<<<<<<< HEAD
=======
	// Your initialization code here.
>>>>>>> 6809e0e (Commited the basic code)

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
