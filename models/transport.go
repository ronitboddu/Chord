package models

import (
	"Test2/pb"
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ChordServer struct {
	pb.UnimplementedKeyServiceServer
}

type Transport struct {
	Node        *pb.Node
	Finger      *Fingers
	LogFileName string
	Quit        chan bool
}

func (t *Transport) Register() *pb.NodeIp {
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	conn, err := grpc.DialContext(ctx, "127.0.0.254:50001", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	c := pb.NewKeyServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	random_ip, err := c.RPCRegisterNode(ctx, &pb.NodeIp{Id: t.Node.Id, IpAddr: t.Node.CurrIp.IpAddr, Port: t.Node.CurrIp.Port})

	if err != nil {
		panic(err)
	}
	return random_ip
}

/*Get successor from a node*/
func (t *Transport) GetSuccessor(node *pb.NodeIp) (*pb.NodeIp, error) {
	// fmt.Println("in GetSuccessor")
	node_ip, node_port := node.IpAddr, node.Port
	// fmt.Println(node_ip, node_port)
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	conn, err := grpc.DialContext(ctx, node_ip+":"+node_port, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, nil
	}
	defer conn.Close()

	c := pb.NewKeyServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	res, _ := c.RPCGetSuccessor(ctx, &pb.Empty{})
	// fmt.Println(res.Id, res.IpAddr, res.Port)
	// fmt.Println("out GetSuccessor")
	return res, nil
}

/*Get predecessor from a node*/
func (t *Transport) GetPredecessor(node *pb.NodeIp) (*pb.NodeIp, error) {
	// fmt.Println("in GetPredecessor")
	node_ip, node_port := node.IpAddr, node.Port
	// fmt.Println(node_ip, node_port)
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	conn, err := grpc.DialContext(ctx, node_ip+":"+node_port, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return &pb.NodeIp{}, nil
	}
	defer conn.Close()

	c := pb.NewKeyServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	res, _ := c.RPCGetPredecessor(ctx, &pb.Empty{})
	// fmt.Println("out GetPredecessor")
	return res, nil
}

func (t *Transport) FindPredecessor(id int32, lookupLogFlag bool) *pb.NodeIp {
	// fmt.Println("in FindPredecessor")
	// t.Finger.Mu.Lock()
	// defer t.Finger.Mu.Unlock()
	curr_node := pb.NodeIp{Id: t.Node.Id, IpAddr: t.Node.CurrIp.IpAddr, Port: t.Node.CurrIp.Port}
	for {
		if curr_node.IpAddr != "" {
			break
		}
		curr_node = pb.NodeIp{Id: t.Node.Id, IpAddr: t.Node.CurrIp.IpAddr, Port: t.Node.CurrIp.Port}
	}
	succ_node := &pb.NodeIp{Id: t.Node.SuccIp.Id, IpAddr: t.Node.SuccIp.IpAddr, Port: t.Node.SuccIp.Port}
	curr_id := curr_node.Id
	succ_id := succ_node.Id
	if lookupLogFlag {
		WriteLookupLog(fmt.Sprint(curr_id)+" -> ", t.LogFileName)
	}
	for !(IsBetweenRightIncl(id, curr_id, succ_id)) {
		curr_node = *t.Closest_preceding_finger(&curr_node, id)
		succ_node, _ = t.GetSuccessor(&curr_node)
		if succ_node == nil {
			continue
		}
		curr_id = curr_node.Id
		succ_id = succ_node.Id
		if lookupLogFlag {
			WriteLookupLog(fmt.Sprint(curr_id)+" -> ", "lookup_log.txt")
		}
	}
	if lookupLogFlag {
		WriteLookupLog("Key Present at Node "+fmt.Sprint(succ_id)+"\n", "lookup_log.txt")
	}

	// t.Finger.Mu.Unlock()
	// fmt.Println("out FindPredecessor")
	// fmt.Println(curr_node.Id, curr_node.IpAddr, curr_node.Port)
	return &curr_node
}

/* inititalize a node, by creating finger table and by setting successor and predecessor*/
func (t *Transport) InitializeNode() {
	t.Finger.CreateFingerTable()
	random_node := t.Register()

	// if there are nodes already present in the chord ring
	if random_node.IpAddr != "" {
		for {
			t.Finger.Mu.Lock()
			succ_node := t.FindNodeSuccessor(t.Node.Id, random_node)
			t.Finger.Mu.Unlock()
			if succ_node != nil && succ_node.IpAddr != "" {
				SetNodeSuccessor(succ_node, t)
				t.Notify(succ_node)
				t.GetKeys(succ_node)
				break
			}
			time.Sleep(2 * time.Second)
		}
	}
	PrintNode(t)
	t.Finger.PrintFingerTable()
}

func (t *Transport) GetKeys(succNode *pb.NodeIp) {
	node_ip, node_port := succNode.IpAddr, succNode.Port
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, node_ip+":"+node_port, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return
	}
	defer conn.Close()

	c := pb.NewKeyServiceClient(conn)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	// c.RPCNotify(ctx, t.Node.CurrIp)
	keyMap, _ := c.RPCGetKeys(ctx, &pb.Key{Key: t.Node.Id})
	if keyMap.HashTable != nil {
		t.Node.HashTable = keyMap.HashTable
	}
}

func (t *Transport) Stabilize() {
	// fmt.Println("in Stabilize")
	// t.Finger.Mu.Lock()
	// defer t.Finger.Mu.Unlock()
	if t.Node.PredIp != nil && t.Node.SuccIp == nil {
		t.Node.SuccIp = t.Node.PredIp
	}
	if t.Node.SuccIp != nil {
		x, _ := t.GetPredecessor(t.Node.SuccIp)
		if x.IpAddr != "" && IsBetween(x.Id, t.Node.Id, t.Node.SuccIp.Id) {
			SetNodeSuccessor(x, t)
		}
		t.Notify(t.Node.SuccIp)
	}
	PrintNode(t)
	// fmt.Println("out Stabilize")
}

func (t *Transport) Notify(succNode *pb.NodeIp) {
	// fmt.Println("in Notify")
	node_ip, node_port := succNode.IpAddr, succNode.Port
	// fmt.Println(t.Node.CurrIp.IpAddr, node_ip)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, node_ip+":"+node_port, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return
	}
	defer conn.Close()

	c := pb.NewKeyServiceClient(conn)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	c.RPCNotify(ctx, t.Node.CurrIp)
	// fmt.Println("out Notify")
}

/*calls chord_node's RPC to find successor of the id passed*/
func (t *Transport) FindNodeSuccessor(id int32, chord_node *pb.NodeIp) *pb.NodeIp {
	// fmt.Println("in FindNodeSuccessor")
	node_ip, node_port := chord_node.IpAddr, chord_node.Port
	// fmt.Println(node_ip, node_port)
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	conn, err := grpc.DialContext(ctx, node_ip+":"+node_port, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		fmt.Println(node_ip)
		// panic(err)
		return nil
	}
	defer conn.Close()

	c := pb.NewKeyServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	res, _ := c.RPCFindSuccessor(ctx, &pb.Id{Id: id})
	// fmt.Println(res.Id, res.IpAddr, res.Port)
	// fmt.Println("out FindNodeSuccessor")
	return res
}

/*call RPC for getting closest preceding finger of a node*/
func (t *Transport) Closest_preceding_finger(node *pb.NodeIp, id int32) *pb.NodeIp {
	// fmt.Println("in Closest_preceding_finger")
	node_ip, node_port := node.IpAddr, node.Port
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	conn, err := grpc.DialContext(ctx, node_ip+":"+node_port, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		fmt.Println(node_ip)
		panic(err)
	}
	defer conn.Close()

	c := pb.NewKeyServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	res, _ := c.RPCClosestPrecedingFinger(ctx, &pb.IdM{Id: id, M: t.Finger.M})
	// fmt.Println(res.Id, res.IpAddr, res.Port)
	// fmt.Println("out Closest_preceding_finger")
	return res
}

func (t *Transport) NotifySucc(succNode *pb.NodeIp, predNode *pb.NodeIp) {
	node_ip, node_port := succNode.IpAddr, succNode.Port
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	conn, err := grpc.DialContext(ctx, node_ip+":"+node_port, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	c := pb.NewKeyServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	c.RPCNotifySucc(ctx, predNode)
}

func (t *Transport) NotifyPred(succNode *pb.NodeIp, predNode *pb.NodeIp) {
	node_ip, node_port := predNode.IpAddr, predNode.Port
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	conn, err := grpc.DialContext(ctx, node_ip+":"+node_port, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	c := pb.NewKeyServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	c.RPCNotifyPred(ctx, succNode)
}

func (t *Transport) RemoveNode() {
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	conn, err := grpc.DialContext(ctx, "127.0.0.254:50001", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	c := pb.NewKeyServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	_, err = c.RPCRemoveNode(ctx, t.Node.CurrIp)

	if err != nil {
		panic(err)
	}
}

func (t *Transport) TransferKeys() {
	if t.Node.SuccIp != nil {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := grpc.DialContext(ctx, t.Node.SuccIp.IpAddr+":"+t.Node.SuccIp.Port, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err != nil {
			panic(err)
		}
		defer conn.Close()

		c := pb.NewKeyServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		_, err = c.RPCTransferKeys(ctx, &pb.KeyMap{HashTable: t.Node.HashTable})

		if err != nil {
			panic(err)
		}
	}
}

func (t *Transport) PeriodicFunc(ticker *time.Ticker) {
	for {
		select {
		case <-t.Quit:
			ticker.Stop()
			return
		case <-ticker.C:
			t.Stabilize()
			t.Finger.FixFingers(t)
			PrintKeys(t)

		}
	}
}
