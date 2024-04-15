package listener

import (
	"Test2/models"
	"Test2/pb"
	"context"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"
)

type ChordServer struct {
	pb.UnimplementedKeyServiceServer
	T      *models.Transport
	server *grpc.Server
}

func (c *ChordServer) RPCFindSuccessor(ctx context.Context, id *pb.Id) (*pb.NodeIp, error) {
	// t.Finger.Mu.Lock()
	// defer t.Finger.Mu.Unlock()
	if c.T.Node.SuccIp == nil || c.T.Node.SuccIp.IpAddr == "" {
		return &pb.NodeIp{Id: c.T.Node.Id, IpAddr: c.T.Node.CurrIp.IpAddr, Port: c.T.Node.CurrIp.Port}, nil
	}
	pred := c.T.FindPredecessor(id.Id, id.LookupLogFlag)
	succ, _ := c.T.GetSuccessor(&pb.NodeIp{Id: pred.Id, IpAddr: pred.IpAddr, Port: pred.Port})
	return succ, nil
}

func (c *ChordServer) RPCClosestPrecedingFinger(ctx context.Context, id_m *pb.IdM) (*pb.NodeIp, error) {
	// t.Finger.Mu.Lock()
	// defer t.Finger.Mu.Unlock()
	ft := c.T.Finger.FingerTable
	id, _ := id_m.Id, id_m.M
	max, min := int32(0), int32(2147483647)
	max_node, min_node := &pb.NodeIp{}, &pb.NodeIp{}
	for i := c.T.Finger.M - 1; i >= 0; i-- {
		finger := c.T.Finger.GetFingerKey(c.T.Node.Id, i)
		if ft[finger] == nil {
			continue
		}
		finger_succ_id := ft[finger].Id
		if finger_succ_id > max {
			max = finger_succ_id
			max_node = ft[finger]
		}
		if id > finger_succ_id && (id-finger_succ_id) < min {
			min = (id - finger_succ_id)
			min_node = ft[finger]
		}
	}
	if min_node.IpAddr == "" {
		return max_node, nil
	}
	return min_node, nil
}

func (c *ChordServer) RPCGetSuccessor(ctx context.Context, emp *pb.Empty) (*pb.NodeIp, error) {
	if c.T.Node.SuccIp != nil {
		return c.T.Node.SuccIp, nil
	}
	return nil, nil
}

func (c *ChordServer) RPCGetPredecessor(ctx context.Context, emp *pb.Empty) (*pb.NodeIp, error) {
	if c.T.Node.PredIp != nil {
		return c.T.Node.PredIp, nil
	}
	return nil, nil
}

func (c *ChordServer) RPCDepart(ctx context.Context, emp *pb.Empty) (*pb.Empty, error) {
	c.T.RemoveNode()
	c.T.TransferKeys()
	if c.T.Node.PredIp != nil {
		c.T.NotifyPred(c.T.Node.SuccIp, c.T.Node.PredIp)
	}
	if c.T.Node.SuccIp != nil {
		c.T.NotifySucc(c.T.Node.SuccIp, c.T.Node.PredIp)
	}

	// defer os.Exit(0)
	c.T.Quit <- true
	c.server.Stop()
	fmt.Println("Server stopped for ", c.T.Node.CurrIp.IpAddr)
	return nil, nil
}

func (c *ChordServer) RPCTransferKeys(ctx context.Context, keyMap *pb.KeyMap) (*pb.Empty, error) {
	for k, v := range keyMap.HashTable {
		c.T.Node.HashTable[k] = v
	}
	return nil, nil
}

func (c *ChordServer) RPCNotifySucc(ctx context.Context, predNode *pb.NodeIp) (*pb.Empty, error) {
	if c.T.Node.CurrIp.IpAddr == predNode.IpAddr {
		c.T.Node.PredIp = nil
	} else {
		c.T.Node.PredIp = predNode
	}
	return nil, nil
}

func (c *ChordServer) RPCNotifyPred(ctx context.Context, succNode *pb.NodeIp) (*pb.Empty, error) {
	if c.T.Node.CurrIp.IpAddr == succNode.IpAddr {
		c.T.Node.SuccIp = nil
	} else {
		models.SetNodeSuccessor(succNode, c.T)
		c.T.Notify(succNode)
	}
	return nil, nil
}

func (c *ChordServer) RPCGetKeys(ctx context.Context, key *pb.Key) (*pb.KeyMap, error) {
	hashTable := make(map[int32]string)
	node_id := key.Key
	for k, v := range c.T.Node.HashTable {
		if c.T.Node.PredIp == nil {
			if models.IsBetweenRightIncl(k, c.T.Node.CurrIp.Id, node_id) {
				hashTable[k] = v
				delete(c.T.Node.HashTable, k)
			}
		} else {
			if models.IsBetweenRightIncl(k, c.T.Node.PredIp.Id, node_id) {
				hashTable[k] = v
				delete(c.T.Node.HashTable, k)
			}
		}
	}
	return &pb.KeyMap{HashTable: hashTable}, nil
}

func (c *ChordServer) RPCNotify(ctx context.Context, node *pb.NodeIp) (*pb.Empty, error) {
	// fmt.Println("in RPCNotify")
	c.T.Finger.Mu.Lock()
	// fmt.Println(c.T.Node.CurrIp.IpAddr, node.IpAddr)
	defer c.T.Finger.Mu.Unlock()
	if c.T.Node.PredIp == nil || models.IsBetween(node.Id, c.T.Node.PredIp.Id, c.T.Node.CurrIp.Id) {
		c.T.Node.PredIp = node
	}
	// fmt.Println("out RPCNotify")
	return &pb.Empty{}, nil
}

func (c *ChordServer) RPCAddkey(ctx context.Context, key *pb.Key) (*pb.Empty, error) {
	c.T.Node.HashTable[key.Key] = fmt.Sprint(key.Key)
	return nil, nil
}

func GRPCListen(wg *sync.WaitGroup, transport *models.Transport) {
	// t = transport
	// hashTable = t.Node.HashTable
	// fmt.Println(transport.Node.CurrIp.IpAddr, "In GrpcListen")
	// fmt.Println(t.Node.CurrIp.IpAddr)
	lis, err := net.Listen("tcp", transport.Node.CurrIp.IpAddr+":"+transport.Node.CurrIp.Port)
	if err != nil {
		panic(err)
	}

	s := grpc.NewServer()
	pb.RegisterKeyServiceServer(s, &ChordServer{T: transport, server: s})
	models.WriteLookupLog(fmt.Sprintf("gRPC server started on port %s\n", transport.Node.CurrIp.Port), transport.LogFileName)

	if err := s.Serve(lis); err != nil {
		panic(fmt.Sprintf("Failed to listen for gRPC: %v", err))
	}
	// fmt.Println("out GrpcListen")
	defer wg.Done()
}
