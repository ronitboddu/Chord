package central_server

import (
	"Test2/models"
	"Test2/pb"
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ChordServer struct {
	pb.UnimplementedKeyServiceServer
}

var registry = make(map[int32]*pb.NodeIp)
var list_key []int32
var mu sync.Mutex
var logFileName = "central_server.txt"

func (c *ChordServer) RPCRegisterNode(ctx context.Context, node *pb.NodeIp) (*pb.NodeIp, error) {
	// fmt.Println(node.IpAddr, node.Port)
	// fmt.Println(list_key)
	mu.Lock()
	list_key = append(list_key, node.Id)
	models.WriteLookupLog(node.IpAddr+"\n", logFileName)
	res_node, err := &pb.NodeIp{}, error(nil)
	if len(registry) == 0 {
		res_node, err = nil, nil
	} else {
		res_node, err = getRandomNode(), nil
	}
	registry[node.Id] = node
	mu.Unlock()
	return res_node, err
}

func (c *ChordServer) RPCRemoveNode(ctx context.Context, node *pb.NodeIp) (*pb.Empty, error) {
	delete(registry, node.Id)
	Delete(node.Id)
	fmt.Println("Removed node " + fmt.Sprint(node.Id))
	return nil, nil
}

func (c *ChordServer) RPCLookup(ctx context.Context, K *pb.Key) (*pb.NodeIp, error) {
	res_node := getRandomNode()
	node_ip, node_port := res_node.IpAddr, res_node.Port
	// fmt.Println("Random node: "+node_ip, node_port)
	// fmt.Println(list_key)
	models.WriteLookupLog(fmt.Sprintf("Random node: %s\n", node_ip), "lookup_log.txt")
	// fmt.Println("here")
	// node_ip, node_port := "127.0.0.3", "50001"
	// models.WriteLookupLog(fmt.Sprintf("Lookup for key %d triggred at Node %s\n\n", K.Key, node_ip), logFileName)
	ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
	conn, err := grpc.DialContext(ctx, node_ip+":"+node_port, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	co := pb.NewKeyServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	res, _ := co.RPCFindSuccessor(ctx, &pb.Id{Id: K.Key, LookupLogFlag: true})
	// models.WriteLookupLog(fmt.Sprintf("%d  %s  %s\n", res.Id, res.IpAddr, res.Port), logFileName)
	return res, nil
}

func getRandomNode() *pb.NodeIp {
	// for k, _ := range registry {
	// 	return registry[k]
	// }
	// return nil
	if len(registry) == 0 {
		return nil
	} else {
		random_index := rand.IntN(len(list_key) - 1)
		// fmt.Println("Random index: ", random_index)
		random_key := list_key[random_index]
		// fmt.Println("Random index is ", random_index)
		// fmt.Println("Registry is ", registry)
		// fmt.Println("Random key is ", registry[random_key])
		return registry[random_key]
	}

}

func Delete(elem int32) {
	for i := 0; i < len(list_key); i++ {
		if list_key[i] == elem {
			list_key = append(list_key[:i], list_key[i+1:]...)
			break
		}
	}
}

func GRPCListen(wg *sync.WaitGroup) {
	models.ClearLog(logFileName)
	lis, err := net.Listen("tcp", "127.0.0.254:50001")
	if err != nil {
		panic(err)
	}

	s := grpc.NewServer()
	pb.RegisterKeyServiceServer(s, &ChordServer{})
	models.WriteLookupLog(fmt.Sprintf("Central gRPC server started on port %s\n", "50001"), logFileName)

	if err := s.Serve(lis); err != nil {
		panic(fmt.Sprintf("Failed to listen for gRPC: %v", err))
	}
	defer wg.Done()
}

func StartCentralServer(parent_wg *sync.WaitGroup) {
	defer parent_wg.Done()
	var wg sync.WaitGroup
	wg.Add(1)
	go GRPCListen(&wg)
	wg.Wait()
}
