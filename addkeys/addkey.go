package main

import (
	"Test2/models"
	"Test2/pb"
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func Addkey(key int32) {
	key_node := models.FindkeyNode(key)
	node_ip, node_port := key_node.IpAddr, key_node.Port
	// fmt.Println("Random node: "+node_ip, node_port)
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	conn, err := grpc.DialContext(ctx, node_ip+":"+node_port, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	co := pb.NewKeyServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	co.RPCAddkey(ctx, &pb.Key{Key: key})
}

func main() {
	for i := 0; i <= 99999999; i++ {
		Addkey(int32(i))
	}
}
