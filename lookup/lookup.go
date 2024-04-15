package main

import (
	"Test2/models"
	"fmt"
)

// func FindkeyNode(key int32) *pb.NodeIp {
// 	models.ClearLookupLog()
// 	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
// 	conn, err := grpc.DialContext(ctx, "127.0.0.254:50001", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer conn.Close()

// 	c := pb.NewKeyServiceClient(conn)
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
// 	defer cancel()

// 	node, err := c.RPCLookup(ctx, &pb.Key{Key: key})

// 	if err != nil {
// 		panic(err)
// 	}
// 	return node
// }

func main() {
	node := models.FindkeyNode(845771901)
	fmt.Println(node.Id, node.IpAddr, node.Port)
}

//72718561
//72717621
