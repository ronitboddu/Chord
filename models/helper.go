package models

import (
	"Test2/pb"
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func IsBetweenRightIncl(id int32, a int32, b int32) bool {
	if (a < id && id <= b) || (b < a && (id > a || id <= b)) {
		return true
	}
	return false
}

func IsBetween(id int32, a int32, b int32) bool {
	if (a < id && id < b) || (b < a && (id > a || id < b)) {
		return true
	}
	return false
}

func IsBetweenLeftIncl(id int32, a int32, b int32) bool {
	if (a <= id && id < b) || (b < a && (id > a || id <= b)) {
		return true
	}
	return false
}

func PrintNode(t *Transport) {
	// t.Finger.Mu.Lock()
	if t.Node.CurrIp != nil {
		WriteLookupLog("Id: "+fmt.Sprint(t.Node.Id)+", IpAddr: "+t.Node.CurrIp.IpAddr+" , Port: "+t.Node.CurrIp.Port+"\n", t.LogFileName)
	} else {
		WriteLookupLog("Current Node is nil\n", t.LogFileName)
	}

	if t.Node.SuccIp != nil {
		WriteLookupLog("SuccId: "+fmt.Sprint(t.Node.SuccIp.Id)+", SuccIpAddr: "+t.Node.SuccIp.IpAddr+" , SuccPort: "+t.Node.SuccIp.Port+"\n", t.LogFileName)
	} else {
		WriteLookupLog("Successor Node is nil\n", t.LogFileName)
	}

	if t.Node.PredIp != nil {
		WriteLookupLog("PredId: "+fmt.Sprint(t.Node.PredIp.Id)+", PredIpAddr: "+t.Node.PredIp.IpAddr+" , PredPort: "+t.Node.PredIp.Port+"\n", t.LogFileName)
	} else {
		WriteLookupLog("Predecessor Node is nil\n", t.LogFileName)
	}

	WriteLookupLog("\n", t.LogFileName)
	// t.Finger.Mu.Unlock()
}

func PrintKeys(t *Transport) {
	WriteLookupLog("\nKeys: [", t.LogFileName)
	for k, v := range t.Node.HashTable {
		WriteLookupLog(fmt.Sprintf("%d : "+v+", ", k), t.LogFileName)
	}
	WriteLookupLog("]\n", t.LogFileName)
}

/* Set successor variable of the node*/
func SetNodeSuccessor(succ *pb.NodeIp, t *Transport) {
	// fmt.Println("in SetNodeSuccessor")
	t.Finger.Mu.Lock()
	t.Node.SuccIp = &pb.NodeIp{Id: succ.Id, IpAddr: succ.IpAddr, Port: succ.Port}
	t.Finger.Mu.Unlock()
	// update 1st finger in the finger table
	t.Finger.SetFirstFinger(succ)
	// fmt.Println("out SetNodeSuccessor")
}

func FindkeyNode(key int32) *pb.NodeIp {
	ClearLog("lookup_log.txt")
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	conn, err := grpc.DialContext(ctx, "127.0.0.254:50001", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	c := pb.NewKeyServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	node, err := c.RPCLookup(ctx, &pb.Key{Key: key})

	if err != nil {
		panic(err)
	}
	return node
}

// func Between(key int32, a int32, b int32) bool {
// 	if a > b {
// 		return a < key || b >= key
// 	} else if b > a {
// 		return a < key && b >= key
// 	} else if a == b {
// 		return a != key
// 	}
// 	return false
// }
