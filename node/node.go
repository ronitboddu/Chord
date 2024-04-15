package node

import (
	"Test2/models"
	"Test2/node/listener"
	"Test2/pb"
	"fmt"
	"sync"
	"time"
)

func CreateNode(curr_id int32, curr_ip_addr string, curr_port string, parent_wg *sync.WaitGroup) {
	defer parent_wg.Done()
	// fmt.Println("in node1")
	var wg sync.WaitGroup
	var node = pb.Node{
		Id:        curr_id,
		CurrIp:    &pb.NodeIp{Id: curr_id, IpAddr: curr_ip_addr, Port: curr_port},
		SuccIp:    nil,
		PredIp:    nil,
		HashTable: make(map[int32]string),
	}
	quit := make(chan bool)

	f := models.Fingers{Node: &node, Mu: &sync.RWMutex{}, FingerTable: make(map[int32]*pb.NodeIp), LogFileName: fmt.Sprintf("%d.txt", curr_id), M: 30}
	t := models.Transport{Node: &node, Finger: &f, LogFileName: fmt.Sprintf("%d.txt", curr_id), Quit: quit}

	wg.Add(1)
	go listener.GRPCListen(&wg, &t)
	// models.ClearLog(t.LogFileName)
	t.InitializeNode()
	// models.PrintNode(&t)
	// f.PrintFingerTable()
	ticker := time.NewTicker(5 * time.Second)

	// f.AddKey("3", 3)

	go t.PeriodicFunc(ticker)

	wg.Wait()
}
