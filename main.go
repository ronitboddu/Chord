package main

import (
	"Test2/central_server"
	"Test2/node"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

func main() {
	var wg sync.WaitGroup
	wg.Add(1)
	go central_server.StartCentralServer(&wg)
	num_nodes := 10
	port := "50001"
	node_set := make(map[int32]struct{})
	for i := 1; i <= num_nodes; i++ {
		wg.Add(1)
		ip_addr := "127.0.0." + fmt.Sprint(i)
		id := getRandomId()
		for {
			if _, ok := node_set[id]; !ok {
				node_set[id] = struct{}{}
				break
			}
			id = getRandomId()
		}
		fmt.Println(id, ip_addr)
		go node.CreateNode(id, ip_addr, port, &wg)
		time.Sleep(2 * time.Second)
	}
	// wg.Add(2)
	// go central_server.StartCentralServer(&wg)
	// go node.CreateNode(1, "127.0.0.1", "50001", &wg)
	// time.Sleep(2 * time.Second)
	// go node.CreateNode(11, "127.0.0.11", "50001", &wg)
	// time.Sleep(2 * time.Second)
	// go node.CreateNode(8, "127.0.0.8", "50001", &wg)
	// time.Sleep(2 * time.Second)
	// go node.CreateNode(3, "127.0.0.3", "50001", &wg)
	// time.Sleep(2 * time.Second)
	// go node.CreateNode(14, "127.0.0.14", "50001", &wg)
	// time.Sleep(2 * time.Second)
	// go node.CreateNode(7, "127.0.0.7", "50001", &wg)

	wg.Wait()
}

func getRandomId() int32 {
	return rand.Int32N(999999997)
}

//999999998
//999999997
//1037740589
//1073741824
//2147483647
