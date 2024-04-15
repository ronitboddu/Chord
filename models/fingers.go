package models

import (
	"Test2/pb"
	"fmt"
	"math"
	"sync"
)

type Fingers struct {
	Node        *pb.Node
	FingerTable map[int32]*pb.NodeIp
	Mu          *sync.RWMutex
	LogFileName string
	M           int32
}

func (f *Fingers) CreateFingerTable() {
	//f.FingerTable = make(map[int32]*pb.NodeIp)
	for i := 0; i < int(f.M); i++ {
		id := f.Node.Id
		key := f.GetFingerKey(id, int32(i))
		f.FingerTable[key] = nil
	}
}

func (f *Fingers) PrintFingerTable() {
	WriteLookupLog("Finger Table:\n", f.LogFileName)
	// f.Mu.Lock()
	for i := 0; i < int(f.M); i++ {
		id := f.Node.Id
		key := f.GetFingerKey(id, int32(i))
		// fmt.Println(id, key)
		if f.FingerTable[key] == nil {
			WriteLookupLog(fmt.Sprintf("%d  nil\n", key), f.LogFileName)
			// fmt.Println(key, "nil")
		} else {
			// fmt.Println(key, f.FingerTable[key].IpAddr)
			WriteLookupLog(fmt.Sprintf("%d  {%d , %s}\n", key, f.FingerTable[key].Id, f.FingerTable[key].IpAddr), f.LogFileName)
		}
	}
	// f.Mu.Unlock()
	WriteLookupLog("\n", f.LogFileName)
}

func (f *Fingers) GetFingerKey(id int32, i int32) int32 {
	return (id + int32(math.Pow(float64(2), float64(i)))) % int32(math.Pow(2.0, float64(f.M)))
}

func (f *Fingers) SetFirstFinger(node *pb.NodeIp) {
	// f.Mu.Lock()
	// defer f.Mu.Unlock()
	finger := f.GetFingerKey(f.Node.Id, 0)
	f.Mu.Lock()
	f.FingerTable[finger] = &pb.NodeIp{Id: node.Id, IpAddr: node.IpAddr, Port: node.Port}
	f.Mu.Unlock()
	// f.Mu.Unlock()
}

/*periodically call to fix fingers in finger table*/
func (f *Fingers) FixFingers(t *Transport) {
	// fmt.Println("in FixFingers")
	// t.Finger.Mu.Lock()
	if t.Finger.Node.SuccIp != nil {
		for i := 0; i < int(f.M); i++ {
			id := t.Node.Id
			key := f.GetFingerKey(id, int32(i))
			succ := t.FindNodeSuccessor(key, t.Node.CurrIp)
			// time.Sleep(3 * time.Second)

			f.Mu.Lock()
			f.FingerTable[key] = succ
			f.Mu.Unlock()
		}
	}
	if t.Finger.Node.SuccIp == nil {
		for i := 0; i < int(f.M); i++ {
			id := t.Node.Id
			key := f.GetFingerKey(id, int32(i))
			// time.Sleep(3 * time.Second)
			f.Mu.Lock()
			f.FingerTable[key] = nil
			f.Mu.Unlock()
		}
	}
	f.PrintFingerTable()
	// t.Finger.Mu.Unlock()
	// fmt.Println("out FixFingers")
}

// 3, 3, 8
// 7, 3, 8
// 3, 3, 5
