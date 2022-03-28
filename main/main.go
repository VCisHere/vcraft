package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
	"vcraft"
)

func main() {
	addrs := []string{"127.0.0.1:1500", "127.0.0.1:1501", "127.0.0.1:1502"}

	var rafts []*vcraft.Raft

	wg := &sync.WaitGroup{}

	for i := 0; i < len(addrs); i++ {
		wg.Add(1)
		go func(idx int) {
			if idx == 2 {
				time.Sleep(2 * time.Second)
			}
			rf, applyChan, _ := vcraft.NewRaft(addrs, idx)
			rafts = append(rafts, rf)
			wg.Done()

			go func() {
				value := ""
				for {
					msg := <-applyChan
					fmt.Printf("node%d 提交log: %v\n", idx, msg)
					if msg.CommandValid {
						value = msg.Command.(string)
						fmt.Printf("node%d : %s\n", idx, value)
					}
				}
			}()

			for {
				rf.Push(fmt.Sprintf("日志%d", rand.Int() % 1000))
				time.Sleep(5 * time.Second)
			}
		}(i)
	}

	wg.Wait()

	for i := 0; i < len(addrs); i++ {
		go func(idx int) {
			for {
				vcraft.PrintLogs(rafts[idx])
				time.Sleep(5 * time.Second)
			}
		}(i)
	}

	time.Sleep(13 * time.Second)

	vcraft.SleepSeconds(rafts[1])

	time.Sleep(1 * time.Hour)
}