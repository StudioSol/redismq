package main

import (
	"github.com/adeven/redismq"
	"log"
	"math/rand"
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(5)
	over := redismq.NewOverseer("localhost:6379", "", int64(9))
	server := redismq.NewServer("9999", over)
	go server.Start()
	go write("example")
	go write("example")

	go read("example", "1")
	go read("example", "2")

	select {}
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func write(queue string) {
	testQueue := redismq.NewQueue("localhost:6379", "", int64(9), queue)
	payload := randomString(1024 * 1) //adjust for size
	for {
		testQueue.Put(payload)
	}
}

func read(queue, prefix string) {
	testQueue := redismq.NewQueue("localhost:6379", "", int64(9), queue)
	consumer, err := testQueue.AddConsumer("testconsumer" + prefix)
	if err != nil {
		panic(err)
	}
	consumer.ResetWorking()
	for {
		p, err := consumer.MultiGet(200)
		if err != nil {
			log.Println(err)
			continue
		}
		p[len(p)-1].MultiAck()
	}
}