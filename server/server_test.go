package server

import (
	"testing"
	"time"

	"golang.org/x/net/websocket"
	"fmt"
	"github.com/mesos/mesos-go/api/v0/mesosproto"
)

func TestNew(t *testing.T) {
	defaultOffer := mesosproto.Offer{}
	totalSends := 4

	for i := 0; i<totalSends; i++ {
		go func(){
			time.Sleep(3 * time.Second)
			OfferCh <- defaultOffer
		}()
	}

	q := make(chan bool)
	port := ":8080"
	OfferCh := make(chan mesosproto.Offer)

	go New(port, q, OfferCh)

	origin := fmt.Sprintf("http://localhost%s/", port)
	url := fmt.Sprintf("ws://localhost%s/ws", port)
	ws, err := websocket.Dial(url, "", origin)
	if err != nil {
		t.Fatal("Not connected")
	}

	count := 0
	for i := 0; i<totalSends; i++{
		time.Sleep(time.Duration(1) * time.Second)
		var msg = make([]byte, 512)
		var n int
		if n, err = ws.Read(msg); err != nil {
			t.Fatal(err)
		}

		fmt.Printf("Received: %v.\n", msg[:n])
		count++
	}

	q <- true
	time.Sleep(1 * time.Second)
}
