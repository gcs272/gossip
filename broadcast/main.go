package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastMessage struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

func main() {
	var messages sync.Map
	topology := map[string]map[string]bool{}

	n := maelstrom.NewNode()

	send := func(nid string, message int) {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			if _, err := n.SyncRPC(ctx, nid, BroadcastMessage{
				Type:    "broadcast",
				Message: message,
			}); err == nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body BroadcastMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		_, seen := messages.Load(body.Message)
		messages.Store(body.Message, true)

		// If we haven't seen it before, forward it along
		if !seen {
			for nid := range topology[n.ID()] {
				if nid != n.ID() && nid != msg.Src {
					go send(nid, body.Message)
				}
			}
		}

		return n.Reply(msg, map[string]interface{}{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var vals []int
		messages.Range(func(k, v interface{}) bool {
			vals = append(vals, k.(int))
			return true
		})

		return n.Reply(msg, map[string]interface{}{
			"type":     "read_ok",
			"messages": vals,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		type topologyMessage struct {
			Topology map[string][]string
		}

		var body topologyMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topology = make(map[string]map[string]bool)
		for nid, connected := range body.Topology {
			topology[nid] = make(map[string]bool)
			for _, edge := range connected {
				topology[nid][edge] = true
			}
		}

		return n.Reply(msg, map[string]interface{}{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
