package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	n.Handle("add", func(msg maelstrom.Message) error {
		type addMessage struct {
			Delta int
		}

		var body addMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for {
			ctx := context.Background()
			val, err := kv.ReadInt(ctx, "val")
			if err != nil {
				val = 0
			}

			// Retry until the base number is what we're expecting
			if err := kv.CompareAndSwap(ctx, "val", val, val+body.Delta, true); err == nil {
				return n.Reply(msg, map[string]string{"type": "add_ok"})
			}
		}
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		val, err := kv.ReadInt(context.Background(), "val")
		if err != nil {
			return err
		}

		return n.Reply(msg, map[string]interface{}{
			"type":  "read_ok",
			"value": val,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
