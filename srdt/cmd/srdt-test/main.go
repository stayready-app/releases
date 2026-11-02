package main

import (
	"encoding/json"
	"example.com/srdt/emulator"
	"example.com/srdt/testkit"
	"flag"
	"fmt"
	"os"
)

type Report struct {
	Status   string
	Messages int
	Verified int
	Invalid  int
	Network  emulator.Stats
}

func main() {
	seed := flag.Int64("seed", 42, "test seed")
	count := flag.Int("messages", 1000, "message count")
	min := flag.Int("min-size", 1, "min bytes")
	max := flag.Int("max-size", 8192, "max bytes")
	loss := flag.Float64("loss", 0, "percent lost")
	dropMin := flag.Int("drop-min", -1, "size filter min")
	dropMax := flag.Int("drop-max", -1, "size filter max")
	flag.Parse()
	if *count < 0 || *count > 65536 || *min < 0 || *max < *min || *max > testkit.MaxGeneratedMessage || int64(*count)*(int64(*max)+12) > 64<<20 {
		fmt.Fprintln(os.Stderr, "invalid count")
		os.Exit(2)
	}
	cfg := emulator.Config{LossPercent: *loss, DelayMaxMs: 10}
	if *dropMin >= 0 && *dropMax >= 0 {
		cfg.DropSizes = []emulator.SizeRange{{Min: *dropMin, Max: *dropMax}}
	}
	e, err := emulator.New(*seed, cfg, emulator.Config{})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	g := testkit.NewGenerator(*seed)
	messages := make([]testkit.Message, 0, *count)
	for i := 0; i < *count; i++ {
		m, err := g.Next(*min, *max)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		messages = append(messages, m)
	}
	v, _ := testkit.NewVerifier(messages)
	for _, m := range messages {
		if err := e.Submit(emulator.ClientToServer, testkit.Encode(m)); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}
	invalid := 0
	for _, ev := range e.Drain() {
		m, err := testkit.Decode(ev.Packet.Data)
		if err == nil {
			err = v.Accept(m)
		}
		if err != nil {
			invalid++
		}
	}
	status := "PASS"
	if v.Complete() != nil || invalid != 0 {
		status = "FAIL"
	}
	r := Report{status, len(messages), v.Received(), invalid, e.Stats(emulator.ClientToServer)}
	b, _ := json.MarshalIndent(r, "", "  ")
	fmt.Println(string(b))
	if status != "PASS" {
		os.Exit(1)
	}
}
