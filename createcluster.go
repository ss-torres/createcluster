package main

import (
	"createcluster/commands"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"time"
)

var redisServer = flag.String("redis-server", "/home/u/snap/redis-5.0.8/src/redis-server", "redis server path")
var port = flag.Int("port", 30000, "redis port for first node")
var timeout = flag.Int("timeout", 2000, "redis cluster node timeout")
var nodes = flag.Int("nodes", 6, "redis cluster node count (should be no less than 3)")
var replicas = flag.Int("replicas", 1, "redis cluster replicas")
var command = flag.String("command", "", "redis cluster command")

func main() {
	flag.Parse()
	if *port <= 0 || *port > math.MaxUint16 {
		fmt.Printf("port is invalid: %d\n", *port)
		os.Exit(-1)
	}
	if *timeout <= 0 {
		fmt.Printf("timeout is invalid: %d\n", *timeout)
		os.Exit(-1)
	}
	if *nodes <= 3 || (*nodes+*port-1) > math.MaxUint16 {
		fmt.Printf("nodes is invalid: %d\n", *nodes)
		os.Exit(-1)
	}
	if *replicas < 0 {
		fmt.Printf("replicas is invalid: %d\n", *replicas)
		os.Exit(-1)
	}

	coms := sort.StringSlice{}
	coms = append(coms, "start")
	coms = append(coms, "meeting")
	coms = append(coms, "replicate")
	coms.Sort()
	if idx := sort.SearchStrings(coms, *command); idx == len(coms) {
		fmt.Println("invalid command, commands: ", coms)
		return
	}

	c, err := commands.NewCommands(*redisServer, *port, *timeout,
		*nodes, *replicas)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(-1)
	}

	err = c.Start()
	if err != nil {
		fmt.Printf("cluster start failed, error: %v\n", err)
		os.Exit(-1)
	}

	time.Sleep(1 * time.Second)

	err = c.Meeting()
	if err != nil {
		fmt.Printf("cluster meeting failed, error: %v\n", err)
		os.Exit(-1)
	}

	time.Sleep(3 * time.Second)

	err = c.Replicate()
	if err != nil {
		fmt.Printf("cluster replicate failed, error: %v\n", err)
		os.Exit(-1)
	}

	err = c.Stop()
	if err != nil {
		fmt.Printf("close all nodes failed, error: %v\n", err)
		os.Exit(-1)
	}

	err = c.Clean()
	if err != nil {
		fmt.Printf("remove all node files failed, error: %v\n", err)
		os.Exit(-1)
	}
}
