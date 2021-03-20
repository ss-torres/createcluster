package commands

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	kHost        = "127.0.0.1"
	kDefaultPath = "."
)

type Commands struct {
	redisServer string
	port        int
	timeout     int
	nodes       int
	replicas    int
}

func masterCount(nodes int, replicas int) int {
	return nodes / (replicas + 1)
}

func NewCommands(redisServer string, port int, timeout int,
	nodes int, replicas int) (*Commands, error) {
	if nodes%(replicas+1) != 0 {
		return nil, fmt.Errorf("should should be equal replicas for master(temporary)")
	}

	if masterCount(nodes, replicas) < 3 {
		return nil, fmt.Errorf("there should be at least 3 master nodes")
	}
	return &Commands{
		redisServer: redisServer,
		port:        port,
		timeout:     timeout,
		nodes:       nodes,
		replicas:    replicas,
	}, nil
}

func (c *Commands) Start() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nodes_count := c.nodes
	for idx := 0; idx < nodes_count; idx++ {
		portStr := strconv.Itoa(c.port + idx)
		timeoutStr := strconv.Itoa(c.timeout)
		cmd := exec.CommandContext(ctx, c.redisServer, "--port", portStr, "--cluster-enabled", "yes",
			"--cluster-config-file", "nodes-"+portStr+".conf", "--cluster-node-timeout", timeoutStr,
			"--appendonly", "yes", "--appendfilename", "appendonly-"+portStr+".aof",
			"--dbfilename", "dump-"+portStr+".rdb",
			"--logfile", "nodes-"+portStr+".log", "--daemonize", "yes")
		fmt.Printf("source: %s:%s, cmd: %s\n", kHost, portStr, cmd.String())
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("%v, output: %s", err, out)
		}
	}
	return nil
}

func (c *Commands) Meeting() error {
	portStr := strconv.Itoa(c.port)
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{net.JoinHostPort(kHost, portStr)},
	})
	defer rdb.Close()
	for idx := 1; idx < c.nodes; idx++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		status := rdb.ClusterMeet(ctx, kHost, strconv.Itoa(c.port+idx))
		fmt.Printf("source: %s:%s, cmd: %s\n", kHost, portStr, status.String())
		res, err := status.Result()
		if err != nil {
			return err
		}
		fmt.Printf("result: %s\n", res)
	}
	return nil
}

func (c *Commands) Replicate() error {
	if c.replicas == 0 {
		return nil
	}

	portStr := strconv.Itoa(c.port)
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{net.JoinHostPort(kHost, portStr)},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	str_cmd := rdb.ClusterNodes(ctx)
	fmt.Printf("source: %s:%s, cmd: %s\n", kHost, portStr, str_cmd.String())
	res, err := str_cmd.Result()
	if err != nil {
		return err
	}

	replics := []string{}
	buf := bufio.NewScanner(strings.NewReader(res))
	for buf.Scan() {
		replics = append(replics, buf.Text())
	}
	fmt.Println("line: ", len(replics))
	nodeIds := make(map[int]string)
	for _, line := range replics {
		buf = bufio.NewScanner(strings.NewReader(line))
		buf.Split(bufio.ScanWords)
		var nodeId string
		for buf.Scan() {
			if len(nodeId) == 0 {
				nodeId = buf.Text()
				continue
			}
			portStr := strings.SplitN(
				strings.SplitN(buf.Text(), ":", 2)[1], "@", 2)[0]
			port, err := strconv.Atoi(portStr)
			if err != nil {
				fmt.Printf("port parse failed, error: %v\n", err)
				break
			}
			nodeIds[port] = nodeId
			break
		}
	}
	fmt.Println(nodeIds)

	master_count := masterCount(c.nodes, c.replicas)
	for master_idx := 0; master_idx < master_count; master_idx++ {
		for replica_idx := 0; replica_idx < c.replicas; replica_idx++ {
			node_idx := master_idx + (replica_idx+1)*master_count
			portStr := strconv.Itoa(c.port + node_idx)
			slave_rdb := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs: []string{net.JoinHostPort(kHost, portStr)},
			})

			ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			status := slave_rdb.ClusterReplicate(ctx, nodeIds[c.port+master_idx])
			fmt.Printf("source: %s:%s, cmd: %s\n", kHost, portStr, status.String())
			res, err := status.Result()
			if err != nil {
				return err
			}
			fmt.Printf("result: %s\n", res)
		}
	}

	return nil
}

func (c *Commands) Stop() error {
	nodes_count := c.nodes
	for idx := 0; idx < nodes_count; idx++ {
		portStr := strconv.Itoa(c.port + idx)
		rdb := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: []string{net.JoinHostPort(kHost, portStr)},
		})

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		status := rdb.Shutdown(ctx)
		fmt.Printf("source: %s:%s, cmd:%v\n", kHost, portStr, status)
		res, err := status.Result()
		if err == nil {
			fmt.Printf("result: %s\n", res)
		}
	}
	return nil
}

func isRedisLog(fileName string) bool {
	return strings.HasPrefix(fileName, "nodes-") &&
		strings.HasSuffix(fileName, ".log")
}

func isRedisAppendOnly(fileName string) bool {
	return strings.HasPrefix(fileName, "appendonly") &&
		strings.HasSuffix(fileName, ".aof")
}

func isRedisDump(fileName string) bool {
	return strings.HasPrefix(fileName, "dump") &&
		strings.HasSuffix(fileName, ".rdb")
}

func isRedisConf(fileName string) bool {
	return strings.HasPrefix(fileName, "nodes-") &&
		strings.HasSuffix(fileName, ".conf")
}

func isRedisRelated(fileName string) bool {
	return isRedisLog(fileName) || isRedisAppendOnly(fileName) ||
		isRedisDump(fileName) || isRedisConf(fileName)
}

func GetAllRegularFileNames(path string) ([]string, error) {
	dir, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fileInfo, err := dir.Stat()
	if err != nil {
		return nil, err
	}
	if !fileInfo.IsDir() {
		return nil,
			fmt.Errorf("%s not dir, this shouldn't happen", kDefaultPath)
	}
	files, err := dir.Readdir(0)
	if err != nil {
		return nil, err
	}

	fileNames := make([]string, 10, 10)
	for _, file := range files {
		if !file.Mode().IsRegular() {
			continue
		}

		fileNames = append(fileNames, file.Name())
	}

	return fileNames, nil
}

func (c *Commands) Clean() error {
	fileNames, err := GetAllRegularFileNames(kDefaultPath)
	if err != nil {
		return err
	}
	for _, fileName := range fileNames {
		if !isRedisRelated(fileName) {
			continue
		}

		fmt.Printf("remove file %s\n", fileName)
		os.Remove(filepath.Join(fileName))
	}

	return nil
}

func (c *Commands) CleanLogs() error {
	fileNames, err := GetAllRegularFileNames(kDefaultPath)
	if err != nil {
		return err
	}
	for _, fileName := range fileNames {
		if !isRedisLog(fileName) {
			continue
		}

		fmt.Printf("remove file %s\n", fileName)
		os.Remove(filepath.Join(fileName))
	}

	return nil
}
