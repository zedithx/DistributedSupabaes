package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"distributedsupabaes/internal/cluster"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		printUsage()
		return nil
	}

	switch args[0] {
	case "cluster":
		return runCluster(args[1:])
	case "control-plane":
		return runControlPlane(args[1:])
	case "node":
		return runNode(args[1:])
	case "put":
		return runPut(args[1:])
	case "get":
		return runGet(args[1:])
	case "lock":
		return runLock(args[1:])
	case "help", "-h", "--help":
		printUsage()
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runCluster(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing cluster subcommand")
	}

	switch args[0] {
	case "init":
		cfg, banner, err := cluster.ParseServeConfig("cluster init", args[1:], true)
		if err != nil {
			return err
		}

		node, err := cluster.NewNode(cfg)
		if err != nil {
			return err
		}
		if err := node.Start(); err != nil {
			return err
		}
		if err := node.InitCluster(); err != nil {
			return err
		}

		fmt.Println(banner)
		fmt.Printf("cluster-id=%s\n", cfg.ClusterID)
		fmt.Printf("join-token=%s\n", cfg.JoinToken)
		fmt.Printf("advertise-addr=%s\n", node.AdvertiseAddr())

		return waitForShutdown(node)
	case "status":
		return cluster.RunStatus(args[1:])
	default:
		return fmt.Errorf("unknown cluster subcommand %q", args[0])
	}
}

func runNode(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing node subcommand")
	}

	switch args[0] {
	case "join":
		cfg, banner, err := cluster.ParseServeConfig("node join", args[1:], false)
		if err != nil {
			return err
		}

		node, err := cluster.NewNode(cfg)
		if err != nil {
			return err
		}
		if err := node.Start(); err != nil {
			return err
		}
		if err := node.JoinCluster(context.Background()); err != nil {
			return err
		}

		fmt.Println(banner)
		fmt.Printf("joined-cluster=%s\n", cfg.ClusterID)
		fmt.Printf("manager=%s\n", cfg.ManagerAddr)
		fmt.Printf("advertise-addr=%s\n", node.AdvertiseAddr())

		return waitForShutdown(node)
	default:
		return fmt.Errorf("unknown node subcommand %q", args[0])
	}
}

func runControlPlane(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing control-plane subcommand")
	}

	switch args[0] {
	case "serve":
		return cluster.RunControlPlane(args[1:])
	default:
		return fmt.Errorf("unknown control-plane subcommand %q", args[0])
	}
}

func runPut(args []string) error {
	return cluster.RunPut(args)
}

func runGet(args []string) error {
	return cluster.RunGet(args)
}

func runLock(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing lock subcommand")
	}

	switch args[0] {
	case "acquire":
		return cluster.RunLockAcquire(args[1:])
	case "release":
		return cluster.RunLockRelease(args[1:])
	default:
		return fmt.Errorf("unknown lock subcommand %q", args[0])
	}
}

func waitForShutdown(node *cluster.Node) error {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	<-sigCh

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return node.Shutdown(ctx)
}

func printUsage() {
	usage := []string{
		"supaswarm commands:",
		"  cluster init   Start the first node and initialize a cluster",
		"  cluster status Query cluster health and membership",
		"  control-plane serve Start the immortal dashboard/API gateway",
		"  node join      Start a node and join an existing cluster",
		"  put            Write a key/value entry through the cluster",
		"  get            Read a key/value entry through the cluster",
		"  lock acquire   Acquire the distributed maintenance lease",
		"  lock release   Release the distributed maintenance lease",
	}

	fmt.Println(strings.Join(usage, "\n"))
}
