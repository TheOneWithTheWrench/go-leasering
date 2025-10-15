package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	leasering "go-leasering"

	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
)

var (
	nodeID     string
	ringID     string
	vnodeCount int
	leaseTTL   time.Duration
	dbURL      string
	crashMode  bool
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "ringnode",
		Short: "A distributed consistent hashing ring node",
		Long: `Ringnode is a demonstration of the go-lease-ring library.
It connects to a PostgreSQL database and joins a consistent hashing ring,
coordinating with other nodes to distribute partitions across the cluster.`,
		RunE: runNode,
	}

	rootCmd.Flags().StringVar(&nodeID, "node-id", "", "Unique node identifier (required)")
	rootCmd.Flags().StringVar(&ringID, "ring-id", "demo_ring", "Ring identifier")
	rootCmd.Flags().IntVar(&vnodeCount, "vnodes", 8, "Number of virtual nodes")
	rootCmd.Flags().DurationVar(&leaseTTL, "lease-ttl", 30*time.Second, "Lease time-to-live duration")
	rootCmd.Flags().StringVar(&dbURL, "db", "postgres://testuser:testpassword@localhost:5432/leasering_test_db?sslmode=disable", "PostgreSQL connection URL")
	rootCmd.Flags().BoolVar(&crashMode, "crash", false, "Crash mode: exit immediately on Ctrl+C without graceful shutdown")

	rootCmd.MarkFlagRequired("node-id")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runNode(cmd *cobra.Command, args []string) error {
	var ctx = context.Background()

	// Validate ringID
	if err := leasering.ValidateRingID(ringID); err != nil {
		return fmt.Errorf("invalid ring-id: %w", err)
	}

	// Connect to database
	fmt.Printf("Connecting to database...\n")
	var db, err = sql.Open("postgres", dbURL)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	// Create ring with options
	fmt.Printf("Creating ring node: %s\n", nodeID)
	var ring = leasering.NewRing(
		ringID,
		nodeID,
		leasering.WithVNodeCount(vnodeCount),
		leasering.WithLeaseTTL(leaseTTL),
	)

	// Start the ring (join and begin background workers)
	fmt.Printf("Joining ring '%s'...\n", ringID)
	if err := ring.Start(ctx, db); err != nil {
		return fmt.Errorf("failed to start ring: %w", err)
	}

	fmt.Printf("✓ Successfully joined ring!\n\n")

	// Print initial state
	printStatus(ring)

	// Set up periodic status updates
	var ticker = time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Set up signal handling for graceful shutdown
	var sigCh = make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Main loop
	for {
		select {
		case <-ticker.C:
			printStatus(ring)
		case sig := <-sigCh:
			if crashMode {
				fmt.Printf("\n\n💥 Received signal %v, crashing immediately (no cleanup)...\n", sig)
				os.Exit(1)
			}
			fmt.Printf("\n\nReceived signal %v, shutting down gracefully...\n", sig)
			if err := ring.Stop(ctx, db); err != nil {
				return fmt.Errorf("failed to stop ring: %w", err)
			}
			fmt.Printf("✓ Gracefully left ring\n")
			return nil
		}
	}
}

func printStatus(ring *leasering.Ring) {
	fmt.Print("\033[2J\033[H") // Clear screen and move cursor to top
	fmt.Println(ring.String())
	if crashMode {
		fmt.Printf("\n💥 CRASH MODE: Press Ctrl+C to crash (no cleanup)\n")
	} else {
		fmt.Printf("\nPress Ctrl+C to gracefully leave the ring\n")
	}
}
