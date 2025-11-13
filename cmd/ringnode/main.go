package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	leasering "go-leasering"

	_ "github.com/lib/pq"
	"github.com/eiannone/keyboard"
	"github.com/spf13/cobra"
)

var (
	ringID     string
	vnodeCount int
	leaseTTL   time.Duration
	dbURL      string
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

	rootCmd.Flags().StringVar(&ringID, "ring-id", "demo_ring", "Ring identifier")
	rootCmd.Flags().IntVar(&vnodeCount, "vnodes", 8, "Number of virtual nodes")
	rootCmd.Flags().DurationVar(&leaseTTL, "lease-ttl", 10*time.Second, "Lease time-to-live duration")
	rootCmd.Flags().StringVar(&dbURL, "db", "postgres://testuser:testpassword@localhost:5432/leasering_test_db?sslmode=disable", "PostgreSQL connection URL")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runNode(cmd *cobra.Command, args []string) error {
	var (
		ctx         = context.Background()
		db          *sql.DB
		ring        *leasering.Ring
		err         error
		isConnected = false
	)

	// Connect to database
	fmt.Printf("Connecting to database...\n")
	db, err = sql.Open("postgres", dbURL)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	// Create ring node
	// Logs go to stderr so they don't get cleared by status updates
	fmt.Printf("Creating ring node\n")
	ring = leasering.NewRingNode(
		db,
		ringID,
		leasering.WithVNodeCount(vnodeCount),
		leasering.WithLeaseTTL(leaseTTL),
		leasering.WithLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))),
	)

	// Start the ring (join and begin background workers)
	fmt.Printf("Joining ring '%s'...\n", ringID)
	if err := ring.Start(ctx); err != nil {
		return fmt.Errorf("failed to start ring: %w", err)
	}
	isConnected = true

	fmt.Printf("✓ Successfully joined ring!\n\n")

	// Print initial state
	printStatus(ring, isConnected)

	// Set up periodic status updates
	var ticker = time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Set up signal handling for graceful shutdown
	var sigCh = make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Initialize keyboard
	if err := keyboard.Open(); err != nil {
		return fmt.Errorf("failed to initialize keyboard: %w", err)
	}
	defer keyboard.Close()

	// Keyboard input channel
	var keyCh = make(chan rune)
	go func() {
		for {
			char, _, err := keyboard.GetKey()
			if err != nil {
				return
			}
			keyCh <- char
		}
	}()

	// Main loop
	for {
		select {
		case <-ticker.C:
			printStatus(ring, isConnected)
		case key := <-keyCh:
			switch key {
			case 'd', 'D':
				if isConnected {
					fmt.Fprintf(os.Stderr, "\n🔌 Disconnecting from database...\n")
					db.Close()
					isConnected = false
				}
			case 'r', 'R':
				if !isConnected {
					fmt.Fprintf(os.Stderr, "\n🔌 Reconnecting to database...\n")

					// Stop the old ring if it's still running
					ring.Stop(ctx)

					// Create new database connection
					db, err = sql.Open("postgres", dbURL)
					if err != nil {
						fmt.Fprintf(os.Stderr, "❌ Failed to reconnect: %v\n", err)
						break
					}

					if err := db.PingContext(ctx); err != nil {
						fmt.Fprintf(os.Stderr, "❌ Failed to ping database: %v\n", err)
						break
					}

					// Create new ring with new database connection
					ring = leasering.NewRingNode(
						db,
						ringID,
						leasering.WithVNodeCount(vnodeCount),
						leasering.WithLeaseTTL(leaseTTL),
						leasering.WithLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
							Level: slog.LevelInfo,
						}))),
					)

					// Start the ring
					if err := ring.Start(ctx); err != nil {
						fmt.Fprintf(os.Stderr, "❌ Failed to rejoin ring: %v\n", err)
						break
					}

					isConnected = true
					fmt.Fprintf(os.Stderr, "✓ Reconnected to database and rejoined ring\n")
				}
			case 'c', 'C':
				fmt.Printf("\n\n💥 Crashing immediately (no cleanup)...\n")
				os.Exit(1)
			case 'q', 'Q':
				fmt.Printf("\n\nShutting down gracefully...\n")
				if isConnected {
					if err := ring.Stop(ctx); err != nil {
						return fmt.Errorf("failed to stop ring: %w", err)
					}
					fmt.Printf("✓ Gracefully left ring\n")
				}
				return nil
			}
		case sig := <-sigCh:
			fmt.Printf("\n\n💥 Received signal %v, crashing immediately (no cleanup)...\n", sig)
			os.Exit(1)
		}
	}
}

func printStatus(ring *leasering.Ring, isConnected bool) {
	fmt.Print("\033[2J\033[H") // Clear screen and move cursor to top
	fmt.Println(ring.String())

	if !isConnected {
		fmt.Printf("\n⚠️  DATABASE DISCONNECTED\n")
	}

	fmt.Printf("\nControls:\n")
	if isConnected {
		fmt.Printf("  [d] Disconnect from database\n")
	} else {
		fmt.Printf("  [r] Reconnect to database\n")
	}
	fmt.Printf("  [c] Crash without cleanup\n")
	fmt.Printf("  [q] Quit gracefully\n")
}
