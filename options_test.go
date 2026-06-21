package leasering

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOptions(t *testing.T) {
	t.Run("should derive timing defaults from lease ttl", func(t *testing.T) {
		// Act
		opts := defaultOptions()

		// Assert
		assert.Equal(t, 15*time.Second, opts.leaseTTL)
		assert.Equal(t, 5*time.Second, opts.renewalInterval)
		assert.Equal(t, 7500*time.Millisecond, opts.refreshInterval)
		assert.Equal(t, 15*time.Second, opts.proposalTTL)
		assert.Equal(t, 22500*time.Millisecond, opts.joinTimeout)
	})

	t.Run("should derive timing options from custom lease ttl", func(t *testing.T) {
		// Arrange
		opts := defaultOptions()

		// Act
		WithLeaseTTL(6 * time.Second)(&opts)

		// Assert
		assert.Equal(t, 6*time.Second, opts.leaseTTL)
		assert.Equal(t, 2*time.Second, opts.renewalInterval)
		assert.Equal(t, 3*time.Second, opts.refreshInterval)
		assert.Equal(t, 6*time.Second, opts.proposalTTL)
		assert.Equal(t, 9*time.Second, opts.joinTimeout)
	})
}
