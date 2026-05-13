package corrosion

import (
	"context"
	"time"
)

const healthTimeout = 2 * time.Second

// Health returns nil if the Corrosion API answers a trivial query, otherwise
// an error.
func (c *APIClient) Health(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	rows, err := c.QueryContext(ctx, "SELECT 1")
	if err != nil {
		return err
	}
	return rows.Close()
}
