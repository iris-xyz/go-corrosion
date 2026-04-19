// Package corrosion provides a client for the Corrosion distributed SQLite database.
//
// [APIClient] is the main entry point for querying and subscribing to changes.
// All methods are safe for concurrent use.
//
//	client, err := corrosion.NewAPIClient(addr)
//	rows, err := client.QueryContext(ctx, "SELECT * FROM nodes")
//	sub, err := client.SubscribeContext(ctx, "SELECT * FROM nodes", nil, false)
//
// For unit tests that drive a subscription without a real Corrosion server,
// use the helpers in the corrosiontest subpackage.
package corrosion
