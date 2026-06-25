// Package tenant provides the cross-cutting policy-callback layer (DESIGN.md §2, §8):
// a [tenant.Resolver] maps a tenant id to limits, retention, downsampling, and routing,
// hot-reloadable by the consumer. Not yet implemented (M3).
package tenant
