package storage

// ringSize is the number of nodes in s's published hash ring.
//
// Cluster tests must gate readiness on this, not on len(membership.Members()). The two are updated
// separately: a watch event writes the member map under the lock, and the ring is rebuilt from that
// map and published afterwards through an atomic pointer. So Members() can report the full set while
// Ring() still holds one fewer node, and a test that waits on the member count then asserts on a
// ring lookup fails intermittently — TestClusterECRackAwarePlacement saw exactly that, waiting for 6
// members and then reading 5 owners out of the ring.
//
// The ring is what every code path under test actually reads (placement, ownership, routing), so it
// is what a readiness check must wait for. It is also the stronger condition: the ring is built from
// the member map, so a ring of N nodes implies N members were present when it was published.
func ringSize(s *Storage) int {
	if s.cluster == nil || s.cluster.membership == nil {
		return 0
	}

	r := s.cluster.membership.Ring()
	if r == nil {
		return 0
	}

	return r.Len()
}
