package bucketindex_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/backend/bucketindex"
)

func TestRepairStatsAdd(t *testing.T) {
	t.Parallel()

	s := bucketindex.RepairStats{Local: 1, Fetched: 2, Unsatisfiable: 3, Incomplete: 4, Failed: 5, Lost: 6, Revoked: 7}
	s.Add(bucketindex.RepairStats{Local: 10, Fetched: 20, Unsatisfiable: 30, Incomplete: 40, Failed: 50, Lost: 60, Revoked: 70})

	assert.Equal(t, bucketindex.RepairStats{
		Local: 11, Fetched: 22, Unsatisfiable: 33, Incomplete: 44, Failed: 55, Lost: 66, Revoked: 77,
	}, s)
}
