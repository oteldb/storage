package chunk

import (
	"os"
	"testing"

	"github.com/go-faster/sdk/gold"
)

func TestMain(m *testing.M) {
	gold.Init() // registers -update/-clean for golden wire-format files (see _golden/)

	os.Exit(m.Run())
}
