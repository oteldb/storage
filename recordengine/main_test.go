package recordengine_test

import (
	"os"
	"testing"

	"github.com/go-faster/sdk/gold"
)

func TestMain(m *testing.M) {
	gold.Init()

	os.Exit(m.Run())
}
