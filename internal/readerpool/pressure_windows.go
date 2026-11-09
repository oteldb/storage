//go:build windows

package readerpool

import (
	"github.com/go-faster/errors"
	"golang.org/x/sys/windows"
)

func systemDescriptorPressure(err error) bool {
	return errors.Is(err, windows.ERROR_TOO_MANY_OPEN_FILES)
}
