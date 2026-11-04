package cluster

import (
	"bytes"
	"fmt"
	"math"
	"strconv"

	"github.com/go-faster/errors"
)

func decodePrimaryReject(body []byte, header string) (Reject, error) {
	var rej Reject
	if _, err := fmt.Sscanf(string(bytes.TrimSpace(body)), "%d %d %d", &rej.OOO, &rej.Cardinality, &rej.InFlight); err != nil {
		return Reject{}, errors.Wrap(err, "parse reject breakdown")
	}
	if rej.OOO < 0 || rej.Cardinality < 0 || rej.InFlight < 0 {
		return Reject{}, errors.New("negative primary rejection count")
	}
	if rej.OOO > math.MaxInt-rej.Cardinality || rej.OOO+rej.Cardinality > math.MaxInt-rej.InFlight {
		return Reject{}, errors.New("primary rejection total overflows")
	}
	if header != "" {
		n, err := strconv.Atoi(header)
		if err != nil || n < 0 || n > rej.OOO {
			return Reject{}, errors.New("invalid retention rejection count")
		}
		rej.Retention, rej.OOO = n, rej.OOO-n
	}
	return rej, nil
}
