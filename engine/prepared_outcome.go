package engine

type preparedOutcome uint8

const (
	preparedDone preparedOutcome = iota
	preparedSnapshot
	preparedPressure
	preparedWorkLimit
)

func (o preparedOutcome) reason() string {
	switch o {
	case preparedSnapshot:
		return "snapshot"
	case preparedPressure:
		return "pressure"
	case preparedWorkLimit:
		return "work_limit"
	default:
		return "done"
	}
}
