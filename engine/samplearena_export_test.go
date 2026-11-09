package engine

// ClearSampleArenaPoolForTest drops shared free pages. Callers must exclude concurrent engines.
func ClearSampleArenaPoolForTest() {
	for len(freeSampleNumerics) > 0 {
		<-freeSampleNumerics
	}
	for len(freeSampleHeaders) > 0 {
		<-freeSampleHeaders
	}
}
