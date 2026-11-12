package engine

func closePartReaders(parts []*part) {
	for _, p := range parts {
		p.closeReaders()
	}
}
