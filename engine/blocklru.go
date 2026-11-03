package engine

type blockLRU struct {
	head, tail *blockEntry
}

func (l *blockLRU) PushFront(e *blockEntry) {
	e.prev, e.next = nil, l.head
	if l.head == nil {
		l.tail = e
	} else {
		l.head.prev = e
	}
	l.head = e
}

func (l *blockLRU) MoveToFront(e *blockEntry) {
	if l.head == e {
		return
	}
	l.Remove(e)
	l.PushFront(e)
}

func (l *blockLRU) Back() *blockEntry { return l.tail }

func (l *blockLRU) Remove(e *blockEntry) {
	if e.prev == nil {
		l.head = e.next
	} else {
		e.prev.next = e.next
	}
	if e.next == nil {
		l.tail = e.prev
	} else {
		e.next.prev = e.prev
	}
	e.prev, e.next = nil, nil
}
