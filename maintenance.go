package storage

import (
	"sort"

	"github.com/oteldb/storage/internal/parallel"
)

type maintTask struct {
	run      func()
	pressure int64
	deferred bool
}

func sortMaintTasks(tasks []maintTask) {
	sort.SliceStable(tasks, func(i, j int) bool {
		if tasks[i].deferred != tasks[j].deferred {
			return tasks[i].deferred
		}

		return tasks[i].pressure > tasks[j].pressure
	})
}

func runMaintTasks(tasks []maintTask, limit int) {
	sortMaintTasks(tasks)
	for len(tasks) > 0 && tasks[0].deferred {
		tasks[0].run()
		tasks = tasks[1:]
	}

	parallel.ForEach(len(tasks), limit, func(i int) { tasks[i].run() })
}
