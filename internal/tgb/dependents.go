// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tgb

import (
	"context"
	"fmt"
	"sort"
)

const dependentTargetsCancelCheckInterval = 4096

// DependentLabels returns the supplied labels and every target that
// transitively depends on them.
func DependentLabels(ctx context.Context, r *Reader, labels []string) ([]string, error) {
	seeds := make([]int32, 0, len(labels))
	for _, label := range labels {
		pkg, name := splitLabel(label)
		id := r.FindNode(pkg, name)
		if id < 0 {
			return nil, fmt.Errorf("target %q is absent from target graph", label)
		}
		seeds = append(seeds, int32(id))
	}

	reverseOffsets, reverseTargets, err := r.ReverseDepsCSR()
	if err != nil {
		return nil, fmt.Errorf("read graph reverse dependencies: %w", err)
	}
	visited := make([]bool, r.NodeCount())
	queue := make([]int32, 0, len(seeds))
	for _, seed := range seeds {
		if !visited[int(seed)] {
			visited[seed] = true
			queue = append(queue, seed)
		}
	}
	for head := 0; head < len(queue); head++ {
		if head%dependentTargetsCancelCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return nil, context.Cause(ctx)
			}
		}
		id := queue[head]
		for _, dependent := range reverseTargets[reverseOffsets[id]:reverseOffsets[id+1]] {
			if !visited[int(dependent)] {
				visited[dependent] = true
				queue = append(queue, dependent)
			}
		}
	}

	result := make([]string, 0, len(queue))
	for _, id := range queue {
		result = append(result, r.Label(int(id)))
	}
	sort.Strings(result)
	return result, nil
}
