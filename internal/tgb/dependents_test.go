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

package tgb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/tango/internal/tgb"
)

func TestDependentLabels(t *testing.T) {
	reader, err := tgb.NewReader(mustEncode(t, buildTinyGraph(), tgb.EncodeOptions{HashBytes: 20, BlockSize: 16}))
	require.NoError(t, err)

	labels, err := tgb.DependentLabels(context.Background(), reader, []string{"//src/foo/bar:bar.go"})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"//src/foo/bar:all",
		"//src/foo/bar:bar",
		"//src/foo/bar:bar.go",
		"//src/foo/bar:cmd",
		"//src/foo/qux:qux_test",
	}, labels)
}

func TestReverseDepsCSR(t *testing.T) {
	reader, err := tgb.NewReader(mustEncode(t, buildTinyGraph(), tgb.EncodeOptions{HashBytes: 20, BlockSize: 16}))
	require.NoError(t, err)

	offsets, targets, err := reader.ReverseDepsCSR()
	require.NoError(t, err)

	pkg, name := tgb.SplitLabelString("//src/foo/bar:bar.go")
	id := reader.FindNode(pkg, name)
	require.GreaterOrEqual(t, id, 0)

	var labels []string
	for _, target := range targets[offsets[id]:offsets[id+1]] {
		labels = append(labels, reader.Label(int(target)))
	}
	assert.ElementsMatch(t, []string{
		"//src/foo/bar:all",
		"//src/foo/bar:bar",
	}, labels)
}

func TestDependentLabelsRejectsMissingTarget(t *testing.T) {
	reader, err := tgb.NewReader(mustEncode(t, buildTinyGraph(), tgb.EncodeOptions{HashBytes: 20, BlockSize: 16}))
	require.NoError(t, err)

	_, err = tgb.DependentLabels(context.Background(), reader, []string{"//missing:target"})
	require.Error(t, err)
}
