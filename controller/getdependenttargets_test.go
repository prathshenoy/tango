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

package controller

import (
	"bytes"
	"context"
	"encoding/gob"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/tango/core/storage"
	storagemock "github.com/uber/tango/core/storage/storagemock"
	"github.com/uber/tango/entity"
	pb "github.com/uber/tango/tangopb"
	tangomock "github.com/uber/tango/tangopb/tangopbmock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

func TestGetDependentTargets(t *testing.T) {
	ctrl := gomock.NewController(t)
	stream := tangomock.NewMockTangoServiceGetDependentTargetsYARPCServer(ctrl)
	stream.EXPECT().Context().Return(context.Background())
	stream.EXPECT().Send(&pb.GetDependentTargetsResponse{Targets: []string{
		"//app:binary",
		"//lib:library",
		"//service:binary",
	}}).Return(nil)

	var graph bytes.Buffer
	encoder := gob.NewEncoder(&graph)
	require.NoError(t, encoder.Encode(entity.GetTargetGraphResponse{Targets: []entity.OptimizedTarget{
		{ID: 1},
		{ID: 2, DirectDependencies: []int32{1}},
		{ID: 3, DirectDependencies: []int32{2}},
	}}))
	require.NoError(t, encoder.Encode(entity.GetTargetGraphResponse{Metadata: &entity.Metadata{TargetIDMapping: map[int32]string{
		1: "//lib:library",
		2: "//service:binary",
		3: "//app:binary",
	}}}))

	store := storagemock.NewMockStorage(ctrl)
	gomock.InOrder(
		store.EXPECT().Get(gomock.Any(), gomock.Any()).
			Return(storage.DownloadResponse{ReadCloser: newMockReadCloser([]byte("treehash"))}, nil),
		store.EXPECT().Get(gomock.Any(), gomock.Any()).
			Return(storage.DownloadResponse{ReadCloser: newMockReadCloser(graph.Bytes())}, nil),
	)
	c := NewController(context.Background(), Params{
		RepoConfig: allowAnyRepositoryConfigProvider{},
		Logger:     zaptest.NewLogger(t),
		Storage:    store,
	})

	err := c.GetDependentTargets(&pb.GetDependentTargetsRequest{
		BuildDescription: &pb.BuildDescription{
			Strategy: pb.COMPUTATION_STRATEGY_UNSET,
			Remote:   "repo:go-code",
			BaseSha:  "sha",
		},
		Targets: []string{"//lib:library"},
	}, stream)
	require.NoError(t, err)
}

func TestValidateGetDependentTargetsRequest(t *testing.T) {
	assert.Error(t, validateGetDependentTargetsRequest(nil))
	assert.Error(t, validateGetDependentTargetsRequest(&pb.GetDependentTargetsRequest{}))
	assert.Error(t, validateGetDependentTargetsRequest(&pb.GetDependentTargetsRequest{
		BuildDescription: &pb.BuildDescription{Strategy: pb.COMPUTATION_STRATEGY_UNSET, Remote: "repo:go-code", BaseSha: "sha"},
	}))
	assert.Error(t, validateGetDependentTargetsRequest(&pb.GetDependentTargetsRequest{
		BuildDescription: &pb.BuildDescription{Strategy: pb.COMPUTATION_STRATEGY_UNSET, Remote: "repo:go-code", BaseSha: "sha"},
		Targets:          []string{""},
	}))
	assert.NoError(t, validateGetDependentTargetsRequest(&pb.GetDependentTargetsRequest{
		BuildDescription: &pb.BuildDescription{Strategy: pb.COMPUTATION_STRATEGY_UNSET, Remote: "repo:go-code", BaseSha: "sha"},
		Targets:          []string{"//lib:library"},
	}))
}

func TestSendDependentTargetsSplitsResponses(t *testing.T) {
	ctrl := gomock.NewController(t)
	stream := tangomock.NewMockTangoServiceGetDependentTargetsYARPCServer(ctrl)
	first := "//lib:first"
	second := "//lib:second"
	stream.EXPECT().Send(&pb.GetDependentTargetsResponse{Targets: []string{first}}).Return(nil)
	stream.EXPECT().Send(&pb.GetDependentTargetsResponse{Targets: []string{second}}).Return(nil)

	maxBytes := (&pb.GetDependentTargetsResponse{Targets: []string{first}}).Size()
	require.NoError(t, sendDependentTargets(stream, []string{first, second}, maxBytes))
}

func TestDependentLabels(t *testing.T) {
	reader := newGraphReader(t,
		entity.GetTargetGraphResponse{Targets: []entity.OptimizedTarget{
			{ID: 1},
			{ID: 2, DirectDependencies: []int32{1}},
			{ID: 3, DirectDependencies: []int32{2}},
		}},
		entity.GetTargetGraphResponse{Metadata: &entity.Metadata{TargetIDMapping: map[int32]string{
			1: "//lib:library",
			2: "//service:binary",
			3: "//app:binary",
		}}},
	)

	labels, err := dependentLabels(context.Background(), reader, []string{"//lib:library"})
	require.NoError(t, err)
	assert.Equal(t, []string{"//app:binary", "//lib:library", "//service:binary"}, labels)
}

func TestDependentLabelsRejectsMissingTarget(t *testing.T) {
	reader := newGraphReader(t,
		entity.GetTargetGraphResponse{Targets: []entity.OptimizedTarget{{ID: 1}}},
		entity.GetTargetGraphResponse{Metadata: &entity.Metadata{TargetIDMapping: map[int32]string{1: "//lib:library"}}},
	)

	_, err := dependentLabels(context.Background(), reader, []string{"//missing:target"})
	require.Error(t, err)
}
