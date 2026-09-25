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
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"sort"
	"time"

	tangoerrors "github.com/uber/tango/core/errors"
	"github.com/uber/tango/core/storage"
	"github.com/uber/tango/entity"
	"github.com/uber/tango/internal/mapper"
	"github.com/uber/tango/internal/tgb"
	"github.com/uber/tango/observability/metrics"
	pb "github.com/uber/tango/tangopb"
	"go.uber.org/zap"
)

// GetDependentTargets returns every target that transitively depends on a requested target.
func (c *controller) GetDependentTargets(request *pb.GetDependentTargetsRequest, stream pb.TangoServiceGetDependentTargetsYARPCServer) (retErr error) {
	validationErr := validateGetDependentTargetsRequest(request)
	if validationErr != nil {
		validationErr = tangoerrors.NewUser(validationErr)
	}
	repoCfg, repo, repositoryErr := c.resolveRequestRepository(request.GetBuildDescription().GetRemote(), validationErr)
	e := c.emitter.Tagged(map[string]string{metrics.TagRepo: repo})
	op := metrics.Begin(e, opGetDependentTargets, metrics.SlowDurationBuckets)
	logger := c.logger.WithLazy(zap.String("repository", repo))

	defer func() {
		op.Complete(retErr)
		if retErr != nil {
			logger.Error("GetDependentTargets failed", tangoerrors.Fields(retErr)...)
			retErr = toWireError(retErr)
		}
	}()
	if repositoryErr != nil {
		return repositoryErr
	}

	ctx, cancel := c.linkRequestCtx(stream.Context())
	defer cancel()
	start := time.Now()

	build, err := mapper.ProtoToBuildDescription(request.GetBuildDescription())
	if err != nil {
		return tangoerrors.NewUser(fmt.Errorf("convert build description: %w", err))
	}

	graphRequest := entity.GetTargetGraphRequest{
		Build:       build,
		BypassCache: request.GetBypassCache(),
	}
	reader, err := c.getGraph(ctx, e, graphRequest, repoCfg.RepositoryID)
	if err != nil {
		return fmt.Errorf("get graph: %w", err)
	}
	if reader == nil {
		return nil
	}
	defer func() { _ = reader.Close() }()

	var labels []string
	if graph, ok := reader.(*storage.TGBGraphReader); ok {
		labels, err = tgb.DependentLabels(ctx, graph.TGB(), request.GetTargets())
	} else {
		labels, err = dependentLabels(ctx, reader, request.GetTargets())
	}
	if err != nil {
		return err
	}

	if err := sendDependentTargets(stream, labels, c.maxMessageBytes); err != nil {
		return fmt.Errorf("send response: %w", err)
	}
	logger.Info("GetDependentTargets: Successfully processed request",
		zap.Int("target_count", len(labels)),
		zap.Duration("total_duration", time.Since(start)),
	)
	return nil
}

func validateGetDependentTargetsRequest(request *pb.GetDependentTargetsRequest) error {
	if request == nil {
		return errors.New("request is required")
	}
	build := request.GetBuildDescription()
	if build == nil {
		return errors.New("build description is required")
	}
	if build.GetRemote() == "" {
		return errors.New("build description remote is required")
	}
	if build.GetBaseSha() == "" {
		return errors.New("build description base_sha is required")
	}
	if len(request.GetTargets()) == 0 {
		return errors.New("at least one target is required")
	}
	for i, target := range request.GetTargets() {
		if target == "" {
			return fmt.Errorf("targets[%d] is required", i)
		}
	}
	return nil
}

func dependentLabels(ctx context.Context, reader storage.GraphReader, requested []string) ([]string, error) {
	targets := make(map[int32][]int32)
	labels := make(map[int32]string)

	for {
		chunk, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		for _, target := range chunk.Targets {
			targets[target.ID] = target.DirectDependencies
		}
		if chunk.Metadata != nil {
			maps.Copy(labels, chunk.Metadata.TargetIDMapping)
		}
	}

	byLabel := make(map[string]int32, len(labels))
	reverse := make(map[int32][]int32)

	for id, dependencies := range targets {
		label := labels[id]
		if label == "" {
			return nil, fmt.Errorf("target ID %d has no label mapping", id)
		}
		byLabel[label] = id
		for _, dependency := range dependencies {
			reverse[dependency] = append(reverse[dependency], id)
		}
	}

	queue := make([]int32, 0, len(requested))
	seen := make(map[int32]struct{}, len(requested))

	for _, label := range requested {
		id, ok := byLabel[label]
		if !ok {
			return nil, fmt.Errorf("target %q is absent from target graph", label)
		}
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			queue = append(queue, id)
		}
	}

	for head := 0; head < len(queue); head++ {
		if head%cancelCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return nil, context.Cause(ctx)
			}
		}
		for _, dependent := range reverse[queue[head]] {
			if _, ok := seen[dependent]; !ok {
				seen[dependent] = struct{}{}
				queue = append(queue, dependent)
			}
		}
	}

	result := make([]string, 0, len(queue))
	for _, id := range queue {
		result = append(result, labels[id])
	}
	sort.Strings(result)
	return result, nil
}

func sendDependentTargets(stream pb.TangoServiceGetDependentTargetsYARPCServer, labels []string, maxBytes int) error {
	response := &pb.GetDependentTargetsResponse{}

	for _, label := range labels {
		response.Targets = append(response.Targets, label)
		if response.Size() > maxBytes && len(response.Targets) > 1 {
			last := response.Targets[len(response.Targets)-1]
			response.Targets = response.Targets[:len(response.Targets)-1]
			if err := stream.Send(response); err != nil {
				return err
			}
			response = &pb.GetDependentTargetsResponse{Targets: []string{last}}
		}
	}

	return stream.Send(response)
}
