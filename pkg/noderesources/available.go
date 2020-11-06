package noderesources

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/config"
)

// Available is a score plugin that favors nodes based on their available
// resources.
type Available struct {
	handle  framework.FrameworkHandle
	mode    config.ModeType
	weights resourceToWeightMap
}

var _ = framework.ScorePlugin(&Available{})

// AvailableName is the name of the plugin used in the Registry and configurations.
const AvailableName = "NodeResourceAvailable"

// NewAvailable initializes a new plugin and returns it.
func NewAvailable(availArgs runtime.Object, h framework.FrameworkHandle) (framework.Plugin, error) {
	// Start with default values.
	mode := config.Most
	weights := defaultResourcesToWeightMap

	if availArgs != nil {
		args, ok := availArgs.(*config.NodeResourcesAvailableArgs)
		if !ok {
			return nil, fmt.Errorf("want args to be of type NodeResourcesAvailableArgs, got %T", availArgs)
		}
		if args.Mode != "" {
			mode = args.Mode
			if mode != config.Least && mode != config.Most {
				return nil, fmt.Errorf("invalid mode, got %s", mode)
			}
		}

		if len(args.Resources) > 0 {
			if err := validateResources(args.Resources); err != nil {
				return nil, err
			}

			weights = make(resourceToWeightMap)
			for _, resource := range args.Resources {
				weights[v1.ResourceName(resource.Name)] = resource.Weight
			}
		}
	}

	return &Available{
		handle:  h,
		mode:    mode,
		weights: weights,
	}, nil
}

// Name returns name of the plugin. It is used in logs, etc.
func (avail *Available) Name() string {
	return AvailableName
}

// Score invoked at the score extension point.
func (avail *Available) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	node, e := avail.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if e != nil {
		return 0, framework.NewStatus(framework.Error, "get node info", nodeName)
	}

	available := nodeAvailableResources(avail.weights, node.Allocatable, node.Requested)
	var score int64
	for resource, weight := range avail.weights {
		if available[resource] > 0 {
			score += weight * available[resource]
		}
	}

	// returns higher score for preferred node, that is:
	// if most available node resource is preferreed, score equals the value of (weighted) available resource;
	// otherwise, it is the negative.
	if avail.mode == config.Least {
		score = -score
	}

	return score, nil
}

func nodeAvailableResources(resources resourceToWeightMap, allocatable, requested *framework.Resource) resourceToWeightMap {
	available := make(resourceToWeightMap)

	for resource := range resources {
		switch resource {
		case v1.ResourceCPU:
			available[resource] = allocatable.MilliCPU - requested.MilliCPU
		case v1.ResourceMemory:
			available[resource] = allocatable.Memory - requested.Memory
		case v1.ResourceEphemeralStorage:
			available[resource] = allocatable.EphemeralStorage - requested.EphemeralStorage
		default:
			available[resource] = allocatable.ScalarResources[resource] - requested.ScalarResources[resource]
		}
	}

	return available
}

// ScoreExtensions of the Score plugin.
func (avail *Available) ScoreExtensions() framework.ScoreExtensions {
	return avail
}

// NormalizeScore invoked after scoring all nodes.
func (avail *Available) NormalizeScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	return normalizeScore(ctx, state, pod, scores)
}
