/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package reservationmigration

import (
	"fmt"
	"sort"

	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
)

// migratingLabelKeys returns the keys of the labels that differ between an on-demand node and the same node once it
// runs in a capacity reservation: the capacity type, the reservation ID, and the cloud provider's reserved capacity
// labels.
func migratingLabelKeys() []string {
	keys := sets.New(v1.CapacityTypeLabelKey).Union(cloudprovider.ReservedCapacityLabels)
	if cloudprovider.ReservationIDLabel != "" {
		keys.Insert(cloudprovider.ReservationIDLabel)
	}
	return sets.List(keys)
}

// reservedLabels returns the value every migrating label takes once the node runs in the offering's reservation, nil for
// labels the node won't carry. It returns false if the offering isn't a reservation in the node's zone that the node can
// move into, or doesn't define every migrating label.
func reservedLabels(nodeLabels map[string]string, offering *cloudprovider.Offering) (map[string]*string, bool) {
	keys := sets.New(migratingLabelKeys()...)
	values := map[string]*string{}
	for key := range keys {
		req := offering.Requirements.Get(key)
		switch {
		case req.Operator() == corev1.NodeSelectorOpIn && req.Len() == 1:
			values[key] = new(req.Values()[0])
		case req.Operator() == corev1.NodeSelectorOpDoesNotExist:
			values[key] = nil
		default:
			return nil, false
		}
	}
	if lo.FromPtr(values[v1.CapacityTypeLabelKey]) != v1.CapacityTypeReserved || lo.FromPtr(values[cloudprovider.ReservationIDLabel]) != offering.ReservationID() {
		return nil, false
	}
	// The instance doesn't move, so every other property of the offering must already hold for the node. The zone must be
	// known and match, other properties must not contradict a label the node already carries.
	zone, ok := nodeLabels[corev1.LabelTopologyZone]
	if !ok || !offering.Requirements.Get(corev1.LabelTopologyZone).Has(zone) {
		return nil, false
	}
	for key, req := range offering.Requirements {
		if keys.Has(key) {
			continue
		}
		value, ok := nodeLabels[key]
		if !ok {
			continue
		}
		if req.Operator() == corev1.NodeSelectorOpDoesNotExist || !req.Has(value) {
			return nil, false
		}
	}
	return values, true
}

// applyLabels returns a copy of the labels with the migrating labels set to the given values.
func applyLabels(current map[string]string, values map[string]*string) map[string]string {
	updated := lo.Assign(current)
	for key, value := range values {
		if value == nil {
			delete(updated, key)
		} else {
			updated[key] = *value
		}
	}
	return updated
}

// incompatibility returns why relabeling the node with the given migrating label values would break a scheduling
// decision made against its current labels, or "" if it wouldn't:
//   - the NodePool must accept the relabeled NodeClaim, otherwise it would be drifted right away,
//   - every pod bound to the node must still match it on required node affinity and node selectors,
//   - no DaemonSet may newly select the node, which could add a pod the node has no room for,
//   - no bound pod's required pod (anti-)affinity or DoNotSchedule topology spread may use a migrating label as its
//     topology key, if the node hosts the pod or a pod the term selects, since moving the node changes those domains.
func incompatibility(nodePool *v1.NodePool, nodeClaim *v1.NodeClaim, node *corev1.Node, values map[string]*string, pods []*corev1.Pod, daemonSets []*appsv1.DaemonSet, constraints []topologyConstraint) string {
	if err := scheduling.NewLabelRequirements(applyLabels(nodeClaim.Labels, values)).Compatible(scheduling.NewNodeSelectorRequirementsWithMinValues(nodePool.Spec.Template.Spec.Requirements...)); err != nil {
		return fmt.Sprintf("nodepool %q is incompatible with the reserved labels, %s", nodePool.Name, err)
	}
	relabeled := node.DeepCopy()
	relabeled.Labels = applyLabels(node.Labels, values)
	changed := sets.New[string]()
	for key := range values {
		before, hadBefore := node.Labels[key]
		after, hasAfter := relabeled.Labels[key]
		if hadBefore != hasAfter || before != after {
			changed.Insert(key)
		}
	}
	for _, pod := range pods {
		if podutils.IsTerminal(pod) {
			continue
		}
		if matches, err := nodeaffinity.GetRequiredNodeAffinity(pod).Match(relabeled); err != nil || !matches {
			return fmt.Sprintf("pod %s/%s doesn't match the reserved labels", pod.Namespace, pod.Name)
		}
	}
	for _, ds := range daemonSets {
		pod := &corev1.Pod{Spec: ds.Spec.Template.Spec}
		before, errBefore := nodeaffinity.GetRequiredNodeAffinity(pod).Match(node)
		after, errAfter := nodeaffinity.GetRequiredNodeAffinity(pod).Match(relabeled)
		if errBefore != nil || errAfter != nil || (!before && after) {
			return fmt.Sprintf("daemonset %s/%s would select the node once it is relabeled", ds.Namespace, ds.Name)
		}
	}
	for _, tc := range constraints {
		if !changed.Has(tc.topologyKey) {
			continue
		}
		if tc.nodeName == node.Name {
			return fmt.Sprintf("pod %s/%s on the node uses topology key %q", tc.namespace, tc.name, tc.topologyKey)
		}
		for _, pod := range pods {
			if !podutils.IsTerminal(pod) && tc.selects(pod) {
				return fmt.Sprintf("pod %s/%s selects pod %s/%s on the node with topology key %q", tc.namespace, tc.name, pod.Namespace, pod.Name, tc.topologyKey)
			}
		}
	}
	return ""
}

// topologyConstraint is a hard scheduling constraint of a bound pod whose topology domain is defined by a migrating
// label.
type topologyConstraint struct {
	namespace, name, nodeName string
	topologyKey               string
	// namespaces the constraint selects pods in, or nil for every namespace
	namespaces sets.Set[string]
	selector   labels.Selector
}

func (tc topologyConstraint) selects(pod *corev1.Pod) bool {
	return (tc.namespaces == nil || tc.namespaces.Has(pod.Namespace)) && tc.selector.Matches(labels.Set(pod.Labels))
}

// topologyConstraints returns the hard topology spread and pod (anti-)affinity constraints of the bound, non-terminal
// pods whose topology key is one of keys. It errs on the side of selecting too much: an unparsable or unset selector
// selects every pod, and a namespace selector selects every namespace.
func topologyConstraints(pods []*corev1.Pod, keys []string) []topologyConstraint {
	topologyKeys := sets.New(keys...)
	selector := func(ls *metav1.LabelSelector) labels.Selector {
		if ls == nil {
			return labels.Everything()
		}
		s, err := metav1.LabelSelectorAsSelector(ls)
		if err != nil {
			return labels.Everything()
		}
		return s
	}
	var constraints []topologyConstraint
	for _, pod := range pods {
		if pod.Spec.NodeName == "" || podutils.IsTerminal(pod) {
			continue
		}
		newConstraint := func(topologyKey string, namespaces sets.Set[string], ls *metav1.LabelSelector) topologyConstraint {
			return topologyConstraint{
				namespace: pod.Namespace, name: pod.Name, nodeName: pod.Spec.NodeName,
				topologyKey: topologyKey, namespaces: namespaces, selector: selector(ls),
			}
		}
		for _, tsc := range pod.Spec.TopologySpreadConstraints {
			if tsc.WhenUnsatisfiable == corev1.DoNotSchedule && topologyKeys.Has(tsc.TopologyKey) {
				constraints = append(constraints, newConstraint(tsc.TopologyKey, sets.New(pod.Namespace), tsc.LabelSelector))
			}
		}
		if pod.Spec.Affinity == nil {
			continue
		}
		var terms []corev1.PodAffinityTerm
		if pod.Spec.Affinity.PodAffinity != nil {
			terms = append(terms, pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution...)
		}
		if pod.Spec.Affinity.PodAntiAffinity != nil {
			terms = append(terms, pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution...)
		}
		for _, term := range terms {
			if !topologyKeys.Has(term.TopologyKey) {
				continue
			}
			var namespaces sets.Set[string]
			switch {
			case term.NamespaceSelector != nil:
				namespaces = nil
			case len(term.Namespaces) != 0:
				namespaces = sets.New(term.Namespaces...)
			default:
				namespaces = sets.New(pod.Namespace)
			}
			constraints = append(constraints, newConstraint(term.TopologyKey, namespaces, term.LabelSelector))
		}
	}
	sort.SliceStable(constraints, func(i, j int) bool {
		return constraints[i].namespace+"/"+constraints[i].name < constraints[j].namespace+"/"+constraints[j].name
	})
	return constraints
}
