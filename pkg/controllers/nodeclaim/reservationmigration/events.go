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
	"time"

	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
)

func Migrated(nodeClaim *v1.NodeClaim, reservationID string) events.Event {
	return events.Event{
		InvolvedObject: nodeClaim,
		Type:           corev1.EventTypeNormal,
		Reason:         events.ReservationMigrated,
		Message:        fmt.Sprintf("Migrated into capacity reservation %s", reservationID),
		DedupeValues:   []string{string(nodeClaim.UID), reservationID},
	}
}

func MigrationFailed(nodeClaim *v1.NodeClaim, reservationID, message string) events.Event {
	return events.Event{
		InvolvedObject: nodeClaim,
		Type:           corev1.EventTypeWarning,
		Reason:         events.ReservationMigrationFailed,
		Message:        fmt.Sprintf("Failed migrating into capacity reservation %s, %s", reservationID, message),
		DedupeValues:   []string{string(nodeClaim.UID), reservationID},
	}
}

func MigrationMismatched(nodeClaim *v1.NodeClaim, message string) events.Event {
	return events.Event{
		InvolvedObject: nodeClaim,
		Type:           corev1.EventTypeWarning,
		Reason:         events.ReservationMigrationMismatched,
		Message:        message,
		DedupeValues:   []string{string(nodeClaim.UID)},
	}
}

func MigrationNotObserved(nodeClaim *v1.NodeClaim, reservationID string) events.Event {
	return events.Event{
		InvolvedObject: nodeClaim,
		Type:           corev1.EventTypeWarning,
		Reason:         events.ReservationMigrationFailed,
		Message:        fmt.Sprintf("Cloud provider hasn't reported the instance in capacity reservation %s yet; the node stays fenced until it does, or the migration is abandoned", reservationID),
		DedupeValues:   []string{string(nodeClaim.UID), reservationID, "not-observed"},
		DedupeTimeout:  10 * time.Minute,
	}
}
