# Optional inactive-node accounting

`Suspended=True` identifies registered inactive Nodes. A node group can implement
`NodeGroupTargetSizeWithSuspended` to return false when its target excludes parked
instances. The default is true, preserving suspended-inclusive target units.
Opted-in inactive Nodes are excluded from scheduling and scale-down destinations.
Groups using the default target units without a snapshot retain legacy inputs.

Providers requiring coherent membership and fresher Kubernetes observations can
implement `NodeGroupAccounting`. Its snapshot combines target, complete instance
inventory, inactive IDs, authoritative Node observations, live inactive
reservations and unavailable target reservations. Node observations must come
from Kubernetes, not synthetic readiness. Identity mismatches are errors.
Core never modifies informer or provider-owned objects.

`UpcomingInactiveNodes` is bounded accepted inactive capacity, not an arbitrary
target deficit. It can supply upcoming capacity after partial acceptance without
manufacturing a scale-up request. Backoff still closes admission. Ordinary fresh
creation uses existing request tracking. `UnavailableTargetNodes` also covers
accepted, expired fresh capacity without instance IDs. It stays excluded from
upcoming calculations when subsequent requests arrive.

Inactive members are not unregistered or failed-creation cleanup candidates.
Active targets, including unavailable reservations, consume upper node and
resource limits. Synthetic upper-limit Nodes never enter scheduling snapshots
or satisfy resource minimums.

The provider owns operations, deadlines, failure notification, Kubernetes
condition publication, readiness freshness and cleanup receipts. Reconcile in
`CloudProvider.Refresh`, returning errors for required reads or writes. Publish
failures through the existing scale-state notifier outside provider locks and
before core considers aggregate completion. Snapshot getters must only read
immutable observations. They must not write APIs, wait for cloud operations,
reconcile state or notify observers. Unknown state returns an error; `(nil, nil)`
opts out of the coherent view. The optional target-unit interface still applies.

`NodeGroupDeletionTaintHandler` delegates existing marking and startup/error
cleanup boundaries. Only `(handled=false, err=nil)` permits legacy handling.
Owned or uncertain restrictions, including after policy removal, must not fall
through. Handled success returns a non-nil Node with unchanged name, UID and
provider identity. Core retains normal drain, PDB and eviction processing.
Providers recheck their private ownership and operation state before cloud
actuation and perform successful-reuse cleanup themselves.

This interface does not provide a suspend/resume controller or crash recovery.
Process-local evidence cannot be reconstructed from Node age or cloud power
state. A provider that loses required evidence must block unsafe actions.
