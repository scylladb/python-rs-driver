use crate::cluster::node::PyNode;
use crate::cluster::state::PyClusterState;
use crate::enums::PyConsistency;
use crate::enums::PySerialConsistency;
use crate::errors::{LoadBalancingPolicyError, get_type_name, with_cause};
use crate::routing::PyToken;
use crate::utils::WithOriginalPyObject;
use pyo3::PyAny;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::prelude::{PyAnyMethods, PyModule, PyModuleMethods};
use pyo3::sync::MutexExt;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyIterator;
use pyo3::types::PyList;
use pyo3::types::PyString;
use pyo3::types::PyTuple;
use pyo3::{Bound, BoundObject, Py, PyResult, Python, pyclass, pymethods, pymodule};
use scylla::cluster::ClusterState;
use scylla::cluster::NodeRef;
use scylla::frame::response::result::TableSpec;
use scylla::policies::load_balancing::DefaultPolicy;
use scylla::policies::load_balancing::FallbackPlan;
use scylla::policies::load_balancing::LoadBalancingPolicy;
use scylla::policies::load_balancing::NodeIdentifier;
use scylla::policies::load_balancing::RoutingInfo;
use scylla::policies::load_balancing::SingleTargetLoadBalancingPolicy;
use scylla::routing::NodeLocationPreference;
use scylla::routing::Shard;
use scylla::routing::Token;
use scylla::statement::{Consistency, SerialConsistency};
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;
use uuid::Uuid;

/// Describes the preferred location of nodes to contact when executing requests.
#[pyclass(
    module = "scylla.policies.load_balancing",
    name = "NodeLocationPreference",
    frozen
)]
struct PyNodeLocationPreference {
    #[pyo3(get)]
    preferred_datacenter: Option<Py<PyString>>,
    #[pyo3(get)]
    preferred_rack: Option<Py<PyString>>,
}

#[pymethods]
impl PyNodeLocationPreference {
    #[classattr]
    #[allow(non_snake_case)]
    fn ANY(py: Python<'_>) -> Py<PyNodeLocationPreference> {
        static ANY_INSTANCE: PyOnceLock<Py<PyNodeLocationPreference>> = PyOnceLock::new();
        ANY_INSTANCE
            .get_or_init(py, || {
                Py::new(
                    py,
                    PyNodeLocationPreference {
                        preferred_datacenter: None,
                        preferred_rack: None,
                    },
                )
                .expect("Failed to create NodeLocationPreference.ANY instance")
            })
            .clone_ref(py)
    }

    #[staticmethod]
    fn datacenter(py: Python<'_>, name: Py<PyString>) -> PyResult<Py<Self>> {
        Py::new(
            py,
            Self {
                preferred_datacenter: Some(name),
                preferred_rack: None,
            },
        )
    }

    #[staticmethod]
    fn datacenter_and_rack(
        py: Python<'_>,
        datacenter_name: Py<PyString>,
        rack_name: Py<PyString>,
    ) -> PyResult<Py<Self>> {
        Py::new(
            py,
            Self {
                preferred_datacenter: Some(datacenter_name),
                preferred_rack: Some(rack_name),
            },
        )
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<Py<PyString>> {
        let repr_str = match (&self.preferred_datacenter, &self.preferred_rack) {
            (None, None) => PyString::new(py, "NodeLocationPreference.ANY"),
            (Some(dc), None) => {
                PyString::from_fmt(py, format_args!("NodeLocationPreference.datacenter({dc})"))?
            }
            (Some(dc), Some(rack)) => PyString::from_fmt(
                py,
                format_args!("NodeLocationPreference.datacenter_and_rack({dc}, {rack})"),
            )?,
            (None, Some(_)) => unreachable!("rack without datacenter is not allowed"),
        };

        Ok(repr_str.into())
    }
}

/// Python representation of routing information for a request.
/// Exposed to Python as `RoutingInfo`.
#[pyclass(
    module = "scylla.policies.load_balancing",
    frozen,
    name = "RoutingInfo"
)]
pub struct PyRoutingInfo {
    consistency: Consistency,
    serial_consistency: Option<SerialConsistency>,
    token: Option<Token>,
    ks_name: Option<String>,
    table_name: Option<String>,
    #[pyo3(get)]
    is_confirmed_lwt: bool,
    node_location_preference: NodeLocationPreference,

    // Cached Python-side representations used by the getters.
    py_consistency: PyOnceLock<Py<PyConsistency>>,
    py_serial_consistency: PyOnceLock<Py<PySerialConsistency>>,
    py_token: PyOnceLock<Py<PyToken>>,
    py_ks: PyOnceLock<Py<PyString>>,
    py_table: PyOnceLock<Py<PyString>>,
    py_preferred_datacenter: PyOnceLock<Py<PyString>>,
    py_preferred_rack: PyOnceLock<Py<PyString>>,
    py_node_location_preference: PyOnceLock<Py<PyNodeLocationPreference>>,
}

impl<'a> From<&RoutingInfo<'a>> for PyRoutingInfo {
    fn from(info: &RoutingInfo<'a>) -> Self {
        let ks_name = info.table.map(|t| t.ks_name().to_string());
        let table_name = info.table.map(|t| t.table_name().to_string());

        Self {
            consistency: info.consistency,
            serial_consistency: info.serial_consistency,
            token: info.token,
            ks_name,
            table_name,
            is_confirmed_lwt: info.is_confirmed_lwt,
            node_location_preference: info.node_location_preference.clone(),

            py_consistency: PyOnceLock::new(),
            py_serial_consistency: PyOnceLock::new(),
            py_token: PyOnceLock::new(),
            py_ks: PyOnceLock::new(),
            py_table: PyOnceLock::new(),
            py_preferred_datacenter: PyOnceLock::new(),
            py_preferred_rack: PyOnceLock::new(),
            py_node_location_preference: PyOnceLock::new(),
        }
    }
}

impl PyRoutingInfo {
    /// The table this request targets, if both halves of the name are known.
    fn table_spec(&self) -> Option<TableSpec<'_>> {
        match (&self.ks_name, &self.table_name) {
            (Some(ks), Some(table)) => Some(TableSpec::borrowed(ks.as_str(), table.as_str())),
            _ => None,
        }
    }

    fn to_routing_info<'a>(&'a self, table_spec: Option<&'a TableSpec<'a>>) -> RoutingInfo<'a> {
        RoutingInfo::new(
            self.consistency,
            self.serial_consistency,
            self.token,
            table_spec,
            self.is_confirmed_lwt,
            &self.node_location_preference,
        )
    }
}

/// Where a load balancing policy keeps its plan.
enum PlanShape {
    /// The whole plan is in `fallback`
    Fallback,
    /// The plan is the single `pick`
    Pick,
}

/// Builds `policy`'s plan for one request and collects it into a Python list.
fn plan_targets(
    py: Python<'_>,
    policy: &dyn LoadBalancingPolicy,
    shape: PlanShape,
    py_routing_info: &PyRoutingInfo,
    py_cluster_state: &PyClusterState,
) -> PyResult<Py<PyList>> {
    let table_spec = py_routing_info.table_spec();
    let routing_info = py_routing_info.to_routing_info(table_spec.as_ref());
    let cluster_state = py_cluster_state.inner.as_ref();

    match shape {
        PlanShape::Fallback => collect_targets(
            py,
            py_cluster_state,
            policy.fallback(&routing_info, cluster_state),
        ),
        PlanShape::Pick => collect_targets(
            py,
            py_cluster_state,
            policy.pick(&routing_info, cluster_state),
        ),
    }
}

/// Collects a load balancing plan into a Python list of `(Node, shard)` pairs.
fn collect_targets<'a>(
    py: Python<'_>,
    py_cluster_state: &PyClusterState,
    plan: impl IntoIterator<Item = (NodeRef<'a>, Option<Shard>)>,
) -> PyResult<Py<PyList>> {
    let list = PyList::empty(py);
    let known_nodes = py_cluster_state.known_nodes.bind(py);

    for (node, shard) in plan {
        match known_nodes.get_item(node.host_id)? {
            Some(py_node) => list.append((py_node, shard))?,
            None => log::debug!(
                "Skipping target {} in load balancing plan: not part of the cluster state",
                node.host_id
            ),
        }
    }

    Ok(list.unbind())
}

#[pymethods]
impl PyRoutingInfo {
    #[getter]
    fn consistency(&self, py: Python<'_>) -> PyResult<Py<PyConsistency>> {
        let bound_enum = self.py_consistency.get_or_try_init(py, || {
            let py_enum: PyConsistency = self.consistency.into();
            Py::new(py, py_enum)
        })?;
        Ok(bound_enum.clone_ref(py))
    }

    #[getter]
    fn serial_consistency(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.serial_consistency {
            None => Ok(py.None()),
            Some(sc) => {
                let bound_enum = self.py_serial_consistency.get_or_try_init(py, || {
                    let py_enum: PySerialConsistency = sc.into();
                    Py::new(py, py_enum)
                })?;
                Ok(bound_enum.clone_ref(py).into_any())
            }
        }
    }

    #[getter]
    fn token(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.token {
            None => Ok(py.None()),
            Some(token) => {
                let bound_token = self
                    .py_token
                    .get_or_try_init(py, || Py::new(py, PyToken::from(token)))?;
                Ok(bound_token.clone_ref(py).into_any())
            }
        }
    }

    #[getter]
    fn keyspace(&self, py: Python<'_>) -> Py<PyAny> {
        match &self.ks_name {
            None => py.None(),
            Some(ks) => {
                let bound_str = self
                    .py_ks
                    .get_or_init(py, || PyString::new(py, ks).unbind());
                bound_str.clone_ref(py).into_any()
            }
        }
    }

    #[getter]
    fn table(&self, py: Python<'_>) -> Py<PyAny> {
        match &self.table_name {
            None => py.None(),
            Some(table) => {
                let bound_str = self
                    .py_table
                    .get_or_init(py, || PyString::new(py, table).unbind());
                bound_str.clone_ref(py).into_any()
            }
        }
    }

    #[getter]
    fn preferred_datacenter(&self, py: Python<'_>) -> Py<PyAny> {
        match self.node_location_preference.datacenter() {
            None => py.None(),
            Some(dc) => {
                let bound_str = self
                    .py_preferred_datacenter
                    .get_or_init(py, || PyString::new(py, dc).unbind());
                bound_str.clone_ref(py).into_any()
            }
        }
    }

    #[getter]
    fn preferred_rack(&self, py: Python<'_>) -> Py<PyAny> {
        match self.node_location_preference.rack() {
            None => py.None(),
            Some(rack) => {
                let bound_str = self
                    .py_preferred_rack
                    .get_or_init(py, || PyString::new(py, rack).unbind());
                bound_str.clone_ref(py).into_any()
            }
        }
    }

    #[getter]
    fn node_location_preference(&self, py: Python<'_>) -> PyResult<Py<PyNodeLocationPreference>> {
        let bound_pref = self.py_node_location_preference.get_or_try_init(py, || {
            match &self.node_location_preference {
                NodeLocationPreference::Any => Ok(PyNodeLocationPreference::ANY(py)),
                NodeLocationPreference::Datacenter(dc) => {
                    let dc_py = PyString::new(py, dc).unbind();
                    PyNodeLocationPreference::datacenter(py, dc_py)
                }
                NodeLocationPreference::DatacenterAndRack(dc, rack) => {
                    let dc_py = PyString::new(py, dc).unbind();
                    let rack_py = PyString::new(py, rack).unbind();
                    PyNodeLocationPreference::datacenter_and_rack(py, dc_py, rack_py)
                }
                _ => unreachable!("We need consider this branch as it is non exhaustive"),
            }
        })?;
        Ok(bound_pref.clone_ref(py))
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<Py<PyString>> {
        let repr_str = PyString::from_fmt(
            py,
            format_args!(
                "RoutingInfo(consistency={:?}, serial_consistency={:?}, token={}, keyspace={:?}, table={:?}, is_confirmed_lwt={})",
                self.consistency,
                self.serial_consistency,
                self.token(py)?.bind(py).repr()?,
                self.ks_name,
                self.table_name,
                self.is_confirmed_lwt
            ),
        )?;

        Ok(repr_str.into())
    }
}

/// The load balancing policy that pins a request to a single target: one node,
/// and optionally one shard on that node.
pub(crate) struct PyTargetPolicy(Arc<dyn LoadBalancingPolicy>);

impl PyTargetPolicy {
    pub(crate) fn into_inner(self) -> Arc<dyn LoadBalancingPolicy> {
        self.0
    }
}

impl<'py> FromPyObject<'_, 'py> for PyTargetPolicy {
    type Error = TargetConversionError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let (node, shard) = if let Ok(tuple) = obj.cast::<PyTuple>() {
            if tuple.len() != 2 {
                return Err(TargetConversionError::InvalidTupleLen { len: tuple.len() });
            }

            // The shard is the only part that can still fail to extract.
            tuple
                .extract::<(Bound<'py, PyAny>, Option<u32>)>()
                .map_err(TargetConversionError::invalid_shard_type)?
        } else {
            (obj.to_owned(), None)
        };

        let node_identifier = if let Ok(py_node) = node.cast::<PyNode>() {
            NodeIdentifier::Node(Arc::clone(&py_node.get().inner))
        } else if let Ok(host_id) = node.extract::<Uuid>() {
            NodeIdentifier::HostId(host_id)
        } else {
            return Err(TargetConversionError::invalid_node(node.as_borrowed()));
        };

        Ok(Self(SingleTargetLoadBalancingPolicy::new(
            node_identifier,
            shard,
        )))
    }
}

/// Built-in load balancing policy that pins every request to a single target,
/// equivalent to the Rust driver's `SingleTargetLoadBalancingPolicy`.
#[derive(Debug)]
#[pyclass(
    module = "scylla.policies.load_balancing",
    name = "SingleTargetPolicy",
    frozen
)]
struct PySingleTargetPolicy {
    inner: Arc<dyn LoadBalancingPolicy>,

    #[pyo3(get)]
    target: Py<PyAny>,
}

#[pymethods]
impl PySingleTargetPolicy {
    #[new]
    #[pyo3(signature = (target, /))]
    fn new(target: WithOriginalPyObject<PyTargetPolicy>) -> Self {
        Self {
            inner: target.extracted.into_inner(),
            target: target.original,
        }
    }

    fn pick_targets(
        &self,
        py: Python<'_>,
        py_routing_info: Py<PyRoutingInfo>,
        py_cluster_state: Py<PyClusterState>,
    ) -> PyResult<Py<PyList>> {
        plan_targets(
            py,
            self.inner.as_ref(),
            PlanShape::Pick,
            py_routing_info.get(),
            py_cluster_state.get(),
        )
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<Py<PyString>> {
        let target = self.target.bind(py).repr()?;
        let repr_str = PyString::from_fmt(py, format_args!("SingleTargetPolicy({target})"))?;

        Ok(repr_str.into())
    }
}

/// Python representation of structures implementing the load balancing trait.
#[derive(Clone)]
pub(crate) struct PyLoadBalancingPolicy {
    pub(crate) inner: Arc<dyn LoadBalancingPolicy>,
}

impl PyLoadBalancingPolicy {
    pub(crate) fn into_inner(self) -> Arc<dyn LoadBalancingPolicy> {
        self.inner
    }
}

impl<'py> FromPyObject<'_, 'py> for PyLoadBalancingPolicy {
    type Error = DriverLoadBalancingPolicyError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(default) = obj.cast::<PyDefaultPolicy>() {
            return Ok(Self {
                inner: default.get().inner.clone(),
            });
        }

        if let Ok(single_target) = obj.cast::<PySingleTargetPolicy>() {
            return Ok(Self {
                inner: single_target.get().inner.clone(),
            });
        }

        if obj
            .hasattr(intern!(obj.py(), "pick_targets"))
            .unwrap_or(false)
        {
            return Ok(Self {
                inner: Arc::new(CustomLoadBalancingPolicy {
                    inner: obj.unbind(),
                    cluster_cache: Mutex::new(None),
                }),
            });
        }

        Err(DriverLoadBalancingPolicyError::invalid_policy(obj))
    }
}

/// Represents a custom load balancing policy object implemented by the Python user.
#[derive(Debug)]
struct CustomLoadBalancingPolicy {
    inner: Py<PyAny>,
    cluster_cache: Mutex<Option<(Arc<ClusterState>, Py<PyClusterState>)>>,
}

impl CustomLoadBalancingPolicy {
    fn get_cluster_state(
        &self,
        py: Python,
        cluster: &ClusterState,
    ) -> Result<Py<PyClusterState>, PyErr> {
        let incoming_ptr = cluster as *const ClusterState;
        let mut cache = self.cluster_cache.lock_py_attached(py).unwrap();

        if let Some((cached_arc, py_cache)) = &*cache
            && std::ptr::eq(Arc::as_ptr(cached_arc), incoming_ptr)
        {
            return Ok(py_cache.clone_ref(py));
        };

        // SAFETY:, HACK:
        // &'a cluster::ClusterState comes from `Arc::deref`, so it's always in an Arc.
        // Claiming exactly 1 strong reference lets us "clone" the Arc through the raw pointer.
        // This lets us invalidate cache entries when pointers don't match.
        // This approach is sound due to PyLoadBalancingPolicy keeping the ClusterState alive
        // thus preventing ABA problem and guaranteeing that:
        // ClusterState changes if and only if the pointer changes.
        let new_arc = unsafe {
            Arc::increment_strong_count(incoming_ptr);
            Arc::from_raw(incoming_ptr)
        };

        let new_py_cluster_state = Py::new(py, PyClusterState::try_from(Arc::clone(&new_arc))?)?;
        *cache = Some((new_arc, new_py_cluster_state.clone_ref(py)));

        Ok(new_py_cluster_state)
    }
}

impl LoadBalancingPolicy for CustomLoadBalancingPolicy {
    fn pick<'a>(
        &'a self,
        _request: &'a RoutingInfo,
        _cluster: &'a ClusterState,
    ) -> Option<(NodeRef<'a>, Option<Shard>)> {
        None
    }

    fn fallback<'a>(
        &'a self,
        request: &'a RoutingInfo,
        cluster: &'a ClusterState,
    ) -> FallbackPlan<'a> {
        type FallbackPlanHead<'a> = (Option<(NodeRef<'a>, Option<Shard>)>, Py<PyIterator>);

        let py_result = Python::attach(|py| -> PyResult<FallbackPlanHead<'a>> {
            let py_request = PyRoutingInfo::from(request);

            let py_cluster_state = self
                .get_cluster_state(py, cluster)
                .inspect_err(|err| log::error!("Error occurred on Python side: {}", err))?;

            let py_policy = self.inner.bind(py);

            let targets_iterable = py_policy
                .call_method1(intern!(py, "pick_targets"), (py_request, py_cluster_state))
                .inspect_err(|err| {
                    log::error!(
                        "Failed to call 'pick_targets' method on LoadBalancing Policy: {}",
                        err
                    );
                })?;

            let mut targets_iter = targets_iterable.try_iter().inspect_err(|err| {
                log::error!(
                    "The value returned by 'pick_targets' is not iterable: {}",
                    err
                );
            })?;

            // Eagerly extract the first target while we already hold the GIL. We will need at least the first
            // target returned by the iterator so it is not a waste to do it earlier.
            let first = PyTargetsIter::extract_next_target(&mut targets_iter, cluster);

            Ok((first, targets_iter.unbind()))
        });

        match py_result {
            Ok((Some(first), py_iter)) => Box::new(PyTargetsIter {
                first: Some(first),
                py_iter,
                exhausted: false,
                cluster,
            }),
            _ => {
                let empty_iter = std::iter::empty::<(NodeRef<'a>, Option<Shard>)>();
                Box::new(empty_iter)
            }
        }
    }

    fn name(&self) -> String {
        Python::attach(|py| {
            self.inner
                .bind(py)
                .get_type()
                .name()
                .map(|name| name.to_string())
                .unwrap_or_else(|_| "Unknown Load Balancing Policy".to_string())
        })
    }
}

struct PyTargetsIter<'a> {
    first: Option<(NodeRef<'a>, Option<Shard>)>,
    py_iter: Py<PyIterator>,
    exhausted: bool,
    cluster: &'a ClusterState,
}

impl<'a> PyTargetsIter<'a> {
    /// Extract the next target from a Python iterator while the GIL is held.
    fn extract_next_target(
        iter: &mut Bound<'_, PyIterator>,
        cluster: &'a ClusterState,
    ) -> Option<(NodeRef<'a>, Option<Shard>)> {
        let item = iter.next()?;

        let item = item
            .inspect_err(|err| {
                log::error!("Failed to iterate over 'pick_targets' result: {}", err);
            })
            .ok()?;

        let (py_node, shard) = item
            .extract::<(Py<PyNode>, Option<Shard>)>()
            .inspect_err(|err| {
                log::error!(
                    "Failed to extract NodeShard from 'pick_targets' iterator: {}",
                    err
                );
            })
            .ok()?;

        let id = py_node.get().inner.host_id;
        let node = cluster.get_node_by_host_id(id).or_else(|| {
            log::error!(
                "Failed to retrieve node with host_id: {}, stopping iteration",
                id
            );
            None
        })?;

        Some((node, shard))
    }
}

/// Lazily acquires the GIL for each `next()`.
/// On error, logs and exhausts the iterator.
/// This means the first `next()` error will log and then the plan becomes empty.
impl<'a> Iterator for PyTargetsIter<'a> {
    type Item = (NodeRef<'a>, Option<Shard>);

    fn next(&mut self) -> Option<Self::Item> {
        // Return the eagerly-extracted first target before going lazy.
        if let Some(first) = self.first.take() {
            return Some(first);
        }

        if self.exhausted {
            return None;
        }

        Python::attach(|py| -> Option<(NodeRef<'a>, Option<Shard>)> {
            let mut py_iter = self.py_iter.bind(py).clone();

            let result = Self::extract_next_target(&mut py_iter, self.cluster);
            if result.is_none() {
                self.exhausted = true;
            }
            result
        })
    }
}

/// Built-in load balancing policy, equivalent to the Rust driver's DefaultPolicy.
#[derive(Debug)]
#[pyclass(
    module = "scylla.policies.load_balancing",
    name = "DefaultPolicy",
    frozen
)]
struct PyDefaultPolicy {
    inner: Arc<dyn LoadBalancingPolicy>,
    #[pyo3(get)]
    token_aware: bool,
    #[pyo3(get)]
    permit_dc_failover: bool,
    #[pyo3(get)]
    enable_shuffling_replicas: bool,
    #[pyo3(get)]
    node_location_preference: Option<Py<PyNodeLocationPreference>>,
}

#[pymethods]
impl PyDefaultPolicy {
    #[new]
    #[pyo3(signature = (*,
        node_location_preference = None,
        token_aware = true,
        permit_dc_failover = false,
        enable_shuffling_replicas = true,
    ))]
    fn new(
        py: Python<'_>,
        node_location_preference: Option<Py<PyNodeLocationPreference>>,
        token_aware: bool,
        permit_dc_failover: bool,
        enable_shuffling_replicas: bool,
    ) -> Result<Self, DriverLoadBalancingPolicyError> {
        let mut builder = DefaultPolicy::builder();

        builder = builder
            .enable_shuffling_replicas(enable_shuffling_replicas)
            .permit_dc_failover(permit_dc_failover)
            .token_aware(token_aware);

        if let Some(ref pref) = node_location_preference {
            let pref = pref.get();
            match (&pref.preferred_datacenter, &pref.preferred_rack) {
                (Some(dc), Some(rack)) => {
                    let dc_str = dc
                        .bind(py)
                        .to_str()
                        .map_err(
                            DriverLoadBalancingPolicyError::default_policy_string_conversion_failed,
                        )?
                        .to_string();
                    let rack_str = rack
                        .bind(py)
                        .to_str()
                        .map_err(
                            DriverLoadBalancingPolicyError::default_policy_string_conversion_failed,
                        )?
                        .to_string();
                    builder = builder.prefer_datacenter_and_rack(dc_str, rack_str);
                }
                (Some(dc), None) => {
                    let dc_str = dc
                        .bind(py)
                        .to_str()
                        .map_err(
                            DriverLoadBalancingPolicyError::default_policy_string_conversion_failed,
                        )?
                        .to_string();
                    builder = builder.prefer_datacenter(dc_str);
                }
                (None, None) => {
                    builder = builder.prefer_no_datacenter();
                }
                (None, Some(_)) => unreachable!("rack without datacenter is not allowed"),
            }
        }

        Ok(Self {
            inner: builder.build(),
            node_location_preference,
            token_aware,
            permit_dc_failover,
            enable_shuffling_replicas,
        })
    }

    /// The preferred datacenter, or None. Convenience getter.
    #[getter]
    fn preferred_datacenter(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_location_preference
            .as_ref()
            .and_then(|pref| pref.get().preferred_datacenter.as_ref())
            .map_or_else(|| py.None(), |dc| dc.clone_ref(py).into_any())
    }

    /// The preferred rack, or None. Convenience getter.
    #[getter]
    fn preferred_rack(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_location_preference
            .as_ref()
            .and_then(|pref| pref.get().preferred_rack.as_ref())
            .map_or_else(|| py.None(), |rack| rack.clone_ref(py).into_any())
    }

    fn pick_targets(
        &self,
        py: Python,
        py_routing_info: Py<PyRoutingInfo>,
        py_cluster_state: Py<PyClusterState>,
    ) -> PyResult<Py<PyList>> {
        plan_targets(
            py,
            self.inner.as_ref(),
            PlanShape::Fallback,
            py_routing_info.get(),
            py_cluster_state.get(),
        )
    }
}

#[pymodule]
pub(crate) fn load_balancing(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyNodeLocationPreference>()?;
    module.add_class::<PyDefaultPolicy>()?;
    module.add_class::<PySingleTargetPolicy>()?;
    module.add_class::<PyRoutingInfo>()?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
#[must_use]
pub(crate) enum DriverLoadBalancingPolicyError {
    #[error(
        "Invalid load balancing policy '{type_name}': Object does not implement the \
         LoadBalancingPolicy protocol (missing required 'pick_targets' method)."
    )]
    InvalidPolicy { type_name: String },
    #[error("String conversion failed while creating default load balancing policy")]
    DefaultPolicyStringConversionFailed { source: Box<PyErr> },
}

impl DriverLoadBalancingPolicyError {
    pub(crate) fn invalid_policy(obj: Borrowed<PyAny>) -> Self {
        Self::InvalidPolicy {
            type_name: get_type_name(obj),
        }
    }

    pub(crate) fn default_policy_string_conversion_failed(source: PyErr) -> Self {
        Self::DefaultPolicyStringConversionFailed {
            source: Box::new(source),
        }
    }
}

impl From<DriverLoadBalancingPolicyError> for PyErr {
    fn from(e: DriverLoadBalancingPolicyError) -> PyErr {
        let err = LoadBalancingPolicyError::new_err(e.to_string());
        match e {
            DriverLoadBalancingPolicyError::InvalidPolicy { .. } => err,
            DriverLoadBalancingPolicyError::DefaultPolicyStringConversionFailed { source } => {
                with_cause(err, *source)
            }
        }
    }
}

/* Target conversion errors */

/// Errors that can occur while converting a Python request target - a `Target`.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, thiserror::Error)]
#[must_use]
pub(crate) enum TargetConversionError {
    #[error("invalid target node '{type_name}': expected a Node or a host id (uuid.UUID)")]
    InvalidNode { type_name: String },

    #[error("invalid target: expected a (node, shard) pair, got a tuple of length {len}")]
    InvalidTupleLen { len: usize },

    #[error("invalid target shard: expected an integer or None")]
    InvalidShardType { source: Box<PyErr> },
}

impl TargetConversionError {
    pub(crate) fn invalid_node(obj: Borrowed<PyAny>) -> Self {
        Self::InvalidNode {
            type_name: get_type_name(obj),
        }
    }

    pub(crate) fn invalid_shard_type(source: PyErr) -> Self {
        Self::InvalidShardType {
            source: Box::new(source),
        }
    }
}

impl From<TargetConversionError> for PyErr {
    fn from(e: TargetConversionError) -> PyErr {
        let err = pyo3::exceptions::PyValueError::new_err(e.to_string());
        match e {
            TargetConversionError::InvalidShardType { source } => with_cause(err, *source),
            _ => err,
        }
    }
}
