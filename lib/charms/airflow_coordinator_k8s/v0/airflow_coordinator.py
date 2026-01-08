"""Library to manage the relation provided by the Airflow Coordinator charm.

This library contains the Requires and Provides classes for handling the relation
between the Airflow Coordinator charm and the Airflow core charms (Scheduler,
API Server, Triggerer, DAG Processor).

Since the Coordinator is expected to share sensitive data with related Airflow
core charms, it is essential to prevent storing this data in plaintext in the
relation databag. Thus, the library abstracts the transparent storage and
retrieval of fields in pydantic models in the databag if plaintext, and in a
juju secret if sensitive. We expand upon the implementation approach established
in data_interfaces v1.

### Requirer Charm

The following presents an example usage of the AirflowCoordinatorRequires class:

```python
import charms.airflow_coordinator_k8s.v0.airflow_coordinator as airflow_coordinator

class AirflowCoreCharm(ops.CharmBase):
    def __init__(self, *args) -> None:
        super().__init__(*args)

        self.requirer = airflow_coordinator.AirflowCoordinatorRequires(
            self,
            "airflow-coordinator", # relation endpoint
            component="scheduler", # the component this charm represents
            workload_container=self.unit.get_container("scheduler"),
            callback=self.reconcile,
        )

    def reconcile(self, event) -> None:
        # Determine current state of charm, what it should be, and how to get there
```

The AirflowCoordinatorRequires surfaces the following:
1. `ready`: indicates whether the relation is ready and config available in databag
2. `airflow_core_validation_failures`: all Airflow Core charm validation failures in the cluster.
3. `validation_failure_messages`: all validation failures for this charm
4. `missing_core_components_exist`: if any core charms are missing in the cluster
5. `can_write_airflow_config`: all prerequisites met to be able to render and
write airflow config file
6. `write_airflow_config(filepath)`: renders and writes the airflow config in
the workload container
7. `can_write_kubernetes_executor_pod_spec`: all prerequisites met to be able
to render and write the k8s executor pod spec
8. `write_kubernetes_pod_spec(filepath)`: renders and write the k8s executor
pod spec in the workload container

The AirflowCoordinatorRequires will invoke the provided `callback` when:
- the coordinator charm shares validation failures for all related core charms
- the coordinator charm first shares the airflow config and/or k8s executor
pod spec files
- the coordinator charm updates anything that affects the airflow config and/or
k8s executor pod spec files
- the relation with the coordinator charm is broken

### Provider Charm

The following presents an example usage of the AirflowCoordinatorProvides class:

```python
import charms.airflow_coordinator_k8s.v0.airflow_coordinator as airflow_coordinator

class AirflowCoordinatorCharm(ops.CharmBase):
    def __init__(self, *args) -> None:
        super().__init__(*args)

        self.requirer = airflow_coordinator.AirflowCoordinatorProvides(
            self,
            "airflow-coordinator", # relation endpoint
            callback=self.reconcile,
        )

    def reconcile(self, event) -> None:
        # Determine current state of charm, what it should be, and how to get there
```

The AirflowCoordinatorProvides surfaces the following:
1. `missing_core_components`: a set of missing core charms that need to be added
to the cluster
2. `airflow_version_with_max_count`: the airflow version with the max count
amongst the related core charms
3. `workload_image_hash_with_max_count`: the workload image hash with the max
count amongst the related core charms
4. `are_airflow_versions_consistent`: whether airflow versions consistent amongst
all required related core charms
4. `are_workload_image_hashes_consistent`: whether workload image hashes are
consistent amongst all required related core charms
5. `set_validation_errors()`: set any validation errors in databags of all
related core charms
6. `set_airflow_config()`: set the airflow config, k8s executor pod spec if
available, and sensitive data in databag + juju secret to share with all related
core charms

The AirflowCoordinatorProvides will invoke the provided `callback` when:
- a related core charm shares its metadata (including airflow version and workload
image hash)
- the relation with the core charm is broken
"""

import collections
import enum
import json
import logging
import pickle
import typing

import charms.data_platform_libs.v1.data_interfaces as data_interfaces
import jinja2
import ops
import pydantic
import typing_extensions

# The unique Charmhub library identifier, never change it
LIBID = "0a9814b72add4c5c85ca9eef647ab491"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1

# TODO: add your code here! Happy coding!

logger = logging.getLogger(__name__)


class AirflowCoreComponentEnum(str, enum.Enum):
    """Enum to encapsulate the possible Airflow core component options."""

    SCHEDULER = "scheduler"
    API_SERVER = "api-server"
    TRIGGERER = "triggerer"
    DAG_PROCESSOR = "dag-processor"


class AirflowCoreValidationErrorEnum(str, enum.Enum):
    """Enum to encapsulate the possible validation error codes."""

    MISSING_COMPONENT = "missing_component"
    INCONSISTENT_AIRFLOW_VERSION = "inconsistent_airflow_version"
    INCONSISTENT_WORKLOAD_IMAGE_HASH = "inconsistent_workload_image_hash"


METADATA_VALIDATION_ERROR_CODE_TO_MESSAGE = {
    AirflowCoreValidationErrorEnum.MISSING_COMPONENT: "Required component is missing in the cluster",  # noqa: E501
    AirflowCoreValidationErrorEnum.INCONSISTENT_AIRFLOW_VERSION: "Component has an airflow version inconsistent with the cluster",  # noqa: E501
    AirflowCoreValidationErrorEnum.INCONSISTENT_WORKLOAD_IMAGE_HASH: "Component has a workload image hash that is inconsistent with the cluster",  # noqa: E501
}


class MetadataValidationError(pydantic.BaseModel):
    """Represents a failed validation for core component."""

    component: AirflowCoreComponentEnum
    code: AirflowCoreValidationErrorEnum


class AirflowCoordinatorRequirerModel(data_interfaces.BaseCommonModel):
    """Requirer side of the Airflow Coordinator model."""

    airflow_version: str
    workload_image_hash: str
    component: AirflowCoreComponentEnum

    # hack to enable databag diff computation with data_interfaces v1 charm lib
    request_id: str = pydantic.Field(default="fixed_request_id", exclude=True)


SensitiveDataSecretStr = typing.Annotated[
    data_interfaces.OptionalSecretStr, pydantic.Field(exclude=True, default=None), "sensitive-data"
]


class AirflowCoordinatorProviderModel(data_interfaces.BaseCommonModel):
    """Provider side of the Airflow Coordinator model."""

    config_template: str | None = pydantic.Field(default=None)
    kubernetes_executor_pod_spec: str | None = pydantic.Field(default=None)
    sensitive_data: SensitiveDataSecretStr = pydantic.Field(default=None)
    secret_sensitive_data: data_interfaces.SecretString = pydantic.Field(default=None)

    validation_failures: str | None = pydantic.Field(default=None)

    # hack to enable databag diff computation with data_interfaces v1 charm lib
    request_id: str = pydantic.Field(default="fixed_request_id", exclude=True)

    @pydantic.field_validator("validation_failures", mode="before")
    @classmethod
    def validate_validation_failures(
        cls, validation_failures: str | list[dict[str, typing.Any]] | None
    ) -> str:
        """Validator for validation_failures, ensure conversion to string."""
        # data_interfaces.RepositoryInterface.build_model uses json.loads on all
        # fields, meaning the field can be a list of dicts instead of string
        if isinstance(validation_failures, list):
            return json.dumps(validation_failures)

        return validation_failures


TAirflowCoordinatorRequirerModel = typing.TypeVar(
    "TAirflowCoordinatorRequirerModel", bound=AirflowCoordinatorRequirerModel
)
TAirflowCoordinatorProviderModel = typing.TypeVar(
    "TAirflowCoordinatorProviderModel", bound=AirflowCoordinatorProviderModel
)
TAirflowCoordinatorModels = typing.TypeVar(
    "TAirflowCoordinatorModels",
    bound=typing.Union[AirflowCoordinatorRequirerModel, AirflowCoordinatorProviderModel],
)


class AirflowCoordinatorEvent(ops.EventBase, typing.Generic[TAirflowCoordinatorModels]):
    """Airflow config related event."""

    def __init__(
        self,
        handle: ops.Handle,
        relation: ops.Relation,
        app: ops.Application | None,
        unit: ops.Unit | None,
        content: TAirflowCoordinatorModels,
    ):
        super().__init__(handle)
        self.relation = relation
        self.app = app
        self.unit = unit
        self.content = content

    def snapshot(self) -> dict[str, typing.Any]:
        """Save event information."""
        snapshot = {
            "relation_name": self.relation.name,
            "relation_id": self.relation.id,
        }

        if self.app:
            snapshot["app_name"] = self.app.name
        if self.unit:
            snapshot["unit_name"] = self.unit.name

        # Easier to pickle than disect content marshalling. The snapshot dictionary
        # is pickled by ops anyhow.
        snapshot["content"] = pickle.dumps(self.content)

        return snapshot

    def restore(self, snapshot: dict[str, typing.Any]):
        """Restore event information."""
        relation = self.framework.model.get_relation(
            snapshot["relation_name"], snapshot["relation_id"]
        )
        if not relation:
            raise ValueError("Missing relation")

        self.relation = relation

        app_name = snapshot.get("app_name")
        self.app = self.framework.model.get_app(app_name) if app_name else None

        unit_name = snapshot.get("unit_name")
        self.unit = self.framework.model.get_unit(unit_name) if unit_name else None

        self.content = pickle.loads(snapshot["content"])


class AirflowConfigAvailableEvent(AirflowCoordinatorEvent[TAirflowCoordinatorProviderModel]):
    """Event emitted when the Airflow config is available."""


class AirflowConfigUpdatedEvent(AirflowCoordinatorEvent[TAirflowCoordinatorProviderModel]):
    """Event emitted when the Airflow config is updated."""


class AirflowCoreMetadataValidationFailed(
    AirflowCoordinatorEvent[TAirflowCoordinatorProviderModel]
):
    """Event emitted when an Airflow core charm's metadata validation fails."""


class AirflowCoordinatorRequiresEvents(
    ops.CharmEvents, typing.Generic[TAirflowCoordinatorProviderModel]
):
    """Events that Airflow core charms can emit."""

    airflow_config_available = ops.EventSource(AirflowConfigAvailableEvent)
    airflow_config_updated = ops.EventSource(AirflowConfigUpdatedEvent)
    airflow_core_metadata_validation_failed = ops.EventSource(AirflowCoreMetadataValidationFailed)


class AirflowCoreMetadataAvailableEvent(AirflowCoordinatorEvent[TAirflowCoordinatorRequirerModel]):
    """Event emitted when an Airflow core charm shares its metadata with the Coordinator."""


class AirflowCoordinatorProvidesEvents(
    ops.CharmEvents, typing.Generic[TAirflowCoordinatorRequirerModel]
):
    """Events that Airflow Coordinator provider can emit."""

    airflow_core_metadata_available = ops.EventSource(AirflowCoreMetadataAvailableEvent)


class AirflowCoordinatorRequirerEventHandler(
    data_interfaces.EventHandlers, typing.Generic[TAirflowCoordinatorRequirerModel]
):
    """Event Handler for Airflow Coordinator requirer."""

    on = AirflowCoordinatorRequiresEvents[TAirflowCoordinatorProviderModel]()

    def __init__(
        self,
        charm: ops.CharmBase,
        relation_name: str,
        request_model: type[TAirflowCoordinatorProviderModel],
        unique_key: str = "",
    ):
        """Builds an Airflow Coordinator requirer event handler."""
        super().__init__(charm, relation_name, unique_key)
        self.charm = charm
        self.component = self.charm.app
        self.request_model = request_model
        self.interface = data_interfaces.OpsRelationRepositoryInterface(
            charm.model, relation_name, request_model
        )

        self.relation = self.charm.model.get_relation(relation_name)
        self.repository = (
            data_interfaces.OpsRelationRepository(
                self.model, self.relation, component=self.relation.app
            )
            if self.relation
            else None
        )

    def _dispatch_events(
        self,
        event: ops.RelationEvent,
        _diff: data_interfaces.Diff,
        content: AirflowCoordinatorProviderModel,
    ):
        if "validation-failures" in _diff.added or "validation-failures" in _diff.changed:
            getattr(self.on, "airflow_core_metadata_validation_failed").emit(
                event.relation, app=event.app, unit=event.unit, content=content
            )
            return

        if "config-template" in _diff.added:
            getattr(self.on, "airflow_config_available").emit(
                event.relation, app=event.app, unit=event.unit, content=content
            )
            return

        if (
            "config-template" in _diff.changed
            or "kubernetes-executor-pod-spec" in _diff.changed
            or "sensitive_data" in _diff.changed
        ):
            getattr(self.on, "airflow_config_updated").emit(
                event.relation, app=event.app, unit=event.unit, content=content
            )

    @typing_extensions.override
    def _handle_event(
        self,
        event: ops.RelationChangedEvent,
        repository: data_interfaces.AbstractRepository,
        content: AirflowCoordinatorProviderModel,
    ):
        _diff = self.compute_diff(event.relation, content, repository)

        self._dispatch_events(event, _diff, content)

    @typing_extensions.override
    def _on_secret_changed_event(self, event: ops.SecretChangedEvent) -> None:
        if not event.secret.label:
            return

        relation = self._relation_from_secret_label(event.secret.label)
        short_uuid = self._short_uuid_from_secret_label(event.secret.label)

        if not short_uuid:
            return

        if not relation:
            logging.warning(
                f"Received secret {event.secret.label} but couldn't parse, seems irrelevant"
            )
            return

        if relation.name != self.relation_name:
            logging.warning("Secret changed on wrong relation")
            return

        try:
            event.secret.get_info()
            logging.warning("Secret changed event ignored for Secret Owner")
            return
        except ops.SecretNotFoundError:
            pass

        remote_unit = self.get_remote_unit(relation)

        try:
            content = self.interface.build_model(
                self.relation.id, AirflowCoordinatorProviderModel, component=self.relation.app
            )
        except pydantic.ValidationError as e:
            logger.warning(f"Invalid relation contents from the coordinator charm: {e}")
            return

        getattr(self.on, "airflow_config_updated").emit(
            relation,
            app=relation.app,
            unit=remote_unit,
            content=content,
        )

    @typing_extensions.override
    def _on_relation_changed_event(self, event: ops.RelationChangedEvent) -> None:
        if not self.charm.unit.is_leader():
            return

        repository = data_interfaces.OpsRelationRepository(
            self.model, event.relation, component=event.relation.app
        )

        try:
            content = self.interface.build_model(
                self.relation.id, AirflowCoordinatorProviderModel, component=self.relation.app
            )
        except pydantic.ValidationError as e:
            logger.warning(f"Invalid relation contents from the coordinator charm: {e}")
            return

        self._handle_event(event, repository, content)

    def set_metadata(self, metadata: AirflowCoordinatorRequirerModel):
        """Set charm metadaate to share with related Airflow Coordinator charm."""
        if not self.charm.unit.is_leader():
            return

        relation = self.charm.model.get_relation(self.relation_name)
        if not relation:
            raise ValueError("Missing relation")

        self.interface.write_model(relation.id, metadata)

    @property
    def provider_content(self) -> typing.Optional[AirflowCoordinatorProviderModel]:
        """Data from the related Airflow Coordinator charm."""
        try:
            return self.interface.build_model(
                self.relation.id, AirflowCoordinatorProviderModel, component=self.relation.app
            )
        except pydantic.ValidationError:
            return None

    @property
    def validation_failures(self) -> list[MetadataValidationError]:
        """Validation failures from the related Airflow Coordinator charm."""
        return (
            [
                MetadataValidationError(**failure)
                for failure in json.loads(self.provider_content.validation_failures)
            ]
            if self.provider_content and self.provider_content.validation_failures
            else []
        )


class AirflowCoordinatorProviderEventHandler(
    data_interfaces.EventHandlers, typing.Generic[TAirflowCoordinatorProviderModel]
):
    """Event Handler for Airflow Coordinator provider."""

    on = AirflowCoordinatorProvidesEvents[TAirflowCoordinatorRequirerModel]()

    def __init__(
        self,
        charm: ops.CharmBase,
        relation_name: str,
        request_model: type[TAirflowCoordinatorRequirerModel],
        unique_key: str = "",
    ):
        super().__init__(charm, relation_name, unique_key)
        self.component = self.charm.app
        self.request_model = request_model
        self.interface = data_interfaces.OpsRelationRepositoryInterface(
            charm.model, relation_name, request_model
        )

        self.relation = (
            charm.model.relations[relation_name][0]
            if charm.model.relations[relation_name]
            else None
        )

    def _dispatch_events(
        self,
        event: ops.RelationEvent,
        _diff: data_interfaces.Diff,
        content: AirflowCoordinatorRequirerModel,
    ):
        if (
            "airflow-version" in _diff.added
            or "workload-image-hash" in _diff.added
            or "component" in _diff.added
        ):
            if (
                not content.airflow_version
                or not content.workload_image_hash
                or not content.component
            ):
                return

            getattr(self.on, "airflow_core_metadata_available").emit(
                event.relation,
                app=event.app,
                unit=event.unit,
                content=content,
            )

    @typing_extensions.override
    def _handle_event(
        self,
        event: ops.RelationChangedEvent,
        repository: data_interfaces.AbstractRepository,
        content: AirflowCoordinatorRequirerModel,
    ):
        _diff = self.compute_diff(event.relation, content, repository)

        self._dispatch_events(event, _diff, content)

    @typing_extensions.override
    def _on_relation_changed_event(self, event: ops.RelationChangedEvent) -> None:
        if not self.charm.unit.is_leader():
            return

        repository = self.interface.repository(event.relation.id, event.relation.app)

        # Don't do anything until we get some data
        if not repository.get_data():
            return

        try:
            content = self.interface.build_model(
                event.relation.id, AirflowCoordinatorRequirerModel, component=event.relation.app
            )
        except pydantic.ValidationError as e:
            logger.warning(f"Invalid relation contents from a core charm: {e}")
            return

        self._handle_event(event, repository, content)

    @typing_extensions.override
    def _on_secret_changed_event(self, _: ops.SecretChangedEvent) -> None:
        pass

    def update_content(  # noqa: C901
        self,
        config_template: str = None,
        kubernetes_executor_pod_spec: str = None,
        sensitive_data: dict[str, str] = {},
    ):
        """Update data to send to related core charms."""
        if not self.interface.relations:
            return

        if not all([config_template, sensitive_data]):
            return

        if not self.charm.unit.is_leader():
            return

        for relation in self.interface.relations:
            model = None

            if self.interface.repository(relation.id, self.charm.app).get_data():
                try:
                    model = self.interface.build_model(
                        relation.id, AirflowCoordinatorProviderModel, component=self.charm.app
                    )

                    if config_template:
                        model.config_template = config_template

                    if kubernetes_executor_pod_spec:
                        model.kubernetes_executor_pod_spec = kubernetes_executor_pod_spec

                    if sensitive_data:
                        model.sensitive_data = json.dumps(sensitive_data)

                    model.validation_failures = None
                except pydantic.ValidationError:
                    pass

            if not model:
                model = AirflowCoordinatorProviderModel(
                    config_template=config_template,
                    kubernetes_executor_pod_spec=kubernetes_executor_pod_spec,
                    sensitive_data=json.dumps(sensitive_data),
                )

            self.interface.write_model(relation.id, model)

    def set_validation_errors(self, failures: list[MetadataValidationError]) -> None:
        """Update validation errors to send to related core charms."""
        if not self.interface.relations:
            return

        if not self.charm.unit.is_leader():
            return

        failures_serialized = json.dumps([failure.model_dump() for failure in failures])

        for relation in self.interface.relations:
            model = None

            if self.interface.repository(relation.id, self.charm.app).get_data():
                try:
                    model = self.interface.build_model(
                        relation.id, AirflowCoordinatorProviderModel, component=self.charm.app
                    )

                    model.validation_failures = failures_serialized
                    model.config_template = None
                    model.kubernetes_executor_pod_spec = None
                    model.sensitive_data = None
                except pydantic.ValidationError:
                    pass

            if not model:
                model = AirflowCoordinatorProviderModel(
                    validation_failures=failures_serialized,
                )

            self.interface.write_model(relation.id, model)

    @property
    def core_charms_metadata(self) -> dict[str, AirflowCoordinatorRequirerModel]:
        """Charm metadata from each of the related core charms."""

        def _build_requirer_model(
            relation: ops.Relation,
        ) -> typing.Optional[AirflowCoordinatorRequirerModel]:
            try:
                return self.interface.build_model(
                    relation.id, AirflowCoordinatorRequirerModel, component=relation.app
                )
            except pydantic.ValidationError:
                return None

        return {
            metadata.component: metadata
            for metadata in [
                _build_requirer_model(relation)
                for relation in self.interface.relations
                if self.interface.repository(relation.id, relation.app).get_data()
            ]
            if metadata is not None
        }


class AirflowCoordinatorRequires(ops.Object):
    """A requirer handler encapsulating the airflow coordinator relation."""

    def __init__(
        self,
        charm: ops.CharmBase,
        relation_name: str,
        component: str,
        workload_container: ops.Container,
        callback: typing.Callable,
    ):
        self._charm = charm
        self._component = component
        self._relation_name = relation_name

        if not charm.model.get_relation(relation_name):
            return

        super().__init__(charm, relation_name)

        self._requirer_handler = AirflowCoordinatorRequirerEventHandler(
            charm, relation_name, AirflowCoordinatorProviderModel
        )

        self._workload_container = workload_container

        if self._workload_container.can_connect():
            # TODO: pull airflow_version and workload_image_hash from container
            # after https://github.com/canonical/airflow-rocks/issues/13 is resolved
            airflow_version = "3.1.0"
            workload_image_hash = "somehash"

            self._requirer_handler.set_metadata(
                metadata=AirflowCoordinatorRequirerModel(
                    airflow_version=airflow_version,
                    workload_image_hash=workload_image_hash,
                    component=component,
                )
            )

        for event in [
            self._requirer_handler.on.airflow_config_available,
            self._requirer_handler.on.airflow_config_updated,
            self._requirer_handler.on.airflow_core_metadata_validation_failed,
            charm.on[relation_name].relation_broken,
        ]:
            self.framework.observe(event, callback)

    @property
    def _ready(self) -> bool:
        """Indicates whether relation is ready, config available and workload can be started."""
        if not self._charm.model.get_relation(self._relation_name):
            return False

        return all(
            condition
            for condition in [
                not self.missing_core_components_exist,
                not self.validation_failure_messages,
                self._requirer_handler.provider_content,
                self._requirer_handler.provider_content.config_template,
                self._requirer_handler.provider_content.sensitive_data,
            ]
        )

    @property
    def airflow_core_validation_failures(self) -> list[str]:
        """Airflow core charm validation failures for all core charms in cluster."""
        return [failure.code for failure in self._requirer_handler.validation_failures]

    @property
    def validation_failure_messages(self) -> list[str]:
        """Validation failures for this charm from Airflow coordinator."""
        return [
            failure.code
            for failure in self._requirer_handler.validation_failures
            if failure.component == self._component
        ]

    @property
    def missing_core_components_exist(self) -> bool:
        """Indicates if coordinator reports missing components for the cluster."""
        return any(
            failure
            for failure in self._requirer_handler.validation_failures
            if failure.code == AirflowCoreValidationErrorEnum.MISSING_COMPONENT
        )

    @property
    def can_write_airflow_config(self) -> bool:
        """Indicate if it is safe to write the Airflow config to workload container.

        Ensures that the pebble is reachable in the workload container and that the
        coordinator has shared relevant config data in the relation to be able to
        render the Airflow config (and that there is a lack of validation errors).
        """
        return all(
            [
                self._workload_container.can_connect(),
                self._ready,
                self._requirer_handler.provider_content,
                self._requirer_handler.provider_content.sensitive_data,
            ]
        )

    def write_airflow_config(self, config_path: str) -> None:
        """Render the Airflow config in the provided path in the workload container."""
        provider_content = self._requirer_handler.provider_content

        config = (
            jinja2.Environment()
            .from_string(provider_content.config_template)
            .render(**json.loads(provider_content.sensitive_data))
        )

        self._workload_container.push(
            config_path,
            config,
            user="root",
            group="root",
            make_dirs=True,
        )

    @property
    def can_write_kubernetes_executor_pod_spec(self) -> bool:
        """Indicate if it is safe to write the k8s pod spec to the workload container.

        Similar to the airflow config check, ensures the lack of validation errors +
        pebble is reachable in the workload container + k8s executor pod spec present
        in the relation.
        """
        return (
            self._workload_container.can_connect()
            and self._ready
            and self._requirer_handler.provider_content.kubernetes_executor_pod_spec
        )

    def write_kubernetes_executor_pod_spec(self, filepath: str) -> None:
        """Render the K8s executor pod spec in the provided path in the workload container."""
        provider_content = self._requirer_handler.provider_content

        k8s_executor_pod_spec = (
            jinja2.Environment()
            .from_string(provider_content.kubernetes_executor_pod_spec)
            .render(**json.loads(provider_content.sensitive_data))
        )

        self._workload_container.push(
            filepath,
            k8s_executor_pod_spec,
            user="root",
            group="root",
            make_dirs=True,
        )


class AirflowCoordinatorProvides(ops.Object):
    """A provider handler encapsulating the airflow coordinator relation."""

    def __init__(self, charm: ops.CharmBase, relation_name: str, callback: typing.Callable):
        super().__init__(charm, relation_name)

        self._charm = charm
        self._relation_name = relation_name

        self._provider_handler = AirflowCoordinatorProviderEventHandler(
            charm, relation_name, AirflowCoordinatorRequirerModel
        )

        for event in [
            self._provider_handler.on.airflow_core_metadata_available,
            charm.on[relation_name].relation_broken,
        ]:
            self.framework.observe(event, callback)

    @property
    def missing_core_components(self) -> set[str]:
        """Retrieve missing Airflow core components."""
        core_charms_metadata = self._provider_handler.core_charms_metadata

        return sorted(set(AirflowCoreComponentEnum) - set(core_charms_metadata.keys()))

    @property
    def airflow_version_with_max_count(self) -> str:
        """The airflow version with max count amongst all related core charms."""
        airflow_versions = collections.defaultdict(int)

        for metadata in self._provider_handler.core_charms_metadata.values():
            airflow_versions[metadata.airflow_version] += 1

        return max(airflow_versions, key=airflow_versions.get)

    @property
    def workload_image_hash_with_max_count(self) -> str:
        """Airflow core workload image hash with max count amongst all related core charms."""
        workload_image_hashes = collections.defaultdict(int)

        for metadata in self._provider_handler.core_charms_metadata.values():
            workload_image_hashes[metadata.workload_image_hash] += 1

        return max(workload_image_hashes, key=workload_image_hashes.get)

    @property
    def are_airflow_versions_consistent(self) -> bool:
        """Check that all related core charms have the same Airflow version."""
        return (
            len(
                {
                    metadata.airflow_version
                    for metadata in self._provider_handler.core_charms_metadata.values()
                }
            )
            == 1
        )

    @property
    def are_workload_image_hashes_consistent(self) -> bool:
        """Check that all related core charms have the same workload image hash."""
        return (
            len(
                {
                    metadata.workload_image_hash
                    for metadata in self._provider_handler.core_charms_metadata.values()
                }
            )
            == 1
        )

    def set_validation_errors(self) -> None:
        """Set any core charm validation errors in relation databag.

        Will prioritize validation errors where core components are missing.
        If no missing components, all mismatched airflow version and mismatched
        workload image hash validation errors are populated in the databag.
        """
        if self.missing_core_components:
            validation_error_messages = [
                MetadataValidationError(
                    component=component,
                    code=AirflowCoreValidationErrorEnum.MISSING_COMPONENT,
                )
                for component in self.missing_core_components
            ]

            self._provider_handler.set_validation_errors(validation_error_messages)
            return

        if self.are_airflow_versions_consistent and self.are_workload_image_hashes_consistent:
            return

        validation_error_messages = []

        airflow_version_with_max_count = self.airflow_version_with_max_count
        workload_image_hash_with_max_count = self.workload_image_hash_with_max_count

        for component, metadata in self._provider_handler.core_charms_metadata.items():
            if metadata.airflow_version != airflow_version_with_max_count:
                validation_error_messages.append(
                    MetadataValidationError(
                        component=component,
                        code=AirflowCoreValidationErrorEnum.INCONSISTENT_AIRFLOW_VERSION,
                    )
                )

            if metadata.workload_image_hash != workload_image_hash_with_max_count:
                validation_error_messages.append(
                    MetadataValidationError(
                        component=component,
                        code=AirflowCoreValidationErrorEnum.INCONSISTENT_WORKLOAD_IMAGE_HASH,
                    )
                )

        self._provider_handler.set_validation_errors(validation_error_messages)

    def set_airflow_config(
        self,
        config_template: str,
        k8s_executor_pod_spec_template: typing.Optional[str] = None,
        sensitive_data: dict[str, str] = {},
    ) -> None:
        """Update config with related core charms.

        Args:
            config_template: Airflow config (jinja template string)
            k8s_executor_pod_spec_template: (optional) K8s executor pod spec template
            sensitive_data: sensitive data to render config of k8s executor pod
                spec jinja templates with
        """
        self._provider_handler.update_content(
            config_template=config_template,
            kubernetes_executor_pod_spec=k8s_executor_pod_spec_template,
            sensitive_data=sensitive_data,
        )
