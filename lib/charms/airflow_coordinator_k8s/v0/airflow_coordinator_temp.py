# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

# TODO: move code to officially generated charm lib once airflow-coordinator-k8s
# is registered and first revision published on charmhub
"""Temporary charm lib for Airflow Coordinator."""

import logging
import pickle
import typing

import charms.data_platform_libs.v1.data_interfaces as data_interfaces
import jinja2
import ops
import pydantic
import typing_extensions

logger = logging.getLogger(__name__)

REQUIRED_AIRFLOW_CORE_COMPONENTS = [
    "scheduler",
    "api-server",
    "triggerer",
    "dag-processor",
]


class AirflowCoordinatorRequirerModel(data_interfaces.BaseCommonModel):
    """Requirer side of the Airflow Coordinator model."""

    airflow_version: str
    workload_image_hash: str
    component: typing.Literal["scheduler", "api-server", "triggerer", "dag-processor"]

    @pydantic.model_validator(mode="after")
    def validate_fields(self):
        """Validates that no inconsistent request sent to Airflow Coordinator."""
        if not self.airflow_version:
            raise ValueError("Missing airflow version")

        if not self.workload_image_hash:
            raise ValueError("Missing workload image hash")

        if not self.component:
            raise ValueError("Missing component")

        return self


class AirflowCoordinatorProviderModel(data_interfaces.BaseCommonModel):
    """Provider side of the Airflow Coordinator model."""

    config_template: str | None = pydantic.Field(default=None)
    kubernetes_executor_pod_spec: str | None = pydantic.Field(default=None)
    sensitive_data: dict[str, str] | None = pydantic.Field(default=None)
    secret_id: data_interfaces.SecretString | None = pydantic.Field(default=None)

    validation_failure: dict | None = pydantic.Field(default_factory=dict)

    @pydantic.model_validator(mode="after")
    def validate_fields(self):
        """Validates and modifies, if necessary, response to be sent from Airflow Coordinator."""
        if self.validation_failure:
            if not all(
                key in self.validation_failure and self.validation_failure[key]
                for key in ["failed_component", "failure_code", "error_message"]
            ):
                raise ValueError("Invalid format of validation failure")

            # TODO: confirm whether we really want to nullify all data in databag if
            # validation error message present
            self.config_template = None
            self.kubernetes_executor_pod_spec = None
            self.sensitive_data = {}
            # TODO: confirm how to remove this secret id or if it can be reused without deleting
            self.secret_id = None

            return self

        if not self.config_template:
            raise ValueError("Missing config template")

        # TODO: uncomment once we have a valid pod spec to write in the relation
        # if not self.kubernetes_executor_pod_spec:
        #     raise ValueError("Missing kubernetes executor pod spec")

        if not self.sensitive_data:
            # TODO: remove k8s executor pod spec from error message if it has no sensitive data
            raise ValueError(
                "Missing sensitive data to render config or k8s executor pod spec templates"
            )

        return self


TAirflowCoordinatorRequirerModel = typing.TypeVar(
    "TAirflowCoordinatorRequirerModel", bound=AirflowCoordinatorRequirerModel
)
TAirflowCoordinatorProviderModel = typing.TypeVar(
    "TAirflowCoordinatorProviderModel", bound=AirflowCoordinatorProviderModel
)


class AirflowCoordinatorEvent(ops.EventBase):
    """Airflow config related event."""

    def __init__(
        self,
        handle: ops.Handle,
        relation: ops.Relation,
        app: ops.Application | None,
        unit: ops.Unit | None,
        content: TAirflowCoordinatorRequirerModel | TAirflowCoordinatorProviderModel,
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
        self.app = self.framework.model.get(app_name) if app_name else None

        unit_name = snapshot.get("unit_name")
        self.unit = self.framework.model.get(unit_name) if unit_name else None

        self.content = pickle.loads(snapshot["content"])


class AirflowConfigAvailableEvent(
    AirflowCoordinatorEvent, typing.Generic[TAirflowCoordinatorProviderModel]
):
    """Event emitted when the Airflow config is available."""


class AirflowConfigUpdatedEvent(
    AirflowCoordinatorEvent, typing.Generic[TAirflowCoordinatorProviderModel]
):
    """Event emitted when the Airflow config is updated."""


class AirflowCoreMetadataValidationFailed(
    AirflowCoordinatorEvent, typing.Generic[TAirflowCoordinatorProviderModel]
):
    """Event emitted when an Airflow core charm's metadata validation fails."""


class AirflowCoordinatorRequiresEvents(
    ops.CharmEvents, typing.Generic[TAirflowCoordinatorProviderModel]
):
    """Events that Airflow core charms can emit."""

    airflow_config_available = ops.EventSource(AirflowConfigAvailableEvent)
    airflow_config_updated = ops.EventSource(AirflowConfigUpdatedEvent)
    airflow_core_metadata_validation_failed = ops.EventSource(AirflowCoreMetadataValidationFailed)


class AirflowCoreMetadataAvailableEvent(
    AirflowCoordinatorEvent, typing.Generic[TAirflowCoordinatorRequirerModel]
):
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

    on = AirflowCoordinatorRequiresEvents[TAirflowCoordinatorProviderModel]

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

    def _dispatch_events(
        self,
        event: ops.RelationEvent,
        _diff: data_interfaces.Diff,
        content: AirflowCoordinatorProviderModel,
    ):
        if "validation-failure" in _diff.added or "validation-failure" in _diff.changed:
            getattr(self.on, "airflow_core_metadata_validation_failed").emit(
                event.relation, app=event.app, unit=event.unit, content=content
            )

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

        repository = data_interfaces.OpsRelationRepository(
            self.model, event.relation, component=event.relation.app
        )
        content = data_interfaces.build_model(repository, AirflowCoordinatorProviderModel)

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

        # Don't do anything until we get some data
        if not repository.get_data():
            return

        content = data_interfaces.build_model(repository, AirflowCoordinatorProviderModel)
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
    def provider_content(self) -> AirflowCoordinatorProviderModel:
        """Data from the related Airflow Coordinator charm."""
        # TODO: see if the relation and repository can be cached at class level
        relation = self.charm.model.get_relation(self.relation_name)
        repository = data_interfaces.OpsRelationRepository(
            self.model, relation, component=relation.app
        )
        return data_interfaces.build_model(repository, AirflowCoordinatorProviderModel)


class AirflowCoordinatorProviderEventHandler(
    data_interfaces.EventHandlers, typing.Generic[TAirflowCoordinatorProviderModel]
):
    """Event Handler for Airflow Coordinator provider."""

    on = AirflowCoordinatorProvidesEvents[TAirflowCoordinatorRequirerModel]

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
        self.relation = charm.model.relations[relation_name][0]

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

        content = data_interfaces.build_model(repository, AirflowCoordinatorRequirerModel)

        self._handle_event(event, repository, content)

    def update_content(
        self,
        config_template: str = None,
        kubernetes_executor_pod_spec: str = None,
        sensitive_data: dict[str, str] = {},
    ):
        """Update data to send to related core charms."""
        if not any([config_template, kubernetes_executor_pod_spec, sensitive_data]):
            return

        if not self.charm.unit.is_leader():
            return

        try:
            model = self.interface.build_model(self.relation.id, AirflowCoordinatorProviderModel)
        except pydantic.ValidationError:
            model = AirflowCoordinatorProviderModel(
                config_template=config_template,
                kubernetes_executor_pod_spec=kubernetes_executor_pod_spec,
                sensitive_data=sensitive_data,
            )

        if config_template:
            model.config_template = config_template

        if kubernetes_executor_pod_spec:
            model.kubernetes_executor_pod_spec = kubernetes_executor_pod_spec

        if sensitive_data:
            model.sensitive_data.update(sensitive_data)

        for relation in self.interface.relations:
            self.interface.write_model(relation.id, model)

    @property
    def core_charms_metadata(self) -> dict[str, AirflowCoordinatorRequirerModel]:
        """Charm metadata from each of the related core charms."""
        return {
            metadata.component: metadata
            for metadata in [
                self.interface.build_model(
                    relation.id, AirflowCoordinatorRequirerModel, component=relation.app
                )
                for relation in self.interface.relations
                if self.interface.repository(relation.id, relation.app).get_data()
            ]
        }


class AirflowCoordinatorRequires(ops.Object):
    """A requirer handler encapsulating the airflow coordinator relation."""

    def __init__(
        self,
        charm: ops.CharmBase,
        relation_name: str,
        component: str,
        workload_container_name: str,
        callback: typing.Callable,
    ):
        if component not in REQUIRED_AIRFLOW_CORE_COMPONENTS:
            raise ValueError(f"Invalid component {component}")

        super().__init__(charm, relation_name)

        self._requirer_handler = AirflowCoordinatorRequirerEventHandler(
            charm, relation_name, AirflowCoordinatorProviderModel
        )

        self._charm = charm
        self._relation = charm.model.get_relation(relation_name)

        workload_container = charm.unit.get_container(workload_container_name)
        if workload_container.can_connect():
            # TODO: pull airflow_version and workload_image_hash from container
            airflow_version = "3.1.0"
            workload_image_hash = "somehash"

            self._requirer_handler.set_metadata(
                metadata=AirflowCoordinatorRequirerModel(
                    airflow_version=airflow_version,
                    workload_image_hash=workload_image_hash,
                    component=component,
                )
            )

        self.framework.observe(self._requirer_handler.on.airflow_config_available, callback)
        self.framework.observe(self._requirer_handler.on.airflow_config_updated, callback)
        self.framework.observe(
            self._requirer_handler.on.airflow_core_metadata_validation_failed, callback
        )
        self.framework.observe(charm.on[relation_name].relation_broken, callback)

    @property
    def config(self) -> typing.Optional[str]:
        """The Airflow config for the core charm to use."""
        if not self._relation or not self._relation.active:
            return

        provider_content = self.provider_content
        if not provider_content:
            return

        return jinja2.Template(provider_content.config_template).render(
            context=provider_content.sensitive_data
        )

    @property
    def kubernetes_executor_pod_spec(self) -> typing.Optional[str]:
        """The K8s executor pod spec for the core charm to use."""
        if not self._relation or not self._relation.active:
            return

        provider_content = self.provider_content
        if not provider_content:
            return

        return jinja2.Template(provider_content.kubernetes_executor_pod_spec).render(
            context=provider_content.sensitive_data
        )


class AirflowCoordinatorProvides(ops.Object):
    """A provider handler encapsulating the airflow coordinator relation."""

    def __init__(self, charm: ops.CharmBase, relation_name: str):
        super().__init__(charm, relation_name)

        self._charm = charm
        self._relation_name = relation_name

        self._provider_handler = AirflowCoordinatorProviderEventHandler(
            charm, relation_name, AirflowCoordinatorRequirerModel
        )

    # TODO: add logic to set validation error in databag if failure
    @property
    def all_required_components_valid(self) -> tuple[bool, str]:
        """Check validity of all related core charms."""
        core_charms_metadata = self._provider_handler.core_charms_metadata

        missing_components = sorted(
            set(REQUIRED_AIRFLOW_CORE_COMPONENTS) - set(core_charms_metadata.keys())
        )
        if missing_components:
            return False, f"Missing integrations with {', '.join(missing_components)}"

        airflow_versions = {metadata.airflow_version for metadata in core_charms_metadata.values()}
        workload_image_hashes = {
            metadata.workload_image_hash for metadata in core_charms_metadata.values()
        }

        if len(airflow_versions) > 1:
            logger.warning(
                f"Integrated apps with mismatched versions: {', '.join(airflow_versions)}"  # noqa: E501
            )
            return (
                False,
                "Integrated apps with mismatched versions",
            )

        if len(workload_image_hashes) > 1:
            logger.warning(
                f"Integrated apps with inconsistent image hashes: {', '.join(workload_image_hashes)}"  # noqa: E501
            )
            return (False, "Integrated apps with inconsistent image hashes")

        return True, ""

    def set_config(self, config_template: str, sensitive_data: dict[str, str]) -> None:
        """Update config with related core charms."""
        self._provider_handler.update_content(
            config_template=config_template, sensitive_data=sensitive_data
        )

    def set_kubernetes_executor_pod_spec(
        self, k8s_executor_pod_spec_template: str, sensitive_data: dict[str, str]
    ) -> None:
        """Update k8s executor pod spec with related core charms."""
        self._provider_handler.update_content(
            kubernetes_executor_pod_spec=k8s_executor_pod_spec_template,
            sensitive_data=sensitive_data,
        )
