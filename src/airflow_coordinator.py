# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm support for the Airflow Coordinator relation."""

import logging

import ops
import ops.charm
import ops.framework

from constants import AIRFLOW_COORDINATOR_RELATION_SECRET_LABEL, REQUIRED_AIRFLOW_CORE_COMPONENTS

logger = logging.getLogger(__name__)


class AirflowCoordinatorProvides(ops.framework.Object):
    """A provider handler encapsulating all airflow coordinator relations."""

    def __init__(self, charm: ops.charm.CharmBase, relation_name: str):
        super().__init__(charm, relation_name)
        self._charm = charm
        self._relation_name = relation_name

        self._relations = self._populate_relations()

    def _populate_relations(self):
        """Select all relevant relations with core charms."""
        relations = {}

        for relation in [
            relation
            for relations in self._charm.model.relations.values()
            for relation in relations
        ]:
            if relation.name != self._relation_name:
                continue

            component = relation.data[relation.app].get("component")
            if component and component in REQUIRED_AIRFLOW_CORE_COMPONENTS:
                relations[component] = relation

        return relations

    def _get_related_component_airflow_versions(self) -> set[str]:
        """Get the reported Airflow versions of all related components."""
        airflow_versions = set()

        for relation in self._relations.values():
            airflow_versions.add(relation.data[relation.app].get("airflow_version"))

        return airflow_versions

    def _get_related_component_workload_image_hashes(self) -> set[str]:
        """Get the reported Airflow versions of all related components."""
        workload_image_hashes = set()

        for relation in self._relations.values():
            workload_image_hashes.add(relation.data[relation.app].get("workload_image_hash"))

        return workload_image_hashes

    @property
    def all_required_components_valid(self) -> tuple[bool, str]:
        """Checks if all required Airflow core charms are related and valid."""
        missing_components = sorted(
            set(REQUIRED_AIRFLOW_CORE_COMPONENTS) - set(self._relations.keys())
        )
        if missing_components:
            return False, f"Missing integrations with {', '.join(missing_components)}"

        airflow_versions = self._get_related_component_airflow_versions()
        workload_image_hashes = self._get_related_component_workload_image_hashes()

        if None in airflow_versions or len(airflow_versions) > 1:
            logger.warning(
                f"Integrated apps with invalid or mismatched versions: {', '.join(airflow_versions)}"  # noqa: E501
            )
            return (
                False,
                "Integrated apps with invalid or mismatched versions",
            )

        if None in workload_image_hashes or len(workload_image_hashes) > 1:
            logger.warning(
                f"Integrated apps with inconsistent image hashes: {', '.join(workload_image_hashes)}"  # noqa: E501
            )
            return (False, "Integrated apps with inconsistent image hashes")

        return True, ""

    def _update_relation_data(
        self,
        relation: ops.Relation,
        databag_contents: dict[str, str],
        secret_contents: dict[str, str],
    ) -> None:
        """Update contents of the databag to send Airflow config to related components."""
        if not self._charm.unit.is_leader():
            return

        secret = self._charm.create_or_update_secret(
            "app", AIRFLOW_COORDINATOR_RELATION_SECRET_LABEL, secret_contents
        )
        secret.grant(relation)

        databag_contents["secret_id"] = secret.id
        relation.data[self._charm.app].update(databag_contents)

    def set_config(self, config_template: str, sensitive_data: dict[str, str]) -> None:
        """Set the Airflow config template and secrets in all core charm relation databags."""
        for relation in self._relations.values():
            self._update_relation_data(relation, {"config": config_template}, sensitive_data)

    def set_kubernetes_executor_pod_spec(
        self, k8s_executor_pod_spec_template: str, sensitive_data: dict[str, str]
    ) -> None:
        """Set K8s executor pod spec template and secrets in all core charm relation databags."""
        for relation in self._relations.values():
            self._update_relation_data(
                relation,
                {"kubernetes_executor_pod_spec": k8s_executor_pod_spec_template},
                sensitive_data,
            )
