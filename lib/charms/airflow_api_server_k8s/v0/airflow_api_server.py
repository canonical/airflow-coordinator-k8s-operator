"""Library to manage the relation provided by Airflow API Server charm.

This library contains the Requires and Provides classes for handing the relation
between the Airflow API Server charm and the Airflow Coordinator charm. This
relation interface provides a way for the API server charm to convey information
that will affect the global `airflow.cfg` file distributed by Airflow Coordinator.

### Requirer Charm

The following presents an example usage of the AirflowAPIServerRequires class:

```python
import charms.airflow_api_server_k8s.v0.airflow_api_server as airflow_api_server

class AirflowCoordinatorCharm(ops.CharmBase):
    def __init__(self, *args) -> None:
        super().__init__(*args)

        self.requirer = airflow_api_server.AirflowAPIServerRequires(
            self,
            "airflow-api-server", # relation endpoint
            callback=self.reconcile,
        )

    def reconcile(self, event) -> None:
        # Access the API server host and port
        self.requirer.api_server_host
        self.requirer.api_server_port
```

### Provider Charm

The following presents an example usage of the AirflowAPIServerProvides class:

```python
import charms.airflow_api_server_k8s.v0.airflow_api_server as airflow_api_server

class AirflowAPIServerCharm(ops.CharmBase):
    def __init__(self, *args) -> None:
        super().__init__(*args)

        self.requirer = airflow_api_server.AirflowAPIServerProviders(
            self,
            "airflow-api-server", # relation endpoint
            "airflow-api-server-k8s-endpoints.airflow-model.svc.cluster.local", # host
            "8080", # port
        )
```

"""

import logging
import typing

import ops

# The unique Charmhub library identifier, never change it
LIBID = "a0959775f225419d86d202cd754066cf"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 2

HOST_KEY = "host"
PORT_KEY = "port"
INGRESS_URL_KEY = "ingress_url"

logger = logging.getLogger(__name__)


class AirflowAPIServerProvides(ops.Object):
    """A provider handler encapsulating the airflow api server relation."""

    def __init__(
        self,
        charm: ops.CharmBase,
        relation_name: str,
        host: str,
        port: str,
    ):
        super().__init__(charm, relation_name)

        self._charm = charm
        self._relation_name = relation_name
        self._relation = charm.model.get_relation(relation_name)

        self._set_api_server_host_info(host, port)

    def _set_api_server_host_info(self, host: str, port: str):
        """Write the API server host and port to the relation."""
        if not self._relation or not self._charm.unit.is_leader():
            return

        if not host:
            logger.error("Invalid host to set in airflow_api_server relation")
            return

        if not port:
            logger.error("Invalid port to set in airflow_api_server relation")
            return

        self._relation.data[self._charm.app][HOST_KEY] = host
        self._relation.data[self._charm.app][PORT_KEY] = port

    def set_ingress_url(self, url: str) -> None:
        """Write the ingress URL to the relation."""
        relation = self._charm.model.get_relation(self._relation_name)
        if not relation or not self._charm.unit.is_leader():
            return
        relation.data[self._charm.app][INGRESS_URL_KEY] = url

    def clear_ingress_url(self) -> None:
        """Remove the ingress URL from the relation."""
        relation = self._charm.model.get_relation(self._relation_name)
        if not relation or not self._charm.unit.is_leader():
            return
        relation.data[self._charm.app].pop(INGRESS_URL_KEY, None)


class AirflowAPIServerRequires(ops.Object):
    """A requirer handler encapsulating the airflow api server relation."""

    def __init__(
        self,
        charm: ops.CharmBase,
        relation_name: str,
        callback: typing.Callable,
    ):
        super().__init__(charm, relation_name)

        self._charm = charm
        self._relation = charm.model.get_relation(relation_name)

        for event in [
            charm.on[relation_name].relation_changed,
            charm.on[relation_name].relation_broken,
        ]:
            self.framework.observe(event, callback)

    @property
    def api_server_host(self) -> typing.Optional[str]:
        """Return API server host."""
        if not self._relation or not self._relation.app:
            return None

        return self._relation.data[self._relation.app].get(HOST_KEY)

    @property
    def api_server_port(self) -> typing.Optional[str]:
        """Return API server port."""
        if not self._relation or not self._relation.app:
            return None

        return self._relation.data[self._relation.app].get(PORT_KEY)

    @property
    def api_server_ingress_url(self) -> typing.Optional[str]:
        """Return the API server's external ingress URL if available."""
        if not self._relation or not self._relation.app:
            return None
        return self._relation.data[self._relation.app].get(INGRESS_URL_KEY)
