# Airflow Coordinator K8s Operator

A Charmed Operator for coordinating Charmed Airflow operators.

## Providing Custom Airflow Configuration

By default, the Airflow Coordinator generates a default Airflow configuration
for the cluster influenced by applications it is integrated with. While this
offers a robust default configuration, Airflow Coordinator also supports a
mechanism to provide custom configurations relevant to a specific deployment.

There two ways one can input custom Airflow configurations.

For configuration values that are not sensitive (file up to 16MiB), the `custom-airflow-configuration` config option can be used:

```
$ juju config airflow-coordinator-k8s custom-airflow-configuration=@/tmp/custom_config.ini
$ cat /tmp/custom_config.ini
[core]
dags_folder = /opt/airflow/custom_dags

[logging]
base_log_folder = /opt/airflow/logs
```

For configuration values that are sensitive, we provide a way to specify a
juju user secret with the config file (up to 1MiB):

```
$ juju add-secret custom-config-secret sensitive-airflow-configuration-secret#file=/tmp/sensitive_config.ini
$ juju grant-secret custom-config-secret airflow-coordinator-k8s
$ juju config airflow-coordinator-k8s sensitive-airflow-configuration-secret="<URI of custom-config-secret>"
$ cat /tmp/sensitivie_config.ini
[api_auth]
jwt_secret = some_jwt_secret

[api]
secret_key = some_api_server_secret
```

Airflow Coordinator performs various validations on provided custom configurations
(e.g. disallow blacklisted options), and validation failures will result in the
most recently supplied custom configurations being inactive until all failures
are addressed.

Note: Airflow Coordinator will either update or add key-value pairs specified
in custom configurations (sensitive or non-sensitive) depending on whether
the key-value pair exist in the default generated configuration (with override
as the merge strategy, giving precedence to custom configuration).
