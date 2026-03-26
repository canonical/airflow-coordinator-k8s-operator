set export
set fallback


[private]
default:
	just --list

[private]
clean-mock-charm-libs:
	rm -rf tests/integration/mock-core-charm/lib

# Run lint
lint: (clean-mock-charm-libs)
	uv tool run --python 3.12 tox -e lint

# Run format
format: (clean-mock-charm-libs)
	uv tool run --python 3.12 tox -e format

# Run integration tests
integration debug="": (clean)
	#!/usr/bin/bash
	charmcraft pack

	cp -r lib tests/integration/mock-core-charm/lib

	trap 'just clean-mock-charm-libs' EXIT

	cd tests/integration/mock-core-charm && charmcraft pack && cd -

	pdb_options=$(if [ -n "${debug}" ]; then echo "--pdb"; fi)

	JUJU_MODEL=test uv tool run tox -e integration -- ${pdb_options}

# Run unit tests
unit:
	uv tool run --python 3.12 tox -e unit

# Clean up test environment
clean: (clean-mock-charm-libs)
	juju destroy-model --force --destroy-storage --no-prompt test || true

# Get system state for debugging
get-system-state:
    #!/usr/bin/bash

    df -h
    echo "---"

    juju status --model test --color --relations --storage
    echo "---"

    sudo k8s status
    echo "---"
