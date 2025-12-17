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
    uv tool run tox -e unit

# Run format
format: (clean-mock-charm-libs)
    uv tool run tox -e format

# Run integration tests
integration: (clean-mock-charm-libs)
    charmcraft pack

    cp -r lib tests/integration/mock-core-charm/lib

    trap 'just clean-mock-charm-libs' EXIT

    cd tests/integration/mock-core-charm && charmcraft pack

    JUJU_MODEL=test uv tool run tox -e integration
