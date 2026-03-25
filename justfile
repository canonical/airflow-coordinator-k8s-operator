set export
set fallback


[private]
default:
	just --list

[private]
clean-mock-charm-libs:
	rm -rf tests/integration/mock-core-charm/lib

[private]
microceph-node-ip:
	#!/usr/bin/bash
	echo "$(sudo microceph status | head -n 2 | tail -n 1 | awk '{print $3}' | tr -d '()')"

[private]
set-up-microceph-certs:
	#!/usr/bin/bash

	if [ ! -d "microceph_certs" ]; then
		host_ip="(just microceph-node-ip)"

		mkdir microceph_certs

		openssl genrsa -out ./microceph_certs/ca.key

		openssl req \
			-x509 \
			-new \
			-nodes \
			-key ./microceph_certs/ca.key \
			-days 1024 \
			-out ./microceph_certs/ca.crt \
			-outform PEM \
			-subj /C=US/ST=Denial/L=Springfield/O=Dis/CN=www.example.com

		openssl genrsa -out ./microceph_certs/server.key 2048

		openssl req \
			-new \
			-key ./microceph_certs/server.key \
			-out ./microceph_certs/server.csr \
			-subj /C=US/ST=Denial/L=Springfield/O=Dis/CN=www.example.com

		echo "subjectAltName = IP:$host_ip" > ./extfile.cnf

		openssl x509 \
			-req \
			-in ./microceph_certs/server.csr \
			-CA ./microceph_certs/ca.crt \
			-CAkey ./microceph_certs/ca.key \
			-CAcreateserial \
			-out ./microceph_certs/server.crt \
			-days 365 \
			-extfile ./microceph_certs/extfile.cnf
	fi

[private]
set-up-microceph:
	#!/usr/bin/bash

	if [ "$(sudo snap list microceph | wc -l)" -ne "2" ]; then
		sudo snap install microceph --channel squid/stable
		sudo microceph cluster bootstrap
		sudo microceph disk add loop,1G,3

		just setup-microceph-certs

		sudo microceph enable rgw \
			--ssl-certificate="$(base64 -w0 ./microceph_certs/server.crt)" \
			--ssl-private-key="$(base64 -w0 ./microceph_certs/server.key)"

		sudo microceph.radosgw-admin user create \
			--uid test \
			--display-name test \
			--access-key foo \
			--secret-key bar

		sudo microceph.radosgw-admin caps add \
			--uid test
			--caps "buckets=*;users=read;usage=*;metadata=*"
	fi

	sudo microceph status

# Run lint
lint: (clean-mock-charm-libs)
	uv tool run tox -e lint

# Run format
format: (clean-mock-charm-libs)
	uv tool run tox -e format

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
	uv tool run tox -e unit

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
