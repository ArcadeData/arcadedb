.PHONY: build, delete, deploy, docker, install, kind, fmt, vet, test

# Remember to do "export DF_HOME=/path/to/data-fabric/" before running make build
REGISTRY=ghcr.io/raft-tech
IMAGE=df-arcadedb
PROJECT_HOME=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
VERSION=dev
FULL_IMAGE=${REGISTRY}/${IMAGE}:${VERSION}
KIND_CLUSTER=data-fabric
DOCKER=docker

docker:
	$(DOCKER) build --no-cache -f Dockerfile \
        . \
       -t ${FULL_IMAGE}

buildx:
	$(DOCKER) buildx build -f ${PROJECT_HOME}/Dockerfile \
		${PROJECT_HOME} \
		-t ${FULL_IMAGE} \
		--platform linux/amd64

kind: buildx
	kind load docker-image ${FULL_IMAGE} --name ${KIND_CLUSTER}
