#!/bin/zsh

podman run -d --name=copybot-postgres --rm -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 docker.io/postgres:latest
podman run -d --name=copybot-rabbitmq --rm -p 5672:5672 docker.io/rabbitmq:latest
