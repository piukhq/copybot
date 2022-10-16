#!/bin/zsh

docker run -it --rm -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 postgres:latest
docker run -it --rm -p 5672:5672 rabbitmq:latest
