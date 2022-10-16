FROM ghcr.io/binkhq/python:3.10 as build
WORKDIR /src
RUN pip install poetry
ADD . .
RUN poetry build

FROM docker.io/python:3.10-slim

WORKDIR /app
COPY --from=build /src/dist/*.whl .
RUN pip install *.whl && rm *.whl

ENTRYPOINT [ "linkerd-await", "--" ]
CMD [ "/usr/local/bin/copybot" ]
