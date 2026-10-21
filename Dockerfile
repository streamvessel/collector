# Builds sv-log-agent from source. Releases use Dockerfile.release with the
# binary goreleaser built; both run the same static binary on distroless.
FROM golang:1.26.10-bookworm@sha256:ee3901c61e423732605a51f1b8157c849ec01194ea0a274213b5202b2c11e7bf AS build
WORKDIR /src
COPY exporter/svexporter/go.mod exporter/svexporter/go.sum exporter/svexporter/
COPY cmd/sv-log-agent/go.mod cmd/sv-log-agent/go.sum cmd/sv-log-agent/
RUN cd cmd/sv-log-agent && go mod download
COPY exporter/svexporter exporter/svexporter
COPY cmd/sv-log-agent cmd/sv-log-agent
RUN cd cmd/sv-log-agent && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -buildid=" -o /out/sv-log-agent .

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/sv-log-agent /sv-log-agent
COPY examples/agent.yaml /etc/sv-log-agent/agent.yaml
ENV SV_STORAGE_DIR=/var/lib/sv-log-agent
ENTRYPOINT ["/sv-log-agent"]
CMD ["--config", "/etc/sv-log-agent/agent.yaml"]
