FROM golang:1.26.10-bookworm@sha256:ee3901c61e423732605a51f1b8157c849ec01194ea0a274213b5202b2c11e7bf AS build
WORKDIR /src
COPY test/e2e/fakesvbe/go.mod test/e2e/fakesvbe/go.sum ./
RUN go mod download
COPY test/e2e/fakesvbe/*.go ./
RUN CGO_ENABLED=0 go build -trimpath -o /out/fakesvbe .

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/fakesvbe /fakesvbe
ENTRYPOINT ["/fakesvbe"]
CMD ["serve"]
