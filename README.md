go-triparclient
==============

Go HPE 3PAR Object Access REST API client.

[![GoDoc](https://godoc.org/github.com/koofr/go-triparclient?status.png)](https://godoc.org/github.com/koofr/go-triparclient)

## Beware

Only supports a subset of the Object Access API. Feel free to send in pull requests extending this. ;)

## Install

```sh
go get github.com/koofr/go-triparclient
```

## Testing

```sh
go test
```

Coverage:

```sh
go test --coverprofile=go-triparclient.coverprofile && go tool cover -html=go-triparclient.coverprofile
```
