FROM golang:1.24.1-alpine3.20

RUN apk add --no-cache build-base make gcc git

RUN apk add --update --no-cache \
           graphviz \
           ttf-freefont

RUN go install golang.org/x/tools/...@v0.31.0
RUN go install gotest.tools/gotestsum@v1.12.1
RUN go install github.com/uudashr/gopkgs/v2/cmd/gopkgs@v2.1.2
# The version is correct for go-outline. The tag is not prefixed with 'v'.
RUN go install github.com/ramya-rao-a/go-outline@1.0.0
RUN go install github.com/cweill/gotests/...@v1.6.0
RUN go install github.com/fatih/gomodifytags@v1.17.0
RUN go install github.com/josharian/impl@v1.4.0
RUN go install github.com/haya14busa/goplay/cmd/goplay@v1.0.0
RUN go install github.com/go-delve/delve/cmd/dlv@v1.24.1
RUN go install github.com/vektra/mockery/v2@v2.53.3
RUN go install mvdan.cc/gofumpt@v0.7.0
RUN go install github.com/daixiang0/gci@v0.13.6
RUN go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.4.1
RUN go install golang.org/x/tools/gopls@v0.18.1
RUN go install golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@v0.31.0

RUN go install github.com/google/pprof@latest

RUN wget -O- -nv https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s v1.64.8

CMD [ "sleep", "infinity" ]