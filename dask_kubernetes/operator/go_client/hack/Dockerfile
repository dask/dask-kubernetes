FROM golang:1.19.9

ENV GO111MODULE=off

RUN go get k8s.io/code-generator; exit 0
RUN go get k8s.io/api; exit 0
RUN go get k8s.io/apimachinery; exit 0

RUN mkdir -p $GOPATH/src/k8s.io/gengo/boilerplate/
RUN touch $GOPATH/src/k8s.io/gengo/boilerplate/boilerplate.go.txt

RUN mkdir -p $GOPATH/src/k8s.io/gengo/boilerplate/ \
    && touch $GOPATH/src/k8s.io/gengo/boilerplate/boilerplate.go.txt

WORKDIR $GOPATH/src/k8s.io/code-generator
