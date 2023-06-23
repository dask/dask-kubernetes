FROM golang:1.19.9

RUN git clone \
      --depth=1 \
      --branch v0.24.15 \
      https://github.com/kubernetes/code-generator.git \
      $GOPATH/src/k8s.io/code-generator

RUN touch $HOME/boilerplate.txt
