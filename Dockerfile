FROM golang:1.13.4

WORKDIR /go/src/github.com/apiloqbc/sarama-easy

ADD . .

ENV GO111MODULE=on

RUN mkdir -p bin && go build -o bin/ ./...

CMD ["echo use docker-compose up to run the examples"]
