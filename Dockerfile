FROM ebonetti/golang-petsc

RUN set -eux; \
	apt-get update && apt-get install -y --no-install-recommends \
		p7zip-full; \
	apt-get clean; \
	rm -rf /var/lib/apt/lists/*;

ENV GO_DIR /usr/local/go
ENV GOPATH /go
ENV PATH $GOPATH/bin:$GO_DIR/bin:$PATH
ENV PROJECT github.com/negapedia/wikibrief
ADD . $GOPATH/src/$PROJECT
RUN go get $PROJECT/...;
WORKDIR /data