.PHONY: build test clean
.DEFAULT_GOAL := run

build:
	go build -o difffs ./src

test:
	go test ./src

clean:
	fusermount3 -u /tmp/fuse || true
	rm -f difffs
	rm -rf /tmp/fuse

run: clean build
	mkdir -p /tmp/fuse
	./difffs -mount /tmp/fuse
