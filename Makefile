.PHONY: build test clean

build:
	go build -o difffs ./src

test:
	go test ./src

clean:
	rm -f difffs
