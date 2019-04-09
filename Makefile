.PHONY: build test run

build:
	docker build -t picokv .

test: build
	docker run --rm picokv /bin/bash test.sh

run: build
	docker run --rm -p 9001:9001 -d picokv

