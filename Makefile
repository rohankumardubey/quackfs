.PHONY: build test clean run db.init db.drop db.test.init db.test.drop

.DEFAULT_GOAL := run

# Set default PGPASSWORD if not defined
PGPASSWORD ?= password
export PGPASSWORD

build:
	GOEXPERIMENT=synctest go build -o quackfs.exe ./src/cmd/quackfs

test: db.test.drop db.test.init localstack.test.init
	S3_BUCKET_NAME=quackfs-bucket-test GOEXPERIMENT=synctest go test -failfast -timeout 5s -p 1 -race -shuffle=on -v ./src/... $(TEST)

clean: db.drop
	fusermount3 -u /tmp/fuse || true
	rm -f quackfs.exe
	rm -rf /tmp/fuse

db.init:
	@echo "Setting up PostgreSQL database if not already running"
	@sudo service postgresql status > /dev/null || sudo service postgresql start
	@for i in {1..10}; do pg_isready -h localhost -p 5432 -U postgres && break || sleep 1; done
	@if ! psql -h localhost -p 5432 -U postgres -lqt | cut -d \| -f 1 | grep -qw quackfs; then \
		echo "Creating quackfs database..."; \
		psql -h localhost -p 5432 -U postgres -c "CREATE DATABASE quackfs;"; \
		psql -h localhost -p 5432 -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE quackfs TO postgres;"; \
	else \
		echo "Database quackfs already exists"; \
	fi

	@psql -h localhost -p 5432 -U postgres -d quackfs -f schema.sql;

db.test.init:
	@echo "Setting up PostgreSQL test database if not already running"
	# @sudo service postgresql status > /dev/null || sudo service postgresql start
	@for i in {1..10}; do pg_isready -h localhost -p 5432 -U postgres && break || sleep 1; done
	@if ! psql -h localhost -p 5432 -U postgres -lqt | cut -d \| -f 1 | grep -qw quackfs_test; then \
		echo "Creating quackfs_test database..."; \
		psql -h localhost -p 5432 -U postgres -c "CREATE DATABASE quackfs_test;"; \
		psql -h localhost -p 5432 -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE quackfs_test TO postgres;"; \
	else \
		echo "Database quackfs_test already exists"; \
	fi

	@psql -h localhost -p 5432 -U postgres -d quackfs_test -f schema.sql;

db.drop:
	@echo "Cleaning PostgreSQL database"
	@psql -h localhost -p 5432 -U postgres -c "DROP DATABASE IF EXISTS quackfs;" || true

db.test.drop:
	@echo "Cleaning PostgreSQL test database"
	@psql -h localhost -p 5432 -U postgres -c "DROP DATABASE IF EXISTS quackfs_test;" || true

run: clean build db.init localstack.init
	mkdir -p /tmp/fuse
	./quackfs.exe -mount /tmp/fuse

load:
	./duckdb.sh
	duckdb -f ./duckdb.sql /tmp/fuse/db.duckdb

localstack.init:
	@if [ -z "$$(docker ps -q -f name=localstack)" ]; then \
		echo "Starting LocalStack container..."; \
		docker run \
			--rm -it -d --name localstack \
			-p 127.0.0.1:4566:4566 \
			-p 127.0.0.1:4510-4559:4510-4559 \
			-v /var/run/docker.sock:/var/run/docker.sock \
			localstack/localstack; \
	else \
		echo "LocalStack container is already running."; \
		echo "--- LocalStack logs:"; \
		docker logs --tail 30 localstack; \
		echo "---"; \
	fi; \
	if [ -z "$$(docker exec -it localstack awslocal s3 ls | grep quackfs-bucket)" ]; then \
		echo "Creating quackfs-bucket..."; \
		awslocal s3 mb s3://quackfs-bucket; \
	else \
		echo "quackfs-bucket already exists."; \
	fi

localstack.test.init:
	@if [ -z "$$(docker ps -q -f name=localstack)" ]; then \
		echo "Starting LocalStack container for tests..."; \
		docker run \
			--rm -it -d --name localstack \
			-p 127.0.0.1:4566:4566 \
			-p 127.0.0.1:4510-4559:4510-4559 \
			-v /var/run/docker.sock:/var/run/docker.sock \
			localstack/localstack; \
	else \
		echo "LocalStack container is already running."; \
	fi; \
	if [ -z "$$(docker exec -it localstack awslocal s3 ls | grep quackfs-bucket-test)" ]; then \
		echo "Creating test bucket quackfs-bucket-test..."; \
		awslocal s3 mb s3://quackfs-bucket-test; \
	else \
		echo "quackfs-bucket-test already exists. Let's rebuild it."; \
		awslocal s3 rm s3://quackfs-bucket-test --recursive; \
		awslocal s3 mb s3://quackfs-bucket-test; \
	fi

localstack.drop:
	@if [ -n "$$(docker ps -q -f name=localstack)" ]; then \
		echo "Stopping LocalStack container..."; \
		docker stop localstack || true; \
		echo "Removing LocalStack container..."; \
		docker rm --force localstack || true; \
	fi