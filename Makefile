.PHONY: build test clean run db.init db.drop db.test.init db.test.drop

.DEFAULT_GOAL := run

# Set default PGPASSWORD if not defined
PGPASSWORD ?= password
export PGPASSWORD

build:
	GOEXPERIMENT=synctest go build -o quackfs.exe ./src/cmd/quackfs

test: db.test.drop db.test.init
	GOEXPERIMENT=synctest go test -failfast -timeout 5s -p 1 -race -shuffle=on -v ./src/... $(TEST)

clean: db.drop
	fusermount3 -u /tmp/fuse || true
	rm -f quackfs.exe
	rm -rf /tmp/fuse

db.init:
	@echo "Setting up PostgreSQL database if not already running"
	@sudo service postgresql status > /dev/null || sudo service postgresql start
	@for i in {1..10}; do pg_isready -h localhost -U postgres && break || sleep 1; done
	@if ! psql -h localhost -U postgres -lqt | cut -d \| -f 1 | grep -qw quackfs; then \
		echo "Creating quackfs database..."; \
		psql -h localhost -U postgres -c "CREATE DATABASE quackfs;"; \
		psql -h localhost -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE quackfs TO postgres;"; \
	else \
		echo "Database quackfs already exists"; \
	fi

	@psql -h localhost -U postgres -d quackfs -f schema.sql;

db.test.init:
	@echo "Setting up PostgreSQL test database if not already running"
	@sudo service postgresql status > /dev/null || sudo service postgresql start
	@for i in {1..10}; do pg_isready -h localhost -U postgres && break || sleep 1; done
	@if ! psql -h localhost -U postgres -lqt | cut -d \| -f 1 | grep -qw quackfs_test; then \
		echo "Creating quackfs_test database..."; \
		psql -h localhost -U postgres -c "CREATE DATABASE quackfs_test;"; \
		psql -h localhost -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE quackfs_test TO postgres;"; \
	else \
		echo "Database quackfs_test already exists"; \
	fi

	@psql -h localhost -U postgres -d quackfs_test -f schema.sql;

db.drop:
	@echo "Cleaning PostgreSQL database"
	@psql -h localhost -U postgres -c "DROP DATABASE IF EXISTS quackfs;" || true

db.test.drop:
	@echo "Cleaning PostgreSQL test database"
	@psql -h localhost -U postgres -c "DROP DATABASE IF EXISTS quackfs_test;" || true

run: clean build db.init
	mkdir -p /tmp/fuse
	./quackfs.exe -mount /tmp/fuse

load:
	./duckdb.sh
	duckdb -f ./duckdb.sql /tmp/fuse/db.duckdb