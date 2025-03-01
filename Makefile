.PHONY: build test clean run db-init db-clean

.DEFAULT_GOAL := run

build:
	go build -o difffs ./src

test: db.test.clean
	go test -race -shuffle=on -v ./src

clean:
	fusermount3 -u /tmp/fuse || true
	rm -f difffs
	rm -rf /tmp/fuse

db.init:
	@echo "Setting up PostgreSQL database if not already running"
	@sudo service postgresql status > /dev/null || sudo service postgresql start
	@for i in {1..10}; do pg_isready -h localhost && break || sleep 1; done
	@if ! psql -U postgres -lqt | cut -d \| -f 1 | grep -qw difffs; then \
		echo "Creating difffs database..."; \
		psql -U postgres -c "CREATE DATABASE difffs;"; \
		psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE difffs TO postgres;"; \
	else \
		echo "Database difffs already exists"; \
	fi

db.test.init:
	@echo "Setting up PostgreSQL test database if not already running"
	@sudo service postgresql status > /dev/null || sudo service postgresql start
	@for i in {1..10}; do pg_isready -h localhost && break || sleep 1; done
	@if ! psql -U postgres -lqt | cut -d \| -f 1 | grep -qw difffs_test; then \
		echo "Creating difffs_test database..."; \
		psql -U postgres -c "CREATE DATABASE difffs_test;"; \
		psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE difffs_test TO postgres;"; \
	else \
		echo "Database difffs_test already exists"; \
	fi

db.clean:
	@echo "Cleaning PostgreSQL database"
	@psql -U postgres -c "DROP DATABASE IF EXISTS difffs;" || true
	@psql -U postgres -c "CREATE DATABASE difffs;"
	@psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE difffs TO postgres;"

db.test.clean:
	@echo "Cleaning PostgreSQL test database"
	@psql -U postgres -c "DROP DATABASE IF EXISTS difffs_test;" || true
	@psql -U postgres -c "CREATE DATABASE difffs_test;"
	@psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE difffs_test TO postgres;"

run: clean build db-init
	mkdir -p /tmp/fuse
	./difffs -mount /tmp/fuse
