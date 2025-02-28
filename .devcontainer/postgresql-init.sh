#!/bin/bash

# Start PostgreSQL service
service postgresql start

# Wait for PostgreSQL to fully start
for i in {1..30}; do
    if pg_isready -h localhost; then
        break
    fi
    echo "Waiting for PostgreSQL to start... ($i/30)"
    sleep 1
done

# Create the database and user
sudo -u postgres psql -c "CREATE DATABASE difffs;"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE difffs TO postgres;"
sudo -u postgres psql -d difffs -c "ALTER USER postgres WITH ENCRYPTED PASSWORD 'password';"

# Configure PostgreSQL to allow password-based authentication
sudo sed -i '/^host/s/ident/md5/' /etc/postgresql/12/main/pg_hba.conf
sudo sed -i '/^local/s/peer/trust/' /etc/postgresql/12/main/pg_hba.conf
echo "host all all 0.0.0.0/0 md5" | sudo tee -a /etc/postgresql/12/main/pg_hba.conf

# Reload PostgreSQL to apply the new configuration
sudo service postgresql reload

echo "PostgreSQL database 'difffs' has been created and authentication has been configured."

# Show PostgreSQL status
echo "PostgreSQL is running and ready to use."