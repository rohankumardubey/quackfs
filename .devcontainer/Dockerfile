#-------------------------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See https://go.microsoft.com/fwlink/?linkid=2090316 for license information.
#-------------------------------------------------------------------------------------------------------------

FROM ubuntu:focal

COPY first-run-notice.txt /tmp/scripts/

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    # Restore man command
    && yes | unminimize 2>&1 

ENV LANG="C.UTF-8"

# Install basic build tools
RUN apt-get update \
    && apt-get upgrade -y \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        make \
        unzip \
        build-essential \
        swig3.0 \
        # Required for PostgreSQL
        postgresql-contrib \
        postgresql \
        # Required for ts
        moreutils \
        rsync \
        zip \
        libgdiplus \
        jq \
        #.NET Core related pre-requisites
        libc6 \
        libgcc1 \
        libgssapi-krb5-2 \
        libncurses5 \
        liblttng-ust0 \
        libssl-dev \
        libstdc++6 \
        zlib1g \
        libuuid1 \
        libunwind8 \
        sqlite3 \
        libsqlite3-dev \
        software-properties-common \
        tk-dev \
        uuid-dev \
        curl \
        gettext \
        inotify-tools \
    && rm -rf /var/lib/apt/lists/* \
    # This is the folder containing 'links' to benv and build script generator
    && apt-get update \
    && apt-get upgrade -y \
    && add-apt-repository universe \
    && rm -rf /var/lib/apt/lists/*

# Verify expected build and debug tools are present
RUN apt-get update \
    && apt-get -y install build-essential cmake cppcheck valgrind clang lldb llvm gdb wget \
    # Install tools and shells not in common script
    && apt-get install -yq vim vim-doc xtail software-properties-common libsecret-1-dev \
    # Install libs which is need for this project
    && apt-get install -y fuse3 python3-pip \
    # Clean up
    && apt-get autoremove -y && apt-get clean -y \
    # Move first run notice to right spot
    && mkdir -p "/usr/local/etc/vscode-dev-containers/" \
    && mv -f /tmp/scripts/first-run-notice.txt /usr/local/etc/vscode-dev-containers/

# Install DuckDB
RUN wget -O /tmp/duckdb.zip https://github.com/duckdb/duckdb/releases/download/v1.2.1/duckdb_cli-linux-aarch64.zip
RUN unzip -o /tmp/duckdb.zip -d /usr/local/bin/

# Instal awslocal
RUN pip install awscli
RUN pip install awscli-local

# Default to bash shell (other shells available at /usr/bin/fish and /usr/bin/zsh)
ENV SHELL=/bin/zsh \
    DOCKER_BUILDKIT=1

# Remove scripts now that we're done with them
RUN apt-get clean -y && rm -rf /tmp/scripts

# Mount for docker-in-docker 
VOLUME [ "/var/lib/docker" ]

CMD [ "sleep", "infinity" ]

# [Optional] Install debugger for development of Codespaces - Not in resulting image by default
ARG DeveloperBuild
RUN if [ -z $DeveloperBuild ]; then \
        echo "not including debugger" ; \
    else \
        curl -sSL https://aka.ms/getvsdbgsh | bash /dev/stdin -v latest -l /vsdbg ; \
    fi
