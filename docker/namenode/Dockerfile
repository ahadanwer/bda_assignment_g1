FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

MAINTAINER Ivan Ermilov <ivan.s.ermilov@gmail.com>

# Fix Debian Stretch repository URLs (moved to archive)
USER root
RUN echo "deb http://archive.debian.org/debian/ stretch main" > /etc/apt/sources.list && \
    echo "deb http://archive.debian.org/debian-security/ stretch/updates main" >> /etc/apt/sources.list

# Install Python 3
RUN apt-get update && \
    apt-get install -y --no-install-recommends --allow-unauthenticated python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create symlink for python command
RUN ln -sf /usr/bin/python3 /usr/bin/python
