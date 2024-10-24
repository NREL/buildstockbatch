ARG OS_VER
FROM --platform=linux/amd64 nrel/openstudio:$OS_VER as buildstockbatch
ARG CLOUD_PLATFORM=aws

RUN apt-get update && apt-get install -y \
    software-properties-common \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.11 python3.11-venv python3.11-dev

RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1
RUN update-alternatives --set python3 /usr/bin/python3.11

RUN wget https://bootstrap.pypa.io/get-pip.py && \
    python3.11 get-pip.py && \
    rm get-pip.py

RUN apt-get clean && rm -rf /var/lib/apt/lists/*

COPY . /buildstock-batch/
RUN python3 -m pip install "/buildstock-batch[${CLOUD_PLATFORM}]"

# Base plus custom gems
FROM buildstockbatch as buildstockbatch-custom-gems
RUN sudo cp /buildstock-batch/Gemfile /var/oscli/
# OpenStudio's docker image sets ENV BUNDLE_WITHOUT=native_ext
# https://github.com/NREL/docker-openstudio/blob/3.2.1/Dockerfile#L12
# which overrides anything set via bundle config commands.
# Unset this so that bundle config commands work properly.
RUN unset BUNDLE_WITHOUT
# Note the addition of 'set' in bundle config commands
RUN bundle config set git.allow_insecure true
RUN bundle config set path /var/oscli/gems/
RUN bundle config set without 'test development native_ext'
RUN bundle install --gemfile /var/oscli/Gemfile
