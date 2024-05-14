ARG OS_VER
FROM --platform=linux/amd64 nrel/openstudio:$OS_VER as buildstockbatch
ARG CLOUD_PLATFORM=aws

RUN sudo apt update && sudo apt install -y python3-pip
RUN sudo -H pip install --upgrade pip
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
