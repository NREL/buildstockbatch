FROM nrel/openstudio:3.6.1

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ARG DEBIAN_FRONTEND=noninteractive

RUN ln -snf /usr/share/zoneinfo/$CONTAINER_TIMEZONE /etc/localtime && echo $CONTAINER_TIMEZONE > /etc/timezone

RUN sudo apt update && \
    sudo apt install -y wget build-essential checkinstall libreadline-gplv2-dev libncursesw5-dev libssl-dev \
    libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libffi-dev zlib1g-dev

RUN wget https://www.python.org/ftp/python/3.8.8/Python-3.8.8.tgz && \
    tar xzf Python-3.8.8.tgz && \
    cd Python-3.8.8 && \
    ./configure --enable-optimizations && \
    make altinstall && \
    rm -rf Python-3.8.8 && \
    rm -rf Python-3.8.8.tgz


COPY . /buildstock-batch/
RUN python3.8 -m pip install "dask[distributed]" --upgrade
RUN python3.8 -m pip install "bokeh"
RUN python3.8 -m pip install /buildstock-batch

WORKDIR /app/