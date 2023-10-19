ARG OS_VER=3.6.1
ARG PYTHON_VER=3.11.5
FROM --platform=linux/amd64 nrel/openstudio:$OS_VER

RUN curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xvj bin/micromamba && \
    mv bin/micromamba /usr/local/bin/ && \
    rm -rf bin && \
    micromamba shell init -s bash -p /opt/micromamba && \
    micromamba config append channels conda-forge && \
    micromamba config append channels nodefaults && \
    micromamba config set channel_priority strict
COPY . /buildstock-batch/
RUN eval "$( micromamba shell hook --shell=bash /opt/micromamba )" && \
    micromamba activate /opt/micromamba && \
    micromamba install -y python=$PYTHON_VER && \
    python -m pip install "/buildstock-batch[aws]"



# sed -i '/[ -z "\$PS1" ] && return/d' /root/.bashrc
