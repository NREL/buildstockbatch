ARG OS_VER=3.5.0
FROM --platform=linux/amd64 nrel/openstudio:$OS_VER

RUN sudo apt update && sudo apt install -y python3-pip
RUN sudo -H pip install --upgrade pip
COPY . /buildstock-batch/
RUN python3 -m pip install "/buildstock-batch[aws]"
