# -*- coding: utf-8 -*-

import docker
import os


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    client = docker.from_env()
    client.images.build(
        path=here,
        tag='nrel/buildstockbatch',
        rm=True
    )


if __name__ == '__main__':
    main()
