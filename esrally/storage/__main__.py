import logging

import esrally.storage
from esrally import config


def main():
    logging.basicConfig(level=logging.DEBUG)
    # cache_url="s3://es-perf-fressi-eu-central-1/"
    manager = esrally.storage.Manager.from_config(config.Config())
    try:
        tr1 = manager.get("https://ftp.rediris.es/mirror/ubuntu-releases/24.04.2/ubuntu-24.04.2-desktop-amd64.iso")
        # tr2 = manager.get("https://ftp.rediris.es/mirror/ubuntu-releases/24.04.2/ubuntu-24.04.2-live-server-amd64.iso")
        # tr3 = manager.get("https://ftp.rediris.es/mirror/ubuntu-releases/24.04.2/ubuntu-24.04.2-live-server-amd64.zip")
        tr1.wait()
        # tr2.wait()
        # tr3.wait()
    except KeyboardInterrupt:
        pass
    finally:
        manager.shutdown()


main()
