# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import atexit
import logging
import sys
import time

from esrally import actor
from esrally.storage import TransferManager, TransferStatus

LOG = logging.getLogger("esrally.storage")


def main():
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    urls = sys.argv[1:]
    if not urls:
        sys.stderr.write(f"usage: python3 '{sys.argv[0]}' <url> [urls]\n")
        raise sys.exit(1)
    atexit.register(actor.system().shutdown)
    manager = TransferManager.from_config()
    all: dict[str, TransferStatus | None] = {url: None for url in urls}
    try:
        unfinished = set(all)
        while unfinished:
            LOG.info("Downloading files: \n - %s", "\n - ".join(f"{u}: {(s := all.get(u)) and s.progress or '?'}%" for u in unfinished))
            for url in list(unfinished):
                status = manager.get(url)
                all[url] = status
                assert isinstance(status, TransferStatus)
                if status.finished:
                    unfinished.remove(url)
                    LOG.info("Download terminated: %s", status)
                    continue
            if unfinished:
                time.sleep(2.0)
    finally:
        LOG.info("Downloaded files: \n - %s", "\n - ".join(f"{u}: {s and s.progress or '?'}%" for u, s in all.items()))


if __name__ == "__main__":
    main()
