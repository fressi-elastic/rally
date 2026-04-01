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

import functools
import re
import warnings
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

import elastic_transport
import elasticsearch
from elastic_transport.client_utils import percent_encode

from esrally.utils import versions
from esrally.version import minimum_es_version

_WARNING_RE = re.compile(r"\"([^\"]*)\"")
_COMPAT_MIMETYPE_RE = re.compile(r"application/(json|x-ndjson|vnd\.mapbox-vector-tile)")

_MIN_COMPATIBILITY_MODE = int(versions.Version.from_string(minimum_es_version()).major)
_MAX_COMPATIBILITY_MODE = int(elasticsearch.VERSION[0])
_VALID_COMPATIBILITY_MODES = tuple(range(_MIN_COMPATIBILITY_MODE, _MAX_COMPATIBILITY_MODE + 1))
assert len(_VALID_COMPATIBILITY_MODES) > 0, "There should be at least one valid compatibility mode."


def ensure_mimetype_headers(
    *,
    headers: Mapping[str, str] | None = None,
    path: str | None = None,
    body: Any | None = None,
    version: str | int | None = None,
) -> elastic_transport.HttpHeaders:
    # Ensure will use a case-insensitive copy of input headers.
    headers = elastic_transport.HttpHeaders(headers or {})

    if body is not None:
        # Newer Elasticsearch versions are picky about the content-type header when a body is present.
        # Because tracks are not passing headers explicitly, set them here.
        mimetype = "application/json"
        if path and path.endswith("/_bulk"):
            # Server version 9 is picky about this. This should improve compatibility.
            mimetype = "application/x-ndjson"
        for header in ("content-type",):  #  "accept"):
            headers.setdefault(header, mimetype)

    # Ensures compatibility mode is being applied to mime type.
    # If not doing, the vanilla client version 9 will by default ask for compatibility mode 9, which would not
    # allow connecting to server version 8 clusters.
    try:
        compatibility_mode = get_compatibility_mode(version=version)
    except ValueError as ex:
        warnings.warn(f"Invalid compatibility mode {version!r}, defaulting to None: {ex}")
        compatibility_mode = None
    if compatibility_mode is not None:
        for header in ("accept", "content-type"):
            mimetype = headers.get(header)
            if mimetype is None:
                continue
            headers[header] = _COMPAT_MIMETYPE_RE.sub(
                "application/vnd.elasticsearch+%s; compatible-with=%s" % (r"\g<1>", compatibility_mode), mimetype
            )
    return headers


def get_compatibility_mode(version: str | int | None = None, default: int | None = None) -> int | None:
    if version is None:
        # It returns the default compatibility mode.
        return default

    # Normalize version to an integer major version.
    if isinstance(version, str):
        if not versions.is_version_identifier(version):
            raise ValueError(f"Elasticsearch version {version!r} is not valid.")
        version = int(versions.Version.from_string(version).major)

    if not isinstance(version, int):
        raise TypeError(f"Version must be a valid version string or an integer, but got {version!r}.")

    if version not in _VALID_COMPATIBILITY_MODES:
        supported = ", ".join(str(v) for v in _VALID_COMPATIBILITY_MODES)
        raise ValueError(f"Elasticsearch version {version!r} is not supported, supported versions are: {supported}.")
    return version


def wrap_serverless_transport(transport: elastic_transport.Transport | elastic_transport.AsyncTransport) -> None:
    """It ensures client transport never asks for compatibility mode to serverless server."""

    _perform_request = transport.perform_request

    @functools.wraps(_perform_request)
    def wrapped_perform_request(
        *args,
        **kwargs,
    ):
        headers: Mapping[str, Any] | None = kwargs.get("headers")
        if headers is not None:
            for mimetype in ("content-type", "accept"):
                value = headers.get(mimetype)
                if not value or ";" not in value:
                    continue
                # remove any compatibility parameters from the header value, e.g.
                # "application/vnd.elasticsearch+json; compatible-with=8" -> "application/json"
                value = value.split(";")[0].strip().replace("/vnd.elasticsearch+", "/")
                headers[mimetype] = value
        return _perform_request(*args, **kwargs)

    transport.perform_request = wrapped_perform_request


def _escape(value: Any) -> str:
    """
    Escape a single value of a URL string or a query parameter. If it is a list
    or tuple, turn it into a comma-separated string first.
    """

    # make sequences into comma-separated stings
    if isinstance(value, (list, tuple)):
        value = ",".join([_escape(item) for item in value])

    # dates and datetimes into isoformat
    elif isinstance(value, (date, datetime)):
        value = value.isoformat()

    # make bools into true/false strings
    elif isinstance(value, bool):
        value = str(value).lower()

    elif isinstance(value, bytes):
        return value.decode("utf-8", "surrogatepass")

    if not isinstance(value, str):
        return str(value)
    return value


def _quote(value: Any) -> str:
    return percent_encode(_escape(value), ",*")


def _quote_query(query: Mapping[str, Any]) -> str:
    return "&".join([f"{k}={_quote(v)}" for k, v in query.items()])
