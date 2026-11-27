# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0

from starlette.requests import Request

from openviking.server.auth.plugins.api_key import _remove_header


def test_remove_header_refreshes_an_already_materialized_request_view():
    request = Request({"type": "http", "headers": [(b"x-test", b"value"), (b"host", b"test")]})
    assert request.headers.get("x-test") == "value"

    _remove_header(request, b"x-test")

    assert "x-test" not in request.headers
    assert request.headers.get("host") == "test"
