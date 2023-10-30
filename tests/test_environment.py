import os
from unittest import mock
from unittest.mock import patch

from ena_portal_api import ena_handler

class TestEnaEnvironment:
    @mock.patch.dict(
        os.environ, {"ENA_API_USER": "username", "ENA_API_PASSWORD": "password"}
    )
    def test_authentication_set(self):
        ena = ena_handler.EnaApiHandler()
        assert ena.auth == ("username", "password")

    def test_authentication_set_in_constructor(self):
        ena = ena_handler.EnaApiHandler(username="username1", password="password1")
        assert ena.auth == ("username1", "password1")
        assert ena.auth != ("username", "password")

    def test_authentication_not_set(self):
        if "ENA_API_USER" in os.environ:
            del os.environ["ENA_API_USER"]
        if "ENA_API_PASSWORD" in os.environ:
            del os.environ["ENA_API_PASSWORD"]
        ena = ena_handler.EnaApiHandler()
        assert ena.auth is None
