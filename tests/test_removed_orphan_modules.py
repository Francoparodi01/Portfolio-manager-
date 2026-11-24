import importlib

import pytest


@pytest.mark.parametrize(
    "module_name",
    [
        "src.analysis.rotation_engine",
    ],
)
def test_removed_orphan_module_not_importable(module_name):
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(module_name)


def test_credentials_module_is_reintroduced_for_multiuser_security():
    module = importlib.import_module("src.core.credentials")

    assert hasattr(module, "CredentialCipher")
