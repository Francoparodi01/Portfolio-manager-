import importlib

import pytest


@pytest.mark.parametrize(
    "module_name",
    [
        "src.analysis.rotation_engine",
        "src.core.credentials",
    ],
)
def test_removed_orphan_module_not_importable(module_name):
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(module_name)
