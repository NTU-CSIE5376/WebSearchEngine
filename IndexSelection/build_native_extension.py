from pathlib import Path
import os
import sys

from setuptools import setup


BASE_DIR = Path(__file__).resolve().parent

try:
    from pybind11.setup_helpers import Pybind11Extension, build_ext
except ImportError as exc:  # pragma: no cover - build-time only
    raise SystemExit(
        "pybind11 is required to build the native feature extension.\n"
        "Install it first, for example:\n"
        "  python3 -m pip install pybind11\n"
    ) from exc


ext_modules = [
    Pybind11Extension(
        "_url_features_native",
        [str(BASE_DIR / "native" / "url_features_pybind.cpp")],
        cxx_std=17,
    )
]


if __name__ == "__main__":
    os.chdir(BASE_DIR)
    setup(
        name="indexselection-v1-native-features",
        version="0.1.0",
        description="Optional pybind11 extension for IndexSelection_v1 URL feature extraction",
        ext_modules=ext_modules,
        cmdclass={"build_ext": build_ext},
        script_args=["build_ext", "--inplace", *sys.argv[1:]],
    )
