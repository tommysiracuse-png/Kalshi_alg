"""Compatibility entrypoint for :mod:`apps.filter_csv`."""

import runpy

if __name__ == "__main__":
    runpy.run_module("apps.filter_csv", run_name="__main__")
