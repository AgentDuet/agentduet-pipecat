"""Repo-level pytest setup.

nltk (a pipecat transitive dependency) installs an import-security hook that
blocks imports it resolves "from the CWD" (CWE-427 mitigation). It
false-positives on the standard project-local `.venv` layout — site-packages
lives under the CWD — which breaks `import pipecat.pipeline.worker` at test
collection. Disable the hook for test runs; it protects against CWD module
shadowing, which is not a risk in this repo's controlled test environment.
"""

import os

os.environ.setdefault("NLTK_DISABLE_IMPORT_SECURITY", "1")
