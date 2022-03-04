import sys
from mock import patch
import pytest

from supremm import summarize_jobs
from tests.integration_tests.mock_preprocessor import MockPreprocessor
from tests.integration_tests.throwing_plugin import InitThrowingPlugin, ProcessThrowingPlugin, ResultsThrowingPlugin


@pytest.mark.parametrize("threads", [1, 3])
def test_plugin_api(threads):
    test_args = "summarize_jobs.py -d -r 2 -j 972366 --fail-fast --threads {}".format(threads).split()
    preprocs = [MockPreprocessor]
    plugins = []
    # this was very non-obvious to me but since summarize_jobs does "from supremm.plugin import loadpreprocs"
    # you have to patch loadpreprocs as if it was in the summarize_jobs module
    with patch.object(sys, "argv", test_args), patch("supremm.summarize_jobs.loadpreprocessors",  return_value=preprocs), patch("supremm.summarize_jobs.loadplugins", return_value=plugins):
        summarize_jobs.main()


@pytest.mark.parametrize("threads", [1, 3])
def test_exception_init(threads):
    test_args = "summarize_jobs.py -d -r 2 -j 972366 --threads {}".format(threads).split()
    plugins = [InitThrowingPlugin]
    preprocs = []
    with patch.object(sys, "argv", test_args), patch("supremm.summarize_jobs.loadpreprocessors",  return_value=preprocs), patch("supremm.summarize_jobs.loadplugins", return_value=plugins):
        summarize_jobs.main()


@pytest.mark.parametrize("threads", [1, 3])
def test_exception_process(threads):
    test_args = "summarize_jobs.py -d -r 2 -j 972366 --threads {}".format(threads).split()
    plugins = [ProcessThrowingPlugin]
    preprocs = []
    with patch.object(sys, "argv", test_args), patch("supremm.summarize_jobs.loadpreprocessors",  return_value=preprocs), patch("supremm.summarize_jobs.loadplugins", return_value=plugins):
        summarize_jobs.main()


@pytest.mark.parametrize("threads", [1, 3])
def test_exception_results(threads):
    test_args = "summarize_jobs.py -d -r 2 -j 972366 --threads {}".format(threads).split()
    plugins = [ResultsThrowingPlugin]
    preprocs = []
    with patch.object(sys, "argv", test_args), patch("supremm.summarize_jobs.loadpreprocessors",  return_value=preprocs), patch("supremm.summarize_jobs.loadplugins", return_value=plugins):
        summarize_jobs.main()
