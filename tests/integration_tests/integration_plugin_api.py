import sys
from mock import patch

from supremm import summarize_jobs
from mock_preprocessor import MockPreprocessor

def test_plugin_api():
    test_args = "summarize_jobs.py -d -r 2 -j 972366 --fail-fast".split()
    preprocs = [MockPreprocessor]
    plugins = []
    # this was very non-obvious to me but since summarize_jobs does "from supremm.plugin import loadpreprocs"
    # you have to patch loadpreprocs as if it was in the summarize_jobs module
    with patch.object(sys, "argv", test_args), patch("supremm.summarize_jobs.loadpreprocessors",  return_value=preprocs), patch("supremm.summarize_jobs.loadplugins", return_value=plugins):
        summarize_jobs.main()
