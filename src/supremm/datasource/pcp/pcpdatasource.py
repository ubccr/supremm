import os
import shutil
import time
import logging

from supremm.datasource.datasource import Datasource
from supremm.datasource.pcp.pcparchive import extract_and_merge_logs
from supremm.datasource.pcp.pcpsummarize import PCPSummarize
from supremm.errors import ProcessingError

class PCPDatasource(Datasource):
    """ Instance of a PCP datasource class """

    def __init__(self, preprocs, plugins):
        super().__init__(preprocs, plugins)

    def presummarize(self, job, conf, resconf, opts):
        jobmeta = super().presummarize(job, conf, resconf, opts)

        # Error with general presummarize, don't try datasource specific checks
        if jobmeta.result != 0 and jobmeta.error != None:
            return jobmeta
        else:
            mergestart = time.time()
            if not job.has_any_archives():
                jobmeta.result = 1
                jobmeta.mdata["skipped_noarchives"] = True
                jobmeta.error = ProcessingError.NO_ARCHIVES
                jobmeta.missingnodes = job.nodecount
                logging.info("Skipping %s, skipped_noarchives", job.job_id)
            elif not job.has_enough_raw_archives():
                jobmeta.result = 1
                jobmeta.mdata["skipped_rawarchives"] = True
                jobmeta.error = ProcessingError.RAW_ARCHIVES
                missingnodes = job.nodecount
                logging.info("Skipping %s, skipped_rawarchives", job.job_id)
            else:
                jobmeta.result = extract_and_merge_logs(job, conf, resconf, opts)
                missingnodes = -1.0 * jobmeta.result

        mergeend = time.time()
        jobmeta.mdata["mergetime"] = mergeend - mergestart

        if opts['extractonly']:
            if jobmeta.result == 0:
                return None
            else:
                logging.error("Failure extracting logs for job %s", job.job_id)
                return None

        return jobmeta

    def summarizejob(self, job, jobmeta, config, opts):
        preprocessors, analytics = super().summarizejob(job, jobmeta, config, opts)

        s = PCPSummarize(preprocessors, analytics, job, conf, opts["fail-fast"])

        enough_nodes = False

        if 0 == jobmeta.result or (job.nodecount !=0 and (jobmeta.missingnodes / job.nodecount < 0.05)):
            enough_nodes = True
            logging.info("Success for %s files in %s (%s/%s)", job.job_id, job.jobdir, jobmeta.missingnodes, job.nodecount)
            s.process()
        elif jobmeta.error == None and job.nodecount != 0 and (jobmeta.missingnodes / job.nodecount >= 0.5):
            # Don't overwrite existing error
            # Don't have enough node data to even try summarization
            jobmeta.mdata["skipped_pmlogextract_error"] = True
            logging.info("Skipping %s, skipped_pmlogextract_error", job.job_id)
            jobmeta.error = ProcessingError.PMLOGEXTRACT_ERROR

        if opts['tag'] != None:
            jobmeta.mdata['tag'] = opts['tag']

        if jobmeta.missingnodes > 0:
            jobmeta.mdata['missingnodes'] = self.missingnodes

        success = s.good_enough()

        if not success and enough_nodes:
            # We get here if the pmlogextract step gave us enough nodes but summarization didn't succeed for enough nodes
            # All other "known" errors should already be handled above.
            jobmeta.mdata["skipped_summarization_error"] = True
            logging.info("Skipping %s, skipped_summarization_error", job.job_id)
            jobmeta.error = ProcessingError.SUMMARIZATION_ERROR

        force_success = False
        if not success:
            force_timeout = opts['force_timeout']
            if (datetime.datetime.now() - job.end_datetime) > datetime.timedelta(seconds=force_timeout):
                force_success = True

        return s, jobmeta.mdata, success or force_success, jobmeta.error

    def cleanup(opts, job):
        if opts['dodelete'] and job.jobdir is not None and os.path.exists(job.jobdir):
            # Clean up
            shutil.rmtree(job.jobdir, ignore_errors=True)
