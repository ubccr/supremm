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
        super().presummarize(job, conf, resconf, opts)

        if self.result != 0 and self.error != None:
            return
        else:
            mergestart = time.time()
            if not job.has_any_archives():
                result = 1
                self.mdata["skipped_noarchives"] = True
                error = ProcessingError.NO_ARCHIVES
                missingnodes = job.nodecount
                logging.info("Skipping %s, skipped_noarchives", job.job_id)
            elif not job.has_enough_raw_archives():
                result = 1
                self.mdata["skipped_rawarchives"] = True
                error = ProcessingError.RAW_ARCHIVES
                missingnodes = job.nodecount
                logging.info("Skipping %s, skipped_rawarchives", job.job_id)
            else:
                result = extract_and_merge_logs(job, conf, resconf, opts)
                missingnodes = -1.0 * self.result

        mergeend = time.time()
        self.mdata["mergetime"] = mergeend - mergestart

        if opts['extractonly']:
            if result == 0:
                return None
            else:
                logging.error("Failure extracting logs for job %s", job.job_id)
                return None

    def summarizejob(self, job, config, opts):
        preprocessors, analytics = super().summarizejob(job)

        s = PCPSummarize(preprocessors, analytics, job, conf, opts["fail-fast"])

        enough_nodes = False

        if 0 == self.result or (job.nodecount !=0 and (self.missingnodes / job.nodecount < 0.05)):
            enough_nodes = True
            logging.info("Success for %s files in %s (%s/%s)", job.job_id, job.jobdir, self.missingnodes, job.nodecount)
            s.process()
        elif self.error == None and job.nodecount != 0 and (self.missingnodes / job.nodecount >= 0.5):
            # Don't overwrite existing error
            # Don't have enough node data to even try summarization
            self.mdata["skipped_pmlogextract_error"] = True
            logging.info("Skipping %s, skipped_pmlogextract_error", job.job_id)
            self.error = ProcessingError.PMLOGEXTRACT_ERROR

        if opts['tag'] != None:
            self.mdata['tag'] = opts['tag']

        if self.missingnodes > 0:
            self.mdata['missingnodes'] = self.missingnodes

        success = s.good_enough()

        if not success and enough_nodes:
            # We get here if the pmlogextract step gave us enough nodes but summarization didn't succeed for enough nodes
            # All other "known" errors should already be handled above.
            self.mdata["skipped_summarization_error"] = True
            logging.info("Skipping %s, skipped_summarization_error", job.job_id)
            self.error = ProcessingError.SUMMARIZATION_ERROR

        force_success = False
        if not success:
            force_timeout = opts['force_timeout']
            if (datetime.datetime.now() - job.end_datetime) > datetime.timedelta(seconds=force_timeout):
                force_success = True

        return s, self.mdata, success or force_success, self.error

    def cleanup(opts, job):
        if opts['dodelete'] and job.jobdir is not None and os.path.exists(job.jobdir):
            # Clean up
            shutil.rmtree(job.jobdir, ignore_errors=True)
