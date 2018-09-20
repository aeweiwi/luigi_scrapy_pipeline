import os
import os.path
import subprocess
import datetime
import luigi
from luigi.contrib.external_program import ExternalProgramTask

"""
to run it use the following command

PYTHONPATH='.:./scrapy_shobiddak' luigi --module crawl_task CrawlTask --local-scheduler

"""
class CrawlTask(luigi.Task):
    """
    Crawl a specific city.
    """

    #today_str = datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d')
    #date = luigi.DateParameter(default=today)
    today_str = luigi.Parameter()
    def output(self):

        output_path = "./data_{0}.csv".format(self.today_str)
        return luigi.LocalTarget(output_path)

    def run(self):


        subprocess.check_output(["scrapy", "crawl", "scrapy_shobiddak",
                                 "-o", self.output().path, "-t", "csv"])


class CrawlShellTask(ExternalProgramTask):
    today_str = luigi.Parameter()

    def program_args(self):
        return ["scrapy", "crawl", "scrapy_shobiddak",
                "-o", self.output().path, "-t", "csv"]

    def output(self):
        output_path = "./data_{0}.csv".format(self.today_str)
        return luigi.LocalTarget(output_path)
