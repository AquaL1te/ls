#!/usr/bin/python

import re
import ConfigParser
import sys
import logging
import logging.handlers as log_handler

class JobStats(object):
    'Collect, parse and send Lustre job metrics'

    def __init__(self, config_src, regex, metric_src, metric_dst):
        self.config_src = config_src
        self.regex = regex
        self.metric_src = metric_src
        self.metric_dst = metric_dst
        self.config = None
        self.logger = None
        self.content = ''
        self.match_dict = {}
        self.read_config()

    def read_config(self):
        self.config = ConfigParser.ConfigParser()
        try:
            self.config.readfp(open(self.config_src))
        except IOError:
            print('%s not found' % (self.config_src))
            sys.exit(1)
        finally:
            self._start_logging_()
            self._read_metrics_()

    def _start_logging_(self):
        log_file = self.config.get('logging', 'log_file')
        max_size = self.config.get('logging', 'max_size')
        backup_count = self.config.get('logging', 'backup_count')
        log_level = self.config.get('logging', 'log_level').upper()

        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler = log_handler.RotatingFileHandler(log_file,
                                                  maxBytes=max_size*1024*1024,
                                                  backupCount=backup_count)
        handler.setFormatter(formatter)
        self.logger = logging.getLogger('lustrejobstats')
        self.logger.addHandler(handler)
        if log_level == 'DEBUG':
            self.logger.setLevel(logging.DEBUG)
        elif log_level == 'INFO':
            self.logger.setLevel(logging.INFO)
        elif log_level == 'WARNING':
            self.logger.setLevel(logging.WARNING)
        elif log_level == 'ERROR':
            self.logger.setLevel(logging.ERROR)
        elif log_level == 'CRITICAL':
            self.logger.setLevel(logging.CRITICAL)
        else:
            print('No valid log level selected in %s' % (self.config_src))
            sys.exit(1)
        self.logger.info('Log started')

    def _read_metrics_(self):
        with open(self.metric_src, 'r') as job_stats:
            self.content = job_stats.read()
            job_stats.close()
        self.parse_metrics()

    def parse_metrics(self):
        matches = self.regex.findall(self.content)
        for match in matches:
            self.match_dict[match[0]] = match[1:]
        self.send_metrics()

    def send_metrics(self):
        metric_path = self.config.get('opentsdb', 'metric_path')
        index = 0
        for key, value in self.match_dict.items():
            for metric in self.match_dict[key]:
                if index in self.metric_dst:
                    self.logger.info('%s.%s.%s %s %s' % (metric_path,
                                      self.metric_dst[index],
                                      key, value[index], value[0]))
                index += 1
            index = 0


if __name__ == '__main__':
    regex = re.compile(r'^-\sjob_id:\s+(\d+)\n'
                       r'\s+snapshot_time:\s+(\d+)\n'
                       r'\s+read_bytes.*samples:\s+(\d+)'
                       r',\sunit:\s(\w+)'
                       r',\smin:\s+(\d+)'
                       r',\smax:\s+(\d+)'
                       r',\ssum:\s+(\d+).*\n'
                       r'\s+write_bytes.*samples:\s+(\d+)'
                       r',\sunit:\s(\w+)'
                       r',\smin:\s+(\d+)'
                       r',\smax:\s+(\d+)'
                       r',\ssum:\s+(\d+)*', re.MULTILINE)
    metric_dst = {1: 'read.samples', 3: 'read.min', 4:'read.max',
                  5: 'read.sum', 6: 'write.samples', 8: 'write.min',
                  9: 'write.max', 10: 'write.sum'}

    obj = JobStats('./lmon.cfg', regex, 'jobvars2', metric_dst)
