#!/usr/bin/python

import re
import os
import sys
import pika
from time import sleep
from datetime import datetime
from graphite import Graphite
from daemon import Daemon


class Lemon(Daemon):
    "Collect, parse and send Lustre job metrics"

    def __init__(self, regex, metric_dst, graphite, pid):
        Daemon.__init__(self, pid, stdin='/dev/null',
                                   stdout='/dev/null',
                                   stderr='/dev/null')
        self.regex = regex
        self.metric_dst = metric_dst
        self.content = ''
        self.match_dict = {}
        self.graphite = graphite
        if self.graphite.read_config('./lmon.cfg'):
            self._scan_directory_()

    def run(self):
        if self.graphite.config.get('sampling', 'interval'):
            interval = int(self.graphite.config.get('sampling', 'interval'))
        else:
            self.graphite.logger.critical("Invalid sample rate configured")
            sys.exit(1)

        start_stamp = datetime.now()
        try:
            while True:
                run_stamp = datetime.now()
                if (datetime.now() - start_stamp).seconds <= interval:
                    self.graphite.logger.debug("Running metric check on %s "
                                               "second interval" % (interval))
                    self._scan_directory_()

                runtime = datetime.now() - run_stamp
                self.graphite.logger.debug("Loop runtime %s"
                                           % (runtime.microseconds))
                start_stamp = datetime.now()
                sleep(max(0,(interval*1000000-runtime.microseconds)/1000000.0))
        except:
            self.graphite.logger.exception('')

    def _scan_directory_(self):
        base_paths = self.graphite.config.get('graphite', 'base_paths')
        base_paths = base_paths.split()

        for base_path in base_paths:
            if os.path.isdir(base_path):
                ls = os.listdir(base_path)
                for directory in ls:
                    if os.path.isdir(base_path + directory):
                        self._read_metrics_(base_path, directory)

    def _read_metrics_(self, base_path, directory):
        self.graphite.logger.debug("Reading metrics in %s%s" % (base_path,
                                   directory))
        with open(base_path + directory + '/job_stats', 'r+w') as job_stats:
            self.content = job_stats.read()
            job_stats.write('clear\n')
            job_stats.close()
        self.parse_metrics(directory)

    def parse_metrics(self, directory):
        matches = self.regex.findall(self.content)
        self.match_dict = {}
        for match in matches:
            self.match_dict["%s/%s" % (match[0], directory)] = match[1:]
        self.send_metrics()

    def send_metrics(self):
        metric_base_path = self.graphite.config.get('graphite',
                                                    'metric_base_path')
        server = self.graphite.config.get('graphite', 'server')
        index = 0
        for key, value in self.match_dict.items():
            for metric in self.match_dict[key]:
                if index in self.metric_dst:
                    key_s = key.split('/')
                    job_id = key_s[0]
                    fs = key_s[1]
                    time_stamp = int(value[0])
                    metric_path = ("%s.%s.%s.%s" % (metric_base_path,
                                   job_id,
                                   fs,
                                   self.metric_dst[index])) 
                    self.graphite.logger.info("%s %s %s" % (metric_path,
                                              value[index],
                                              time_stamp))
                    self.graphite.send_data(metric_path,
                                            int(value[index]),
                                            time_stamp)
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
    #metric_dst = {1: 'read.samples', 3: 'read.min', 4:'read.max',
    #              5: 'read.sum', 6: 'write.samples', 8: 'write.min',
    #              9: 'write.max', 10: 'write.sum'}
    metric_dst = { 5: 'read.sum', 10: 'write.sum'}
    pid = os.path.basename(__file__)
    pid = pid.split('.')[0]
    pid = '/var/run/%s.pid' % (pid)

    graphite = Graphite()
    daemon = Lemon(regex, metric_dst, graphite, pid)
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print("Unknown command")
            sys.exit(1)
        sys.exit(0)
    else:
        print("Usage: %s start|stop|restart" % (sys.argv[0]))
        sys.exit(1)
