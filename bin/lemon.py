#!/usr/bin/python

import re, os, sys, ConfigParser, logging, httplib, time
import logging.handlers as log_handler
from json import dumps as json
from datetime import datetime
from daemon import Daemon


class Lemon(Daemon):
    """
    This daemonized class instance will periodically check certain kernel interfaces in /proc
    for Lustre I/O metrics and relates these to SLURM job IDs. Check lemon.ini to make changes
    to the instance parameters like server and metric settings.
    """
    def __init__(self, regexp, metric_map, pid):
        Daemon.__init__(self, pid, stdin="/dev/null", stdout="/dev/null", stderr="/dev/null")
        self.regexp = regexp
        self.metric_map = metric_map
        self.read_config("/usr/local/etc/lemon.ini")


    def run(self):
        """
        Start daemonizing the instance.
        """
        if self.config.get("sampling", "interval"):
            interval = int(self.config.get("sampling", "interval"))
            self.logger.debug("Sample rate configuration: %i" % (interval))
        else:
            self.logger.critical("Invalid sample rate configured")
            sys.exit(1)
        self.start_stamp = datetime.now()
        try:
            server = self.config.get("opentsdb", "server")
            port = self.config.get("opentsdb", "port")
            self.tsdbcon = httplib.HTTPConnection("%s:%s" % (server, port))
            while True:
                run_stamp = datetime.now()
                # Check if time between the start timestamp and 'now' is less or equal than the interval
                if (datetime.now() - self.start_stamp).seconds <= interval:
                    self.logger.debug("Running metric check on %s second interval" % (interval))
                    self.scan_directory()
                # Calculate the run time of the above (to debug if the interval gave enough time to process)
                runtime = datetime.now() - run_stamp
                self.logger.debug("Loop runtime %s" % (runtime.microseconds))
                # Start logging new start timestamp for new interval
                self.start_stamp = datetime.now()
                # Convert interval to miliseconds, subtract runtime (in miliseconds) and convert to seconds
                time.sleep(max(0,(interval*1000000-runtime.microseconds)/1000000.0))
        except:
            self.logger.exception("Could not send metrics to server (server unreachable?)")
        self.tsdbcon.close()


    def read_config(self, config_src):
        """
        Read out the lemon.ini config which will be used for
        dynamic environment variables.
        """
        self.config = ConfigParser.ConfigParser()
        try:
            self.config.readfp(open(config_src))
        except IOError:
            print("%s not found" % (config_src))
            sys.exit(1)
        finally:
            self.start_logging()


    def start_logging(self):
        """
        Start logging (file rotation enabled), based on the settings in lemon.ini.
        """
        log_file = self.config.get("logging", "log_file")
        max_size = self.config.get("logging", "max_size")
        backup_count = self.config.get("logging", "backup_count")
        log_level = self.config.get("logging", "log_level").upper()

        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        handler = log_handler.RotatingFileHandler(log_file, maxBytes=max_size*1024*1024, backupCount=backup_count)
        handler.setFormatter(formatter)
        self.logger = logging.getLogger("lemon")
        self.logger.addHandler(handler)

        if log_level == "DEBUG":
            self.logger.setLevel(logging.DEBUG)
        elif log_level == "INFO":
            self.logger.setLevel(logging.INFO)
        elif log_level == "WARNING":
            self.logger.setLevel(logging.WARNING)
        elif log_level == "ERROR":
            self.logger.setLevel(logging.ERROR)
        elif log_level == "CRITICAL":
            self.logger.setLevel(logging.CRITICAL)
        else:
            print("No valid log level selected in %s" % (self.config_src))
            sys.exit(1)
        self.logger.debug("Log started/stopped")


    def scan_directory(self):
        """
        Walk through the directories listed in lemon.ini and pass it to the
        method that will read the content of the 'job_stats' data.
        """
        lustre_paths = self.config.get("lustre", "lustre_paths")
        lustre_paths = lustre_paths.split()
        for lustre_path in lustre_paths:
            if os.path.isdir(lustre_path):
                ls = os.listdir(lustre_path)
                for directory in ls:
                    if os.path.isdir(lustre_path + directory):
                        self.read_metrics(lustre_path, directory)


    def read_metrics(self, lustre_path, directory):
        """
        Read out the content of 'job_stats' and pass through the data to be
        parsed.
        """
        self.logger.debug("Reading metrics in %s%s" % (lustre_path, directory))
        with open(lustre_path + directory + "/job_stats", "r+w") as job_stats:
            self.content = job_stats.read()
            job_stats.write("clear\n")
        self.parse_metrics(directory)


    def parse_metrics(self, directory):
        """
        Parse the received data with a regexp and pack it into a list of dicts,
        which is then passed to be send to OpenTSDB. The average value is
        computed from {sum of values}/{count of events}, this computation is
        left to the programmer since it isn't possible to do floating-point
        math in the kernel.
        """
        matches = self.regexp.findall(self.content)
        metric_prefix = self.config.get("opentsdb", "metric_prefix")
        metric_dicts = []
        for match in matches:
            directory_s = directory.split("-")
            match_dict = {"fs": directory_s[0],
                          "dev": directory_s[1],
                          "job_id": match[0],
                          "snapshot_time": match[1],
                          "metric_prefix": metric_prefix}
            for index, metric_name in self.metric_map.iteritems():
                if metric_name == "read_bytes.sum":
                    # Send the read_bytes.sum unaltered
                    metric_dicts.append(self.compile_metric_dict(match[index], metric_name, match_dict))
                    # Rename metric_name, because we're now going to send the average of read_bytes.sum
                    metric_name = "read_bytes.avg"
                    # Get the sample rate to compute the average read sum
                    samples = int(match[2])
                elif metric_name == "write_bytes.sum":
                    # Send the write_bytes.sum unaltered
                    metric_dicts.append(self.compile_metric_dict(match[index], metric_name, match_dict))
                    # Rename metric_name, because we're now going to send the average of write_bytes.sum
                    metric_name = "write_bytes.avg"
                    # Get the sample rate to compute the average write sum
                    samples = int(match[7])
                # Prevent devide by 0 and send remaining metric_map elements (including the sum averages)
                if "samples" in locals() and samples != 0:
                    metric_dicts.append(self.compile_metric_dict(match[index], metric_name, match_dict, samples))
                else:
                    metric_dicts.append(self.compile_metric_dict(match[index], metric_name, match_dict))
        self.send_metrics(metric_dicts)


    def compile_metric_dict(self, metric_value, metric_name, match_dict, samples=1):
        """
        This generic method is used to compute the averages for the sums and return
        the results in a dictionary. The sum is only calculated when the sample rate
        is declared at method invocation.
        """
        # Set timestamp based on daemon interval (correct total values in Grafana)
        if self.config.getboolean("sampling", "align_timestamps"):
            timestamp = time.mktime(self.start_stamp.timetuple())
        # Set timestamp based on value in job_stats (inaccurate total values in Grafana)
        else:
            timestamp = match_dict["snapshot_time"]
        # The sum is accumulated over the period of the interval, so let's {sum of values}/{interval}
        if metric_name.endswith("sum"):
            interval = int(self.config.get("sampling", "interval"))
        else:
            interval = 1
        metric_dict = {"timestamp": timestamp,
                       "metric": "%s.%s" % (match_dict["metric_prefix"], metric_name),
                       "value": (float(metric_value) / samples) / interval,
                       "tags": {"fs": match_dict["fs"],
                                "job_id": match_dict["job_id"],
                                "dev": match_dict["dev"]}}
        return(metric_dict)


    def send_metrics(self, metric_dicts):
        """
        If the received list contains elements, it's converted to JSON and send
        to OpenTSDB via a HTTP POST message.
        """
        if metric_dicts:
            try:
                self.tsdbcon.request("POST","/api/put",json(metric_dicts))
                self.tsdbcon.getresponse().read()
                self.logger.info("%s metrics send to OpenTSDB" % (len(metric_dicts)))
            except httplib.ImproperConnectionState:
                try:
                    self.tsdbcon.close()
                    self.tsdbcon.connect()
                except:
                    pass
            except Exception as e:
                self.logger.warning(sys.exc_info()[0])
            for metric in metric_dicts:
                self.logger.debug(metric)


def main():
    """
    The regexp filters out the following fields. An 'overlay' structure called metric_map in turn
    maps the index of the regex to the metric_name. Which is used in the JSON data structure, sent
    to OpenTSDB.

    ('72',  '1511963334',  '1',          'bytes', '4096', '4096', '4096', '0',           'bytes', '0', '0', '0')
    job_id, snapshot_time, read_samples, unit,    min,    max,    sum,    write_samples, unit,    min, max, sum
    0       1              2             3        4       5       6       7              8        9    10   11
    """
    regexp = re.compile(r'^-\sjob_id:\s+(\d+)\n'
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
    #metric_map = {0: "job_id", 1: "snapshot_time", 2: "read_samples",
    #              3: "unit", 4: "min", 5: "max", 6: "sum",
    #              7: "write_samples", 8: "unit", 9: "min",
    #              10: "max", 11: "sum"}
    metric_map = {2: "read_bytes.samples", 4: "read_bytes.min", 5: "read_bytes.max", 6: "read_bytes.sum",
                  7: "write_bytes.samples", 9: "write_bytes.min", 10: "write_bytes.max", 11: "write_bytes.sum"}

    pid = os.path.basename(__file__)
    pid = pid.split(".")[0]
    pid = "/run/%s.pid" % (pid)
    daemon = Lemon(regexp, metric_map, pid)

    if len(sys.argv) == 2:
        if "start" == sys.argv[1]:
            daemon.start()
        elif "stop" == sys.argv[1]:
            daemon.stop()
        elif "restart" == sys.argv[1]:
            daemon.restart()
        else:
            print("Unknown command")
            sys.exit(1)
        sys.exit(0)
    else:
        print("Usage: %s start|stop|restart" % (sys.argv[0]))
        sys.exit(1)


if __name__ == "__main__":
    main()
