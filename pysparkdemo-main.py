#
# pysparkdemo-main.py - by George K. Thiruvathukal
#
# This is a standalone version of pysparkdemo.ipynb, which is a Jupyter notebook I used to lecture
# about PySpark basics in a group meeting at Argonne. There are quite a few ideas here:
#
# - working with RDDs
# - using command-line arguments to avoid hard-coding stuff that is job specific
# - convincing yourself that code is really running on the cluster
# - interfacing with the os (doing some dirt simple parallel I/O within RDD)
#
# You can launch this code as follows:
# - <path-to>/bin/spark-submit  simple-main.py  --size 512 --nodes 1 --cores 4
# - cluster.sh (if you are on a Cobalt cluster)
#

import argparse
import os
import sys
import socket
import pprint

from pyspark import SparkContext, SparkConf


def driver(args, sc):
    python_version = sys.version
    spark_python_version = sc.pythonVer

    pp = pprint.PrettyPrinter(indent=4)
    rdd = sc.parallelize(range(0, args.size), args.nodes * args.cores)

    rdd.reduce(lambda x, y: x + y)

    rdd_hostnames = rdd.map(lambda id: (socket.gethostname(),))

    hosts_used = rdd_hostnames.reduce(lambda x, y: x + y)
    unique_hosts = set(hosts_used)
    pp.pprint(unique_hosts)
    print("python_version = %(python_version)s; spark_python_version = %(spark_python_version)s" % vars())

    rdd2 = rdd.map(lambda dir_id: create_dir_id(dir_id))
    rdd2.cache()

    rdd3 = rdd2.map(lambda pathname: ('%s:%s' %
                                      (socket.gethostname(),  pathname),))
    result = rdd3.reduce(lambda x, y: x + y)
    pp.pprint(result[:min(args.maxlines, len(result))])

    rdd4 = rdd2.map(lambda pathname: (touch_file(pathname),))
    result = rdd4.reduce(lambda x, y: x + y)
    pp.pprint(result[:min(args.maxlines, len(result))])


def create_dir_id(n):
    my_id = str(n)
    dirname = os.path.join("/scratch/SE_HPC", my_id)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    return dirname


def touch_file(pathname):
    datafile = os.path.join(pathname, "data.txt")
    os.system("touch %s" % datafile)
    return datafile


def bootstrap():
    sconf = SparkConf()
    sc = SparkContext(conf=sconf)
    return sc


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--size', help="workload size", type=int, default=1000)
    parser.add_argument('--cores', help="cores per node", type=int, default=12)
    parser.add_argument('--nodes', help="nodes on cluster",
                        type=int, default=1)
    parser.add_argument('--maxlines', help="don't print more than maxlines (just show size)",
                        type=int, default=100)
    return parser.parse_args()


def main():
    args = get_args()
    sc = bootstrap()
    driver(args, sc)


if __name__ == '__main__':
    main()
