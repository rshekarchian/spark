#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import sys
from random import random
from operator import add

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf


def create_sc():
    sc_conf = SparkConf()
    sc_conf.setAppName("finance-similarity-app")
    sc_conf.set("spark.dynamicAllocation.enabled", "false")
    sc_conf.set("spark.driver.host", "172.31.85.37")
    sc_conf.set('spark.executor.memory', '1g')
    sc_conf.set('spark.executor.cores', '2')
    sc_conf.set('spark.cores.max', '4')
    sc_conf.set('spark.logConf', True)
    print(sc_conf.getAll())

    sc = None
    try:
        sc.stop()
        sc = SparkContext(conf=sc_conf)
    except:
        sc = SparkContext(conf=sc_conf)

    return sc


if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """

    sc = create_sc()
    data = [1, 2, 3, 4, 5]
    distData = sc.parallelize(data)
    sum = distData.reduce(lambda a, b: a + b)

