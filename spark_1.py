from pyspark import SparkContext, SQLContext, SparkConf, StorageLevel
# import numpy as np
# from __future__ import division
from decimal import Decimal
# import matplotlib.pyplot as plt
from collections import namedtuple

saprkconf = SparkConf().setAppName("ODI").setMaster("local[3]")
sc = SparkContext(conf=saprkconf)
# print(sc)
caption_odi = sc.textFile("/home/aman/Downloads/annual-enterprise-survey-2020-financial-year-provisional-csv.csv", 4,
                          use_unicode=True).repartition(6)
# print(caption_odi.take(10))
fields = ("Year", "Industry_aggregation", "Industry_code", "Industry_name_NZSIOC", "Units", "Variable_code")
Industry = namedtuple('Industry', fields)


def parserec(line):
    fields = line.split(',')
    return Industry(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5])


industry = caption_odi.map(lambda rec: parserec(rec))
print(industry.take(5))
