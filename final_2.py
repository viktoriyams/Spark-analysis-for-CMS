#! /usr/bin/env python

import matplotlib
matplotlib.use("Agg")
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import count, desc, max, min, datediff, lag, sum, col, collect_list, udf
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
import numpy as np
import ast
import argparse

spark = SparkSession.builder.appName("victoria").getOrCreate()

# Parsing arguments
parser = argparse.ArgumentParser()
parser.add_argument('site', help='Select CMS site, example: "nebraska"')
parser.add_argument('year', help='Select year for analysis, example: 2018')
parser.add_argument('n',type=int,  help='amount of months for analysis')
parser.add_argument('time',type=int,  help='time T')
args = parser.parse_args()


df = spark.read.format("csv").option("header", True).load("/cms/users/asciaba/test%s/access_year=%s/month=1" %(args.site, args.year))

for i in range(2,args.n+1):
    df_0 = spark.read.format("csv").option("header", True).load("/cms/users/asciaba/test%s/access_year=%s/month=%d" %(args.site, args.year, i))
    df = df.union(df_0)

#PLOTS

sparkPDF1 = df.groupBy("Filename").agg(count("AccessTime").alias("NoAccess")).orderBy(desc("NoAccess"))
newPDF1 = sparkPDF1.select("NoAccess").toPandas()
y=newPDF1["NoAccess"]
plt.title("Distribution of number of accesses")
y.hist(bins=100)
plt.savefig('Hist_Distribution_of_number_of_accesses.png')
plt.close()

sparkPDF2_1 = df.groupBy("Filename").agg(min("AccessTime").alias("TimeIn"), max("AccessTime").alias("TimeOut"))
sparkPDF2_2 = sparkPDF2_1.selectExpr("Filename","(TimeOut-TimeIn) as intermediateTime").orderBy(desc("intermediateTime"))
newPDF2 = sparkPDF2_2.select("intermediateTime").toPandas()
y = newPDF2["intermediateTime"]
#plt.title("Distribution of time between first and last access")
#plt.savefig('Hist_Distribution_of_time_between_first_and_last_access.png')
#plt.close()


day = 60*60*24
y = newPDF2["intermediateTime"]
fig, ax = plt.subplots()
y_max = newPDF2["intermediateTime"][0]
bins = int(round(y_max/day))
out = plt.hist(y,bins = bins, range = [0, y_max], normed = True)
ax.xaxis.set_major_locator(ticker.MultipleLocator(out[1][1] - out[1][0]))
ax.set_xticklabels(range(-1, len(out[1])+1))
ax.set_xlim([0, y_max])
fig.set_figwidth(10)
plt.title("Distribution of time between first and last access")
plt.savefig("Hist_Distribution_of_time_between_first_and_last_access.png")
plt.close()

s = 0
for i in range (args.time,len(out[0])): 
    s += (out[0][i]*(out[1][i+1] - out[1][i]))
#print out
print "Probability = ", s


sparkPDF3_1 = df.withColumn("ConsecutiveTime", (df.AccessTime - lag(df.AccessTime, -1).over(Window.partitionBy("Filename").orderBy(desc("AccessTime")))))
sparkPDF3_2 = sparkPDF3_1.select("ConsecutiveTime").orderBy(desc("ConsecutiveTime"))
newPDF3 = sparkPDF3_2.toPandas()
y=newPDF3["ConsecutiveTime"]
plt.title("Distribution of time between consecutive accesses")
y.hist(bins=100)
plt.savefig('Hist_Distribution_of_time_between_consecutive_accesses.png')
plt.close()
