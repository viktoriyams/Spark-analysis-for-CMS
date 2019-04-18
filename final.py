#! /usr/bin/env python

import matplotlib
matplotlib.use("Agg")
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import count, desc, max, min, datediff, lag, sum, col, collect_list, udf
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql.window import Window
import ast
import numpy as np
import argparse

spark = SparkSession.builder.appName("victoria").getOrCreate()

# Parsing arguments
parser = argparse.ArgumentParser()
parser.add_argument('site', help='Select CMS site, example: "nebraska"')
parser.add_argument('year', help='Select year for analysis, example: 2018')
parser.add_argument('n',type=int,  help='amount of months for analysis')
args = parser.parse_args()

def intervals(times):
    times = sorted(times)
    return [int(times[i+1])-int(times[i]) for i in range(0, len(times)-1)]

intervals_udf = udf(intervals)

def chek(val):
    return (val != 0 and val != 1)

chek_udf = udf(chek)

df = spark.read.format("csv").option("header", True).load("/cms/users/asciaba/test%s/access_year=%s/month=1" %(args.site, args.year))

for i in range(2,args.n+1):
    df_0 = spark.read.format("csv").option("header", True).load("/cms/users/asciaba/test%s/access_year=%s/month=%d" %(args.site, args.year, i))
    df = df.union(df_0)

df_int = df.groupBy("Filename").agg(intervals_udf(collect_list("AccessTime")).alias("Intervals")).orderBy("Filename")
n_all = df_int.count()

def func_int(time):
    def comparison2(intervals):
        arr = ast.literal_eval(intervals)
        if len(arr) == 0:
            return 0
        if len(arr)>1:
            i=0
            while (arr[0]<time and i < len(arr)-1):
                if arr[i]<time:
                    arr[i+1]+=arr[i]
                    del arr[i]
                else:
                    i +=1
        if ((len(arr)==1 and arr[0]<time) or len(arr) == 0):
            return 1
        else:
            return arr
    comparison2_udf = udf(comparison2)

    df_comp = df_int.withColumn("Comparison2", comparison2_udf(col("Intervals"))).orderBy(desc("Comparison2"))
    df_cond = df_comp.where(chek_udf("Comparison2") == True)
    n_suc = df_cond.count()
    prob = n_suc*1.0/n_all
    return prob

day = 60*60*24
if args.n>=6:
    n_1 = args.n-5
else:
    n_1 = 1
x_m = range(n_1,args.n+1)
x_time = [x*day*30 for x in x_m]
y_prob = []
fig, ax_list = plt.subplots(1,1)
for i in x_time:
    y_prob.append(func_int(i))
plt.plot(x_time,y_prob)
#fig = plt.figure()
#fig.savefig("abcd.png")

plt.savefig("Probability.png")
plt.close()
for i in range(len(y_prob)):
    print "time=", x_time[i], " month=", x_m[i]," P=", y_prob[i]



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
plt.title("Distribution of time between first and last access")
#y.hist(bins = 10, range = [1,100000])
out = plt.hist(y)
plt.savefig('Hist_Distribution_of_time_between_first_and_last_access.png')
plt.close()

s = 0
for i in range (0,len(out[0])):
    s += (out[0][i]*abs(out[1][i] - out[1][i+1]))
#print out
print "Integral = ", s

sparkPDF3_1 = df.withColumn("ConsecutiveTime", (df.AccessTime - lag(df.AccessTime, -1).over(Window.partitionBy("Filename").orderBy(desc("AccessTime")))))
sparkPDF3_2 = sparkPDF3_1.select("ConsecutiveTime").orderBy(desc("ConsecutiveTime"))
newPDF3 = sparkPDF3_2.toPandas()
y=newPDF3["ConsecutiveTime"]
plt.title("Distribution of time between consecutive accesses")
y.hist(bins=100)
plt.savefig('Hist_Distribution_of_time_between_consecutive_accesses.png')
plt.close()
