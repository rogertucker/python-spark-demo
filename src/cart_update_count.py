'''
Created on Jan 31, 2016

@author: cloudera
'''


    

from pyspark import SparkContext, SparkConf


if __name__ == '__main__':
    
    sc=SparkContext(appName="MyApp")

    f = sc.textFile("my-dir/etl")
    rdd = f.map(lambda line: line.split(",")).map(lambda record: ((record[0].split(" ")[0], record[1]), 1))
    
    rdd = rdd.reduceByKey(lambda x, y: x + y).sortByKey();
    
    for updates in rdd.collect():
        print "%s\t%s\t%s"%(updates[0][0], updates[0][1], updates[1])
    
    sc.stop()