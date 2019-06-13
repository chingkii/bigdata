#!/usr/bin/env python

from pyspark import SparkContext

def main():
     sc = SparkContext(appName="2WordCount")
     input_file = sc.textFile('/user/cloudera/wp/wordcount.txt')
     token = input_file.map(lambda line : line.strip().split(" "))
     words = token.flatMap(lambda xs : (tuple(x) for x in zip(xs, xs[1:])))
     biwords = words.map(lambda x: (x, 1))
     wp = biwords.reduceByKey(lambda a, b: a + b)
     wp.saveAsTextFile('/user/cloudera/wp/output')
     sc.stop()
     
if __name__ == '__main__':
     main()
