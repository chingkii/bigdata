
#!/usr/bin/env python

from pyspark import SparkContext

def main():
    sc = SparkContext(appName="2WC")
    input_text = sc.textFile('user/cloudera/wz/test_wordcount.txt')
    cut = input_text.map(lambda line : line.strip().split(" "))
    word = cut.flatMap(lambda xs : (tuple(x) for x in zip(xs, xs[1:])))
    dictword = word.map(lambda x: (x, 1))
    wz = dictword.reduceByKey(lambda a, b: a + b)
    wz.saveAsTextFile('user/cloudera/wz/output')
    sc.stop()

if __name__ == '__main__':
    main()
    
