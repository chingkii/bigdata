#!/usr/bin/env python

from operator import itemgetter
import sys
import collections

if __name__ == '__main__':
    for line in sys.stdin:
        word_1 = line.split()
        word_2 = collections.deque(word_1)
        word_2.rotate(1)
        word_1 = list(map(lambda a,b:a+" " +b,list(word_2),word_1))
        del word_1[0]
        for x in word_1:
            sys.stdout.write('{0}\ta\n'.format(x))
