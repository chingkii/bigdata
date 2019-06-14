#!/usr/bin/env python

from operator import itemgetter
import sys
import collections

if __name__ == '__main__':
    WC = dict()
    for line in sys.stdin:
        line = line.strip()
        word, count = line.split('\t',1)
        count = int(count)

        if word in WC:
            WC[word] = WC[word] + count
        else:
            WC[word] = count

    for words in words_count.key():
        sys.stdout.write('{0}\t{1}\n'.format(word,WC[word]))

