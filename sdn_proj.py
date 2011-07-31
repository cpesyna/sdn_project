#!/usr/bin/python

import threading
from os import path, makedirs
from sys import argv
import urllib2
import BeautifulSoup
from nltk import clean_html

BASE_URL = "http://forums.studentdoctor.net/"

def main():
    try:
        input_file = open(argv[1])
    except IOError:
        print "Can't open " + argv[1]

    if not path.exists(output_dirname):
        print "Creating output directory: %s" % output_dirname
        makedirs(output_dirname)

    O


if __name__ == '__main__':
    main()
