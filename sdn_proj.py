#!/usr/bin/python

import Queue
import threading
from os import path, makedirs
from sys import argv
import re
import urllib2
import BeautifulSoup

queue = Queue.Queue()
output_dirname = "./data/output/"

class Scraper(threading.Thread):
    """Threaded SDN forum scraper"""

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def extract(self, forum_id):
        BASE_URL = "http://forums.studentdoctor.net/showthread.php?t=" + forum_id + "&page="
        url = urllib2.urlopen(BASE_URL + "1")
        soup = BeautifulSoup.BeautifulSoup(url.read())
        self.school = soup.find("title").next.extract()
        self.school = self.school.split(" ")
        self.school = " ".join(self.school[2:self.school.index("|")]).replace(" Application Thread", "")
        self.school = self.school.replace("&amp;", "and")
        self.school = self.school.replace("/", "-")
        try:
            page_count = int(soup.find("td", {"class":"vbmenu_control", "style":"font-weight:normal"}).next.extract().split(" ")[-1])
        except AttributeError:
            page_count = 1
        print "Started " + self.school

        self.users = []
        self.statuses = []
        for i in xrange(1, page_count + 1):
            url = urllib2.urlopen(BASE_URL + str(i))
            soup = BeautifulSoup.BeautifulSoup(url.read())
            self.users += [item.next.extract() for item in soup.findAll("a", {"title": "You must be a registered member to view member names."})]
            for item in soup.findAll("table", {"id" : re.compile("post[0-9]*")}):
                try:
                    self.statuses.append(item.find("a" , {"href": re.compile("/memberlist.php")}).next.extract())
                except AttributeError:
                    self.statuses.append("Unknown")

    def export(self):
        outfile = open(output_dirname + self.school, "w")

        for item in zip(self.users, self.statuses):
            outfile.write(",".join(list(item) + [self.school]))
            outfile.write("\n")

        outfile.close()


    def run(self):
        while True:
            self.users = []
            self.statuses = []
            index = self.queue.get()
            self.extract(index)
            self.export()

            self.queue.task_done()

def main():
    try:
        input_file = open(argv[1])
    except IOError:
        print "Can't open " + argv[1]

    if not path.exists(output_dirname):
        print "Creating output directory: %s" % output_dirname
        makedirs(output_dirname)

    num_threads = 10
    for i in xrange(num_threads):
        t = Scraper(queue)
        t.setDaemon(True)
        t.start()

    for line in input_file:
        queue.put(line.strip())

    queue.join()



if __name__ == '__main__':
    main()
