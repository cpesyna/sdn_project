#!/usr/bin/python

from __future__ import division
from os import path, listdir, makedirs, popen
import scipy


class School(object):
    """docstring for School"""
    def __init__(self, base_file):
        super(School, self).__init__()
        f = open(base_file)
        self.users = set()
        for line in f:
            user, user_type, school_name = line.strip().split(",")
            if user_type in ["Pre-Medical", "Unknown"]:
                self.users.add(user)
        f.close()
        self.school_name = school_name.replace("&amp;", "and")


def jaccard_index(s1, s2):
    isect = len(s1.users.intersection(s2.users))
    union = len(s1.users.union(s2.users))

    if union:
        return isect/union
    else:
        return 0

def build_simm_matrix(elements, simm_func):
    """docstring for build_jacc_matrix"""
    num_elems = len(elements)
    outmat = scipy.zeros((num_elems, num_elems))

    for i, item1 in enumerate(elements):
        for j, item2 in enumerate(elements):
            outmat[i, j] = simm_func(item1, item2)

    return outmat

def pcl_out(row_label, column_names, row_names, matrix, fname, delimiter = '\t', col_weight = 1, row_weight = 1):
    header1 = '\t'.join([row_label, 'GWEIGHT'] + column_names)
    nr, nc = matrix.shape
    header2 = '\t'.join(['EWEIGHT', ''] +
                        [str(col_weight) for item in xrange(nc)])

    outfile = open(fname, 'w')
    outfile.write(header1)
    outfile.write('\n')
    outfile.write(header2)

    format_strings = [delimiter.join([name, str(row_weight)] +
                        ['%f']*nc) for name in row_names]
    pairs = zip(format_strings, matrix)
    for fstring, row in pairs:
        outfile.write('\n')
        outfile.write(fstring % tuple(row))

    outfile.close()

def linearize(infile, outfile):
    infile = open(infile)
    outfile = open(outfile, "w")
    outfile.write(",".join(["School1", "School2", "Jaccard_Index\n"]))

    schools = infile.readline().strip().split("\t")[1:]

    for line in infile:
        line = line.strip().split("\t")
        school = line[0]
        scores = line[1:]
        for pair in zip(schools, scores):
            outfile.write(",".join([school] + list(pair)))
            outfile.write("\n")
    infile.close()
    outfile.close()

def main():
    """Performs aggregation of scraped sdn data"""
    datadir = "./data/output/"
    project_files = [datadir + f for f in listdir(datadir)]

    output_dirname = "./data/cluster/"
    if not path.exists(output_dirname):
        print "Creating output directory: %s" % output_dirname
        makedirs(output_dirname)

    schools = [School(f) for f in project_files]
    jacc_mat = build_simm_matrix(schools, jaccard_index)
    snames = [s.school_name for s in schools]
    outfilename = output_dirname + "SxS_jaccard.pcl"
    pcl_out("School_name", snames, snames, jacc_mat, outfilename)

    status = popen('cluster -f ' + outfilename + ' -e 5 -g 5 -m a')
    status.close()

    status = popen('cdt2csv.sh ' + outfilename.replace("pcl", "cdt"))
    status.close()

    linearize(outfilename.replace("pcl", "csv"), output_dirname + "linearized.csv")

if __name__ == '__main__':
    main()
