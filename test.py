import json

import numpy as np
from sklearn import linear_model
from sklearn.externals import joblib

from util import HdfsFile

if __name__ == '__main__':
    data = np.loadtxt(HdfsFile('testworkdir/train.csv'),
                      delimiter=',', skiprows=1)
    reg = linear_model.LinearRegression()
    reg.fit(data[:, :-1], data[:, -1])
    output = HdfsFile('testworkdir/LROLS.m')
    joblib.dump(reg, output)
