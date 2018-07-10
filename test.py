import json

import numpy as np
from sklearn import linear_model
from sklearn.externals import joblib

import myhdfs as mh

if __name__ == '__main__':
    data = np.loadtxt(mh.HdfsFile(path='testworkdir/predict.csv'), delimiter=',', skiprows=1)
    reg = joblib.load(mh.HdfsFile(path='testworkdir/LROLS.m', mode='rb'))
    with mh.HdfsFile(path='testworkdir/LROLS_predict.json', mode='w') as fp:
        json.dump(reg.predict(data).tolist(), fp)
