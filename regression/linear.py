import json
import pickle

import numpy as np
from sklearn import linear_model
from sklearn.externals import joblib

import myhdfs as mh


class OrdinaryLeastSquare:
    @staticmethod
    def train(args: set, data_path: str, model_path: str, result_path: str):
        data = np.loadtxt(mh.HdfsFile(data_path), delimiter=',', skiprows=1)
        reg = linear_model.LinearRegression()
        reg.fit(data[:, :-1], data[:, -1])
        with mh.HdfsFile(model_path, 'wb') as fp:
            joblib.dump(reg, fp)
        with mh.HdfsFile(result_path, 'w') as fp:
            json.dump({'A': reg.coef_.tolist(), 'b': reg.intercept_.tolist()}, fp)

    @staticmethod
    def predict(args: set, data_path: str, model_path: str, result_path: str):
        data = np.loadtxt(mh.HdfsFile(data_path), delimiter=',', skiprows=1)
        reg = joblib.load(mh.HdfsFile(model_path, 'rb'))
        with mh.HdfsFile(result_path, 'w') as fp:
            json.dump(reg.predict(data).tolist(), fp)
