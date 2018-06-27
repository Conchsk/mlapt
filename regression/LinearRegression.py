import json

import numpy as np
from sklearn import linear_model
from sklearn.externals import joblib


class OrdinaryLeastSquare:
    def train(self, data: np.ndarray, model_dir: str, result_dir: str) -> None:
        reg = linear_model.LinearRegression()
        reg.fit(data[:, :-1], data[:, -1])
        joblib.dump(reg, f'{model_dir}/LROLS.m')
        with open(f'{result_dir}/LROLS_train.json', 'w') as fp:
            fp.write(json.dumps({'A': reg.coef_.tolist(), 'b': reg.intercept_.tolist()}))

    def predict(self, data: np.ndarray, model_dir: str, result_dir: str) -> None:
        reg = joblib.load(f'{model_dir}/LROLS.m')
        with open(f'{result_dir}/LROLS_predict.json', 'w') as fp:
            fp.write(json.dumps(reg.predict(data).tolist()))
