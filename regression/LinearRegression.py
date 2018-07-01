import json

import numpy as np
from sklearn import linear_model
from sklearn.externals import joblib


class OrdinaryLeastSquare:
    @staticmethod
    def train(data: np.ndarray, model_dir: str, result_dir: str,
              task_id: str, running_handler, finished_handler, error_handler):
        try:
            running_handler(task_id)
            reg = linear_model.LinearRegression()
            reg.fit(data[:, :-1], data[:, -1])
            joblib.dump(reg, f'{model_dir}/LROLS.m')
            with open(f'{result_dir}/LROLS_train.json', 'w') as fp:
                fp.write(json.dumps(
                    {'A': reg.coef_.tolist(), 'b': reg.intercept_.tolist()}))
            finished_handler(task_id)
        except Exception:
            error_handler(task_id)

    @staticmethod
    def predict(data: np.ndarray, model_dir: str, result_dir: str,
                task_id: str, running_handler, finished_handler, error_handler):
        try:
            running_handler(task_id)
            reg = joblib.load(f'{model_dir}/LROLS.m')
            with open(f'{result_dir}/LROLS_predict.json', 'w') as fp:
                fp.write(json.dumps(reg.predict(data).tolist()))
            finished_handler(task_id)
        except Exception:
            error_handler(task_id)
