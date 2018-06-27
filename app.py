# coding=utf-8
from flask import Flask, render_template, request
from flask_cors import CORS

import tool
from regression import LinearRegression

app = Flask(__name__)
CORS(app)


@app.route('/test')
def test2():
    return 'hello world'


@app.route('/train', methods=['GET', 'POST'])
def train_handler():
    alg_name = request.values.get('alg_name')
    data_path = request.values.get('data_path')
    model_dir = request.values.get('model_dir')
    result_dir = request.values.get('result_dir')

    if alg_name == 'LROLS':
        LROLS = LinearRegression.OrdinaryLeastSquare()
        LROLS.train(tool.csv2array(data_path), model_dir, result_dir)

    return 'success'


@app.route('/predict', methods=['GET', 'POST'])
def predict_handler():
    alg_name = request.values.get('alg_name')
    data_path = request.values.get('data_path')
    model_dir = request.values.get('model_dir')
    result_dir = request.values.get('result_dir')

    if alg_name == 'LROLS':
        LROLS = LinearRegression.OrdinaryLeastSquare()
        LROLS.predict(tool.csv2array(data_path), model_dir, result_dir)

    return 'success'


@app.route('/status', methods=['GET', 'POST'])
def status_handler():
    pass


@app.route('/<page_name>.html', methods=['GET'])
def page_handler(page_name):
    return render_template(f'{page_name}.html')


if __name__ == '__main__':
    app.run(host='localhost', port=5000, threaded=True)
