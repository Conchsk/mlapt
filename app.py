# coding=utf-8
from flask import Flask, render_template, request
from flask_cors import CORS

import myhdfs as mh
import util
from regression import linear

app = Flask(__name__)
CORS(app)


@app.route('/test', methods=['GET'])
def test():
    return 'hello world'


@app.route('/<page_name>.html', methods=['GET'])
def page_handler(page_name):
    return render_template(page_name + '.html')


# submit task
@app.route('/submit', methods=['GET', 'POST'])
def train_handler():
    alg_name = request.values.get('alg_name')
    alg_args = request.values.get('alg_args')
    task_type = request.values.get('task_type')
    data_path = request.values.get('data_path')
    model_path = request.values.get('model_path')
    result_path = request.values.get('result_path')

    if task_type == 'train':
        if alg_name == 'LROLS':
            task_id = process_pool.apply(linear.OrdinaryLeastSquare.train, alg_args,
                                         data_path, model_path, result_path)
    elif task_type == 'predict':
        if alg_name == 'LROLS':
            task_id = process_pool.apply(linear.OrdinaryLeastSquare.predict, alg_args,
                                         data_path, model_path, result_path)
    else:
        task_id = '-1'

    return 'task_id is ' + task_id


# task status query
@app.route('/status', methods=['GET'])
def status_handler():
    task_id = request.values.get('task_id')
    status = redis_pool.get(task_id)
    if status is not None:
        return f'task {status}'
    return 'task not found'


if __name__ == '__main__':
    redis_pool = util.RedisPool()
    process_pool = util.ProcessPool()
    app.run(host='localhost', port=5000)
