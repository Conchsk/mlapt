# coding=utf-8
from flask import Flask, render_template, request
from flask_cors import CORS

import util
from regression import LinearRegression

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
    task_type = request.values.get('task_type')
    data_path = request.values.get('data_path')
    model_dir = request.values.get('model_dir')
    result_dir = request.values.get('result_dir')

    if task_type == str(util.Constants.TYPE_TRIAN):
        if alg_name == 'LROLS':
            task_id = process_pool.apply(LinearRegression.OrdinaryLeastSquare.train, data_path, model_dir, result_dir)
    elif task_type == str(util.Constants.TYPE_PREDICT):
        if alg_name == 'LROLS':
            task_id = process_pool.apply(LinearRegression.OrdinaryLeastSquare.predict, data_path, model_dir, result_dir)
    else:
        task_id = '-1'

    return 'task_id is ' + task_id


# task status query
@app.route('/status', methods=['GET'])
def status_handler():
    task_id = request.values.get('task_id')
    status = redis_pool.get(task_id)
    if status is not None:
        if status == str(util.Constants.TASK_RUNNING):
            return 'task running'
        elif status == str(util.Constants.TASK_FINISHED):
            return 'task finished'
        elif status == str(util.Constants.TASK_ERROR):
            return 'task error'
    return 'task not found'


if __name__ == '__main__':
    redis_pool = util.RedisPool()
    process_pool = util.ProcessPool()
    app.run(host='localhost', port=5000)
