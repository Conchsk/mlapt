from util import HdfsFile, DataAdapter

from sklearn import linear_model
from sklearn.externals import joblib

if __name__ == '__main__':
    data = DataAdapter.csv2array('testworkdir/predict.csv')
    # reg = linear_model.LinearRegression()
    # reg.fit(data[:, :-1], data[:, -1])
    file = HdfsFile('LROLS.m')
    #file = open('testworkdir/LROLS.m', 'rb')
    reg = joblib.load(file)
    print(reg.predict(data))

