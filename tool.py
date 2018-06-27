import numpy as np


def csv2array(path: str) -> np.ndarray:
    return np.loadtxt(path, delimiter=',', skiprows=1)
