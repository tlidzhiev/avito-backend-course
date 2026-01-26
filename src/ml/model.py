import pickle
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression


def train_model():
    np.random.seed(42)
    X = np.random.rand(1000, 4)
    y = (X[:, 0] < 0.3) & (X[:, 1] < 0.2)
    y = y.astype(int)

    model = LogisticRegression()
    model.fit(X, y)
    return model


def save_model(model, path='model.pkl'):
    path_obj = Path(path)
    with open(path_obj, 'wb') as f:
        pickle.dump(model, f)


def load_model(path='model.pkl'):
    path_obj = Path(path)
    if not path_obj.exists():
        return None
    with open(path_obj, 'rb') as f:
        return pickle.load(f)
