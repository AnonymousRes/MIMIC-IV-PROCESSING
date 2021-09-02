from __future__ import absolute_import
from __future__ import print_function

import os
import shutil
import argparse
import pandas as pd
import numpy as np
from sklearn import model_selection

def move_to_partition(args, patients, partition):
    if not os.path.exists(os.path.join(args.subjects_root_path, partition)):
        os.mkdir(os.path.join(args.subjects_root_path, partition))
    for patient in patients:
        src = os.path.join(args.subjects_root_path, patient)
        dest = os.path.join(args.subjects_root_path, partition, patient)
        shutil.move(src, dest)


def main():
    parser = argparse.ArgumentParser(description='Split data into train and test sets.')
    parser.add_argument('--subjects_root_path', type=str, help='Directory containing subject sub-directories.')
    parser.set_defaults(subjects_root_path='/home/leew/mimic4processed/')
    args, _ = parser.parse_known_args()

    # testset.csv
    # def is_subject_folder(x):
    #     return str.isdigit(x)
    # subdirectories = os.listdir(args.subjects_root_path)
    # subjects = list(filter(is_subject_folder, subdirectories))
    # subjects_train, subjects_test = model_selection.train_test_split(subjects, test_size=0.15, random_state=2022)
    # print('Total:', len(subjects), 'Train:', len(subjects_train), 'Test:', len(subjects_test))
    # atr = np.vstack((np.array(subjects_train, dtype=str), np.zeros((len(subjects_train), ), dtype=int))).transpose()
    # ate = np.vstack((np.array(subjects_test, dtype=str), np.ones((len(subjects_test), ), dtype=int))).transpose()
    # atr_te = np.concatenate((atr, ate), axis=0)
    # # print(atr)
    # # print(ate)
    # # print(atr_te)
    # pd.DataFrame(data=atr_te, index=None, columns=None).\
    #     to_csv(os.path.join(os.path.dirname(__file__), '../resources/testset.csv'), index=None, header=None, columns=None)

    # valset.csv
    # testsetdf = pd.read_csv(os.path.join(os.path.dirname(__file__), '../resources/testset.csv'), header=None)
    # testdv = testsetdf.values
    # print(testdv.shape)
    # atr = np.array([list(x) for x in testdv if x[1] == 0])
    # print(atr)
    # dtr, dva = model_selection.train_test_split(atr, test_size=0.15, random_state=2022)
    # dva[:, 1] = 1
    # dtr[:, 1] = 0
    # dt_v = np.concatenate((dtr, dva), axis=0)
    # print(len(dt_v), len(dtr), len(dva))
    # print(dt_v)
    # pd.DataFrame(data=dt_v, index=None, columns=None).\
    #     to_csv(os.path.join(os.path.dirname(__file__), '../resources/valset.csv'), index=None, header=None, columns=None)

    test_set = set()
    with open(os.path.join(os.path.dirname(__file__), '../resources/testset.csv'), "r") as test_set_file:
        for line in test_set_file:
            x, y = line.split(',')
            if int(y) == 1:
                test_set.add(x)

    folders = os.listdir(args.subjects_root_path)
    folders = list((filter(str.isdigit, folders)))
    train_patients = [x for x in folders if x not in test_set]
    test_patients = [x for x in folders if x in test_set]

    assert len(set(train_patients) & set(test_patients)) == 0

    move_to_partition(args, train_patients, "train")
    move_to_partition(args, test_patients, "test")


if __name__ == '__main__':
    main()
