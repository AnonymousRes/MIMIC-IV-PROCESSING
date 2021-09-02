from __future__ import absolute_import
from __future__ import print_function

import csv
import numpy as np
import os
import pandas as pd
from tqdm import tqdm

from mimic4processing.util import dataframe_from_csv


def read_patients_table(mimic4_path):
    pats = dataframe_from_csv(os.path.join(mimic4_path, 'core/patients.csv'))
    pats = pats[['subject_id', 'gender', 'anchor_age']]
    return pats


def read_admissions_table(mimic4_path):
    admits = dataframe_from_csv(os.path.join(mimic4_path, 'core/admissions.csv'))
    admits = admits[['subject_id', 'hadm_id', 'admittime', 'dischtime', 'deathtime', 'ethnicity']]
    admits.admittime = pd.to_datetime(admits.admittime)
    admits.dischtime = pd.to_datetime(admits.dischtime)
    admits.deathtime = pd.to_datetime(admits.deathtime)
    return admits


def read_icustays_table(mimic4_path):
    stays = dataframe_from_csv(os.path.join(mimic4_path, 'icu/icustays.csv'))
    stays.intime = pd.to_datetime(stays.intime)
    stays.outtime = pd.to_datetime(stays.outtime)
    return stays


def read_icd_diagnoses_table(mimic4_path):
    # icd10to9 dict build
    icd10to9 = {}
    icdinf = pd.read_csv(os.path.join(os.path.dirname(__file__), 'resources/icd10toicd9gem.csv'), usecols=['icd10cm', 'icd9cm'], index_col=None, low_memory=False)
    for icd_ in icdinf.values:
        icd10to9[icd_[0]]=icd_[1]
        # print(icd_[0], icd_[1])

    codes = dataframe_from_csv(os.path.join(mimic4_path, 'hosp/d_icd_diagnoses.csv'))
    codes = codes[['icd_code', 'icd_version', 'long_title']]

    diagnoses = dataframe_from_csv(os.path.join(mimic4_path, 'hosp/diagnoses_icd.csv'))
    diagnoses = diagnoses.merge(codes, how='inner', left_on=['icd_code', 'icd_version'], right_on=['icd_code', 'icd_version'])

    print(diagnoses.columns)
    diav = diagnoses.values
    rnum = diav.shape[0]
    unknow_set = set()
    unknow_list = []
    new_div = []
    for ri in tqdm(range(rnum), desc='icd10 -> icd9'):
        if str(diav[ri][-2]) == '10':
            # print('\nbefore:', diav[ri])
            try:
                newicd9 = icd10to9[diav[ri][-3]]
                diav[ri][-3] = newicd9
                diav[ri][-2] = 9
                new_div.append(diav[ri])
            except:
                # print(diav[ri][-3])
                unknow_set.add(diav[ri][-3])
                unknow_list.append(diav[ri][-3])
                pass
            # print('after:', diav[ri])
        else:
            new_div.append(diav[ri])
    print(len(unknow_set), 'missing types;', len(unknow_list), 'mising cases;', len(new_div), 'new all cases;', len(diav), 'old all cases.')
    # exit(0)
    diagnoses = pd.DataFrame(data=np.array(new_div), index=None, columns=['subject_id', 'hadm_id', 'seq_num', 'icd_code', 'icd_version', 'long_title'])
    diagnoses = diagnoses.sort_values(by=['subject_id', 'hadm_id', 'seq_num'])
    diagnoses[['subject_id', 'hadm_id', 'seq_num']] = diagnoses[['subject_id', 'hadm_id', 'seq_num']].astype(int)

    return diagnoses


def read_events_table_by_row(mimic4_path, table):
    nb_rows = {'icu/chartevents': 329499788, 'hosp/labevents': 122103667, 'icu/outputevents': 4457381}
    reader = csv.DictReader(open(os.path.join(mimic4_path, table.lower() + '.csv'), 'r'))
    for i, row in enumerate(reader):
        if 'stay_id' not in row:
            row['stay_id'] = ''
        yield row, i, nb_rows[table.lower()]


def count_icd_codes(diagnoses, output_path=None):
    codes = diagnoses[['icd_code', 'long_title']].drop_duplicates().set_index('icd_code')
    codes['count'] = diagnoses.groupby('icd_code')['stay_id'].count()
    codes['count'] = codes['count'].fillna(0)
    codes['count'] = codes['count'].astype(int)
    codes = codes[codes['count'] > 0]
    if output_path:
        codes.to_csv(output_path, index_label='icd_code')
    return codes.sort_values('count', ascending=False).reset_index()


def remove_icustays_with_transfers(stays):
    stays = stays[(stays.first_careunit == stays.last_careunit)]
    return stays[['subject_id', 'hadm_id', 'stay_id', 'last_careunit', 'intime', 'outtime', 'los']]


def merge_on_subject(table1, table2):
    return table1.merge(table2, how='inner', left_on=['subject_id'], right_on=['subject_id'])


def merge_on_subject_admission(table1, table2):
    return table1.merge(table2, how='inner', left_on=['subject_id', 'hadm_id'], right_on=['subject_id', 'hadm_id'])


def add_age_to_icustays(stays):
    stays.loc[stays.anchor_age < 0, 'anchor_age'] = 90
    return stays


def add_inhospital_mortality_to_icustays(stays):
    mortality = (stays.deathtime.notnull() & ((stays.admittime <= stays.deathtime) & (stays.dischtime >= stays.deathtime)))
    stays['mortality'] = mortality.astype(int)
    stays['mortality_inhospital'] = stays['mortality']
    return stays


def add_inunit_mortality_to_icustays(stays):
    mortality = (stays.deathtime.notnull() & ((stays.intime <= stays.deathtime) & (stays.outtime >= stays.deathtime)))
    stays['mortality_inunit'] = mortality.astype(int)
    return stays


def filter_admissions_on_nb_icustays(stays, min_nb_stays=1, max_nb_stays=1):
    to_keep = stays.groupby('hadm_id').count()[['stay_id']].reset_index()
    to_keep = to_keep[(to_keep.stay_id >= min_nb_stays) & (to_keep.stay_id <= max_nb_stays)][['hadm_id']]
    stays = stays.merge(to_keep, how='inner', left_on='hadm_id', right_on='hadm_id')
    return stays


def filter_icustays_on_age(stays, min_age=18, max_age=np.inf):
    stays = stays[(stays.anchor_age >= min_age) & (stays.anchor_age <= max_age)]
    return stays


def filter_diagnoses_on_stays(diagnoses, stays):
    return diagnoses.merge(stays[['subject_id', 'hadm_id', 'stay_id']].drop_duplicates(), how='inner',
                           left_on=['subject_id', 'hadm_id'], right_on=['subject_id', 'hadm_id'])


def break_up_stays_by_subject(stays, output_path, subjects=None):
    subjects = stays.subject_id.unique() if subjects is None else subjects
    nb_subjects = subjects.shape[0]
    for subject_id in tqdm(subjects, total=nb_subjects, desc='breaking up stays by subjects'):
        dn = os.path.join(output_path, str(subject_id))
        try:
            os.makedirs(dn)
        except:
            pass

        subjectstays = stays[stays.subject_id == subject_id]
        subjectstays = subjectstays.sort_values(by='intime')
        subjectstays.to_csv(os.path.join(dn, 'stays.csv'), index=False)


def break_up_diagnoses_by_subject(diagnoses, output_path, subjects=None):
    subjects = diagnoses.subject_id.unique() if subjects is None else subjects
    nb_subjects = subjects.shape[0]
    for subject_id in tqdm(subjects, total=nb_subjects, desc='breaking up diagnoses by subjects'):
        dn = os.path.join(output_path, str(subject_id))
        try:
            os.makedirs(dn)
        except:
            pass

        diagnoses[diagnoses.subject_id == subject_id].sort_values(by=['stay_id', 'seq_num'])\
                                                     .to_csv(os.path.join(dn, 'diagnoses.csv'), index=False)


def read_events_table_and_break_up_by_subject(mimic4_path, table, output_path,
                                              items_to_keep=None, subjects_to_keep=None):
    obs_header = ['subject_id', 'hadm_id', 'stay_id', 'charttime', 'itemid', 'value', 'valueuom']
    if items_to_keep is not None:
        items_to_keep = set([str(s) for s in items_to_keep])
    if subjects_to_keep is not None:
        subjects_to_keep = set([str(s) for s in subjects_to_keep])

    class datastats(object):
        def __init__(self):
            self.curr_subject_id = ''
            self.curr_obs = []

    data_stats = datastats()

    def write_current_observations():
        dn = os.path.join(output_path, str(data_stats.curr_subject_id))
        try:
            os.makedirs(dn)
            # os.system("mkdir "+dn)
        except:
            pass
        fn = os.path.join(dn, 'events.csv')
        if not os.path.exists(fn) or not os.path.isfile(fn):
            f = open(fn, 'w')
            f.write(','.join(obs_header) + '\n')
            f.close()
        w = csv.DictWriter(open(fn, 'a'), fieldnames=obs_header, quoting=csv.QUOTE_MINIMAL)
        w.writerows(data_stats.curr_obs)
        data_stats.curr_obs = []

    nb_rows_dict = {'icu/chartevents': 329499788, 'hosp/labevents': 122103667, 'icu/outputevents': 4457381}
    nb_rows = nb_rows_dict[table.lower()]

    for row, row_no, _ in tqdm(read_events_table_by_row(mimic4_path, table), total=nb_rows,
                                                        desc='processing {} table'.format(table)):

        if (subjects_to_keep is not None) and (row['subject_id'] not in subjects_to_keep):
            continue
        if (items_to_keep is not None) and (row['itemid'] not in items_to_keep):
            continue

        row_out = {'subject_id': row['subject_id'],
                   'hadm_id': row['hadm_id'],
                   'stay_id': '' if 'stay_id' not in row else row['stay_id'],
                   'charttime': row['charttime'],
                   'itemid': row['itemid'],
                   'value': row['value'],
                   'valueuom': row['valueuom']}
        if data_stats.curr_subject_id != '' and data_stats.curr_subject_id != row['subject_id']:
            write_current_observations()
        data_stats.curr_obs.append(row_out)
        data_stats.curr_subject_id = row['subject_id']

    if data_stats.curr_subject_id != '':
        write_current_observations()
