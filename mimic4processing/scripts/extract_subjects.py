from __future__ import absolute_import
from __future__ import print_function

import argparse
import csv

import yaml
import os
import sys
sys.path.append("..")
from mimic4processing.mimic4csv import *
from mimic4processing.preprocessing import add_hcup_ccs_2015_groups, make_phenotype_label_matrix
from mimic4processing.util import dataframe_from_csv

parser = argparse.ArgumentParser(description='extract per-subject data from mimic-iv (version=1.0) csv files.')
parser.add_argument('mimic4_path', default='/home/leew/MIMIC-IV/', type=str, help='directory containing mimic-iv csv files.')
parser.add_argument('output_path', default='/home/leew/mimic4processed/',  type=str, help='directory where per-subject data should be written.')
parser.add_argument('--event_tables', '-e', type=str, nargs='+', help='tables from which to read events.',
                    default=['icu/chartevents', 'hosp/labevents', 'icu/outputevents'])
parser.add_argument('--phenotype_definitions', '-p', type=str,
                    default=os.path.join(os.path.dirname(__file__), '../resources/hcup_ccs_2015_definitions.yaml'),
                    help='yaml file with phenotype definitions.')
parser.add_argument('--itemids_file', '-i', type=str, help='csv containing list of itemids to keep.')
parser.add_argument('--verbose', '-v', default=True, dest='verbose', action='store_true', help='verbosity in output')
parser.add_argument('--quiet', '-q', dest='verbose', action='store_false', help='suspend printing of details')
# parser.add_argument('--test', default=True, action='store_true', help='test mode: process only 1000 subjects, 1000000 events.')
args, _ = parser.parse_known_args()



try:
    os.makedirs(args.output_path)
except:
    pass

patients = read_patients_table(args.mimic4_path)
admits = read_admissions_table(args.mimic4_path)
stays = read_icustays_table(args.mimic4_path)
if args.verbose:
    print('start:\n\tstay_ids: {}\n\thadm_ids: {}\n\tsubject_ids: {}'.format(stays.stay_id.unique().shape[0],
          stays.hadm_id.unique().shape[0], stays.subject_id.unique().shape[0]))

stays = remove_icustays_with_transfers(stays)
if args.verbose:
    print('remove icu transfers:\n\tstay_ids: {}\n\thadm_ids: {}\n\tsubject_ids: {}'.format(stays.stay_id.unique().shape[0],
          stays.hadm_id.unique().shape[0], stays.subject_id.unique().shape[0]))

stays = merge_on_subject_admission(stays, admits)
stays = merge_on_subject(stays, patients)
stays = filter_admissions_on_nb_icustays(stays)
if args.verbose:
    print('remove multiple stays per admit:\n\tstay_ids: {}\n\thadm_ids: {}\n\tsubject_ids: {}'.format(stays.stay_id.unique().shape[0],
          stays.hadm_id.unique().shape[0], stays.subject_id.unique().shape[0]))

stays = add_age_to_icustays(stays)
stays = add_inunit_mortality_to_icustays(stays)
stays = add_inhospital_mortality_to_icustays(stays)
stays = filter_icustays_on_age(stays)
if args.verbose:
    print('remove patients age < 18:\n\tstay_ids: {}\n\thadm_ids: {}\n\tsubject_ids: {}'.format(stays.stay_id.unique().shape[0],
          stays.hadm_id.unique().shape[0], stays.subject_id.unique().shape[0]))

stays.to_csv(os.path.join(args.output_path, 'all_stays.csv'), index=False)
diagnoses = read_icd_diagnoses_table(args.mimic4_path)
diagnoses = filter_diagnoses_on_stays(diagnoses, stays)
diagnoses.to_csv(os.path.join(args.output_path, 'all_diagnoses.csv'), index=False)
count_icd_codes(diagnoses, output_path=os.path.join(args.output_path, 'diagnosis_counts.csv'))

phenotypes = add_hcup_ccs_2015_groups(diagnoses, yaml.load(open(args.phenotype_definitions, 'r')))
make_phenotype_label_matrix(phenotypes, stays).to_csv(os.path.join(args.output_path, 'phenotype_labels.csv'),
                                                      index=False, quoting=csv.QUOTE_NONNUMERIC)

# if args.test:
#     pat_idx = np.random.choice(patients.shape[0], size=1000)
#     patients = patients.iloc[pat_idx]
#     stays = stays.merge(patients[['subject_id']], left_on='subject_id', right_on='subject_id')
#     args.event_tables = [args.event_tables[0]]
#     print('using only', stays.shape[0], 'stays and only', args.event_tables[0], 'table')

subjects = stays.subject_id.unique()
break_up_stays_by_subject(stays, args.output_path, subjects=subjects)
break_up_diagnoses_by_subject(phenotypes, args.output_path, subjects=subjects)
items_to_keep = set(
    [int(itemid) for itemid in dataframe_from_csv(args.itemids_file)['itemid'].unique()]) if args.itemids_file else None
for table in args.event_tables:
    read_events_table_and_break_up_by_subject(args.mimic4_path, table, args.output_path, items_to_keep=items_to_keep,
                                              subjects_to_keep=subjects)
