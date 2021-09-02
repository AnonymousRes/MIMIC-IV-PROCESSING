MIMIC-IV Dataset Processing
=========================
This repository was modified from https://github.com/YerevaNN/mimic3-benchmarks.

## Structure
The content of this repository can be divided into four big parts:
* Tools for creating the MIMIC-IV datasets.

The `mimic4processing/scripts` directory contains scripts for creating the benchmark datasets.
The reading tools are in `mimic4processing/readers.py`.



## Requirements

We do not provide the MIMIC-IV data itself. You must acquire the data yourself from https://mimic.physionet.org/. Specifically, download the CSVs. Otherwise, generally we make liberal use of the following packages:

- numpy
- pandas
- sklearn

## Processing MIMIC-IV Dataset
### Building Train / Test Dataset
Here are the required steps to build the benchmark. It assumes that you already have MIMIC-IV dataset (lots of CSV files, path is `~/MIMIC-IV/` ) on the disk.
1. Clone the repo. 

       cd ~/
       git clone https://github.com/AnonymousRes/MIMIC-IV-PROCESSING/
       mkdir ~/mimic4processed/
       cd ~/MIMIC-IV-PROCESSING/

2. The following command takes MIMIC-IV CSVs, generates one directory per `SUBJECT_ID` and writes ICU stay information to `~/mimic4processed/{SUBJECT_ID}/stays.csv`, diagnoses to `~/mimic4processed/{SUBJECT_ID}/diagnoses.csv`, and events to `~/mimic4processed/{SUBJECT_ID}/events.csv`. This step might take around an hour.

       python -m mimic4processing.scripts.extract_subjects ~/MIMIC-IV/ ~/mimic4processed/

3. The following command attempts to fix some issues (ICU stay ID is missing) and removes the events that have missing information (more information can be found in [`mimic4processing/scripts/more_on_validating_events.md`](mimic4processing/scripts/more_on_validating_events.md)).

       python -m mimic4processing.scripts.validate_events ~/mimic4processed/

4. The next command breaks up per-subject data into separate episodes (pertaining to ICU stays). Time series of events are stored in ```{SUBJECT_ID}/episode{#}_timeseries.csv``` (where # counts distinct episodes) while episode-level information (patient age, gender, ethnicity, height, weight) and outcomes (mortality, length of stay, diagnoses) are stores in ```{SUBJECT_ID}/episode{#}.csv```. This script requires two files, one that maps event ITEMIDs to clinical variables and another that defines valid ranges for clinical variables (for detecting outliers, etc.). **Outlier detection is disabled in the current version**.

       python -m mimic4processing.scripts.extract_episodes_from_subjects ~/mimic4processed/

5. The next command splits the whole dataset into training and testing sets. Note that the train/test split is the same of all tasks.

       python -m mimic4processing.scripts.split_train_and_test ~/mimic4processed/
	
6. The following commands will generate task-specific datasets, which can later be used in models. These commands are independent, if you are going to work only on one benchmark task, you can run only the corresponding command.

       python -m mimic4processing.scripts.create_in_hospital_mortality ~/mimic4processed/ ~/mimic4processed/in-hospital-mortality/
       python -m mimic4processing.scripts.create_decompensation ~/mimic4processed/ ~/mimic4processed/decompensation/
       python -m mimic4processing.scripts.create_length_of_stay ~/mimic4processed/ ~/mimic4processed/length-of-stay/
       python -m mimic4processing.scripts.create_phenotyping ~/mimic4processed/ ~/mimic4processed/phenotyping/
       python -m mimic4processing.scripts.create_multitask ~/mimic4processed/ ~/mimic4processed/multitask/


After the above commands are done, there will be a directory `~/mimic4processed/{task}` for each created benchmark task.
These directories have two sub-directories: `train` and `test`.
Each of them contains bunch of ICU stays and one file with name `listfile.csv`, which lists all samples in that particular set.
Each row of `listfile.csv` has the following form: `icu_stay, period_length, label(s)`.
A row specifies a sample for which the input is the collection of ICU event of `icu_stay` that occurred in the first `period_length` hours of the stay and the target is/are `label(s)`.
In in-hospital mortality prediction task `period_length` is always 48 hours, so it is not listed in corresponding listfiles.
### Train / Validation Dataset split

Use the following command to extract validation set from the training set. This step is required for running the baseline models. Likewise the train/test split, the train/validation split is the same for all tasks.

       python -m mimic4processing.scripts.split_train_and_val {dataset-directory}
       
`{dataset-directory}` can be either `~/mimic4processed/in-hospital-mortality/`, `~/mimic4processed/decompensation/`, `~/mimic4processed/length-of-stay/`, `~/mimic4processed/phenotyping/` or `~/mimic4processed/multitask/`.
