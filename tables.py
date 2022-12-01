import pandas as pd
import dbm
import sqlalchemy
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv 

load_dotenv('/Users/corinne/Documents/GitHub/-PssP/.vscode/.env')

MYSQL_HOSTNAME = os.getenv ('MYSQL_HOSTNAME')
MYSQL_USER = os.getenv ('MYSQL_USER')
MYSQL_PASSWORD = os.getenv ('MYSQL_PASSWORD')
MYSQL_DATABASE = os.getenv ('MYSQL_DATABASE')

connection_string = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOSTNAME}:3306/{MYSQL_DATABASE}'
db = create_engine(connection_string)
print (db.table_names())

table_patients = """
create table if not exists patients (
    id int auto_increment,
    mrn varchar(255) default null unique,
    first_name varchar(255) default null,
    last_name varchar(255) default null,
    zip_code varchar(255) default null,
    dob varchar(255) default null,
    gender varchar(255) default null,
    pronouns varchar(255) default null,
    insurance varchar(255) default null,
    contact_number varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""

table_medications = """
create table if not exists medications (
    id int auto_increment,
    med_ndc varchar(255) default null unique,
    med_human_name varchar(255) default null,
    PRIMARY KEY (id)
    );
"""

table_patient_medications = """
create table if not exists patient_medications (
    id int auto_increment,
    med_ndc varchar(255) default null,
    med_human_name varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (med_ndc) REFERENCES medications(med_ndc) ON DELETE CASCADE
); 
"""

 #FOREIGN KEY (med_ndc) REFERENCES medications(med_ndc) ON DELETE CASCADE

table_treatment_process = """
create table if not exists treatment_process (
    id int auto_increment,
    mrn varchar(255) default null unique,
    treatment_name varchar(255) default null,
    treatment_type varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""

table_conditions = """
create table if not exists conditions (
    id int auto_increment,
    icd10_code varchar(255) default null unique,
    icd10_description varchar(255) default null,
    PRIMARY KEY (id)
); 
"""

table_patient_conditions = """
create table if not exists patient_conditions(
    id int auto_increment,
    mrn varchar(255) default null,
    icd10_code varchar(255) default null,
    icd10_descriptions varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (icd10_code) REFERENCES conditions(icd10_code) ON DELETE CASCADE
);
"""

table_social_determinants = """
create table if not exists social_determinants(
    id int auto_increment,
    LOINC_NUM varchar(255) default null,
    COMPONENT varchar(255) default null,
    PRIMARY KEY (id)
);
"""

db.execute(table_patients)
db.execute(table_medications)
db.execute(table_treatment_process)
db.execute(table_patient_conditions)
db.execute(table_social_determinants)
db.execute(table_patient_medications)
db.execute(table_conditions)



