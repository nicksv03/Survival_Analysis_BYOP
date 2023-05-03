# synthetic data generator

# features
# source: https://assets.researchsquare.com/files/rs-1015864/v1/c0292884-32a9-4470-8e4c-17f31a67ed9f.pdf?c=1637335886


#%% dependencies

import os
path = r'/Users/nikhil/Library/CloudStorage/GoogleDrive-dcbs17nikhilverma@gmail.com/My Drive/Github/Survival_Analysis_BYOP/dataset'
os.chdir(path.replace('\\', '/'))

# generates records for patients within a random range
min_patients, max_patients = 500, 1000


import pandas as pd
import random
from faker import Faker
fake = Faker()
from datetime import datetime, timedelta
import timeit


# generates range of list starting from 0 and allocates weight to it
def weight_list(value):
    weights = [x for x in range(0, value)]
    weights_sum = sum(weights)
    weights = [w/weights_sum for w in weights]
    weights.sort(reverse=True)
    return weights

def weight_of_list(list):
    weights = list.copy()
    weights_sum = sum(weights)
    weights = [w/weights_sum for w in weights]
    weights.sort(reverse=True)
    return weights



# import baseline
xl_baseline = path+r"/input/____simulation_baseline_ipba.xlsx"

xf = pd.ExcelFile(xl_baseline)
xf.sheet_names

df_diabetes_order = xf.parse('df_diabetes_order')
df_diabetes_ndcs = xf.parse('df_diabetes_ndcs')
df_diabetesx = xf.parse('df_diabetes')
featuresx = xf.parse('features')


#%% simulation

print('\ngenerating simulated data...')
start_time = timeit.default_timer()


#### 1. main structure

# relevant columns
df_diabetes_order = df_diabetes_order[~df_diabetes_order['field'].isnull()]
df_diabetes_order = df_diabetes_order.sort_values(['varnum']).reset_index(drop=True)[['column', 'field', 'derived_in']]

df_diabetes_excel = df_diabetes_order[df_diabetes_order['derived_in'].isin(['excel'])].reset_index(drop=True)
df_diabetes_python = df_diabetes_order[df_diabetes_order['derived_in'].isin(['py'])].reset_index(drop=True)



#### 2. ndcs
df_diabetes_ndcs = df_diabetes_ndcs.rename(columns=df_diabetes_order.set_index('column')['field'].to_dict())

# dictionary: patients will stay or move upward in the treatement_hierarchy for diabetes
ndcs = {k: g["ndc"].tolist() for k, g in df_diabetes_ndcs.groupby("treatement_hierarchy")}



#### 3. main table
# relevant fields only
df_diabetesx = df_diabetesx[df_diabetes_order['column']]

# rename fields: convert dataframe to dictionary
df_diabetesx = df_diabetesx.rename(columns=df_diabetes_order.set_index('column')['field'].to_dict())


# store informaiton into dictionary
claim_type = df_diabetesx['claim_type'].str.split(':', expand=True)[0]
claim_type = list(claim_type[claim_type.notnull()])

disease_type = df_diabetesx['disease_type'].str.split(':', expand=True)[0]
disease_type = list(disease_type[disease_type.notnull()])



#### 4. patient records across time
df_diabetes = pd.DataFrame()
nbr_of_patients = random.randint(min_patients, max_patients)
itr, total_records = 0, 0
for k in range(0, nbr_of_patients):
    
    itr+=1

    # fixed fields
    patient_idx = random.randint(20000, 99999)
    patient_birth_yearx = fake.date_time_between(start_date='-75y', end_date='-5y').year
    # genderx = random.choices(['m', 'f'], weights=[.5, .5])
    genderx = random.choices(['m', 'f'], weights=[0.60, 0.40])[0]
    # disease_typex = random.choice(disease_type)
    disease_typex = random.choices(disease_type, weights=[0.10, 0.90])[0]

    sd = fake.date_between(start_date='-4y', end_date='-1y') #start date for patient record
    ed = fake.date_between(start_date=sd+timedelta(days=random.randint(50,200)), end_date='-3m') 
    individual_records = int(((ed-sd).days) / 45)+1
    total_records += individual_records

    print(str(itr)+'/'+str(nbr_of_patients)+'\nsd: '+ str(sd) + '      ed: '+ str(ed) + '      patinet_id: '+str(patient_idx)+'        records:' + str(individual_records))

    sndc = random.choice(list(ndcs.keys())[:7]) # start group of ndc

    for i in range(0, individual_records):

        # unique fields
        claim_idx = random.randint(100000000, 999999999)
        claim_typex = ''.join(random.choices(population=claim_type, weights=[0.8, 0.1, 0.1])) #weighted probablity inclided towards pd claims
        claim_typey = claim_type.copy()
        claim_typey.remove('pd') #all except 'pd' claims
        quantityx = random.choice([15, 30, 45, 60, 75, 90])

        # interdependent fields
        if i == 0:
            service_datex = sd
            ndcx = random.choice(ndcs.get(sndc))
            days_supplyx = random.choice([15, 30, 45, 60, 75, 90])
        elif claim_typex in claim_typey:
            days_supplyw = weight_list(days_supplyx)
            #next days supply to be probabilistically close to previous for rv, rj claims
            service_datex = service_datex+timedelta(days=int(''.join(str(x) for x in random.choices(range(0, days_supplyx), days_supplyw))))
            ndcx = random.choice(ndcs.get(sndc))
            days_supplyx = random.choice([15, 30, 45, 60, 75, 90])
        else:
            service_datex = service_datex+timedelta(days=days_supplyx)
            ndcx = random.choice(ndcs.get(sndc))
            days_supplyx = random.choice([30, 45, 60, 90])

        tmp_dict = {'claim_id'                  : [claim_idx],
                    'patient_id'                : [patient_idx],
                    'ndc'                       : [ndcx],
                    'service_date'              : [service_datex],
                    'claim_type'                : [claim_typex],
                    'days_supply'               : [days_supplyx],
                    'quantity'                  : [quantityx],
                    'patient_birth_year'        : [patient_birth_yearx],
                    'gender'                    : [genderx],
                    'disease_type'              : [disease_typex],
                    '_sndc'                     : [sndc]}
    
        tmp_line = pd.DataFrame(tmp_dict)
        
        df_diabetes = pd.concat([df_diabetes, tmp_line])

df_diabetes = df_diabetes.reset_index(drop=True)

s1='\n total records generated: '+ str(total_records) + ' for '+ str(nbr_of_patients) + ' patients'
print(s1+'\n'+'-'*len(s1))
print('\n simulating multiple records for a patient...')



#### 5. one patient multiple drug consumption in a day

#taking 75* of the base population
df_diabetes_mult = df_diabetes.sample(int(len(df_diabetes)*.75))
df_diabetes_mult = df_diabetes_mult[['patient_id', 'service_date', '_sndc']].drop_duplicates()

#taking 25* of the sample population
df_diabetes_mult = df_diabetes_mult.sample(int(len(df_diabetes_mult)*.25))
df_diabetes_mult = df_diabetes_mult.sort_values(['patient_id', 'service_date']).reset_index(drop=True)

df_diabetes_mult_rec = pd.DataFrame()
for i in range(0, len(df_diabetes_mult)):

    ndc_keys = list(ndcs.keys())
    ndc_keys.remove(df_diabetes_mult.loc[i, '_sndc'])

    #multiplication factor
    #randomly assigned multiple combination of drug class i.e. patient taking drugs from different classes
    mfw = weight_of_list(ndc_keys)
    mf = int(''.join(str(x) for x in random.choices(ndc_keys, weights=mfw)))

    for n in range(0, mf):
        sndc = random.choice(ndc_keys)
        ndcx = random.choice(ndcs.get(sndc))
        patient_idx = int(df_diabetes_mult.loc[i, 'patient_id'])
        service_date = df_diabetes_mult.loc[i, 'service_date']

        tmp_dict = {'patient_id'    : [patient_idx],
                    'ndc'           : [ndcx],
                    'service_date'  : [service_date]
                   }

        tmp_line = pd.DataFrame(tmp_dict)
        df_diabetes_mult_rec = pd.concat([df_diabetes_mult_rec, tmp_line])

df_diabetes_mult = pd.merge(df_diabetes, df_diabetes_mult_rec, on=['patient_id', 'service_date'], how='left')


# convert float to integer
df_diabetes_mult['ndc'] = df_diabetes_mult[['ndc_y', 'ndc_x']].bfill(axis=1).iloc[:, 0].fillna(0.0).astype('int64')

df_diabetes_mult = df_diabetes_mult.drop(['ndc_x', 'ndc_y', '_sndc'], axis=1)
df_diabetes_mult = df_diabetes_mult.sort_values(['patient_id', 'service_date'])
df_diabetes_mult = df_diabetes_mult.reset_index(drop=True)


# fix claim_id
for i in range(1, len(df_diabetes_mult)):
    if df_diabetes_mult.loc[i-1, 'patient_id'] == df_diabetes_mult.loc[i, 'patient_id']:
        df_diabetes_mult.loc[i-1, 'claim_id'] = df_diabetes_mult.loc[i, 'claim_id']+i
    else:
        df_diabetes_mult.loc[i-1, 'claim_id'] = df_diabetes_mult.loc[i-1, 'claim_id']




#### 6. combine
df_diabetes = pd.merge(df_diabetes_mult, df_diabetes_ndcs, on=['ndc'], how='left')





# unique patients
df_patsx = df_diabetes[['patient_id', 'gender']].drop_duplicates().reset_index(drop=True)

# assign feature based on baseline proportions
def feature_roll(field):
    
    ff = featuresx[featuresx['feature'].isin([field])][['factors', 'share']].reset_index(drop=True)
    # statement = 'ff = featuresx[featuresx['+"'"+"feature"+"'"+"].isin(["+"'"+field+"'])][["+"'factors"+"', "+"'"+"share'"+']]'
    # exec(statement)
    if ff['share'].sum() != 1:
        print('error: the proportion of this variable does not equal 100%')
    
    factors, share = [], []
    for i in range(0, len(ff)):
        factors.append(ff.loc[i, 'factors'])
        share.append(ff.loc[i, 'share'])
    
    # add feature based on the baseline
    itr = 0
    for i in range(0, len(df_patsx)):
        itr+=1
        df_patsx.loc[i, field] = random.choices(factors, weights=share)[0]
        # statement = 'df_patsx.loc[i, '+"'"+field+"'"+"] = random.choices(factors, weights=share)[0]"
        # exec(statement)

    print('\nclassification summary: '+field)
    #statement = 'print(df_patsx['+"'"+field+"'].value_counts())"; exec(statement)
    statement = 'print(df_patsx['+"'"+field+"'].value_counts(normalize=True))"; exec(statement)


feature_roll('past_medical_history')
feature_roll('family_history')
feature_roll('is_there_complication')
feature_roll('marital_status')
feature_roll('educational_status')
feature_roll('employee_status')


# make space
# del [
#      df_diabetes_excel,
#      df_diabetes_mult,
#      df_diabetes_mult_rec,
#      df_diabetes_order,
#      df_diabetes_ndcs,
#      df_diabetes_python,
#      df_diabetesx
#     ]

elapsed = timeit.default_timer() - start_time
print('\nnote: time taken to simulate data', elapsed)


xf.close()



#### final dataset
df_diabetes = pd.merge(df_diabetes, df_patsx, how='left', on=['patient_id', 'gender'])


#%% explore

# data by date range
pd.to_datetime(df_diabetes['service_date']).dt.year.value_counts(normalize=True)*100

# proportion
df_diabetes.gender.value_counts()

df_diabetes.disease_type.value_counts()

# duration of patients stay
dur = pd.merge(df_diabetes.groupby('patient_id').agg({'service_date': 'min'}), 
         df_diabetes.groupby('patient_id').agg({'service_date': 'max'}),
         left_index=True, right_index=True
         ).reset_index().rename(columns={'service_date_x': 'start', 'service_date_y': 'end'})
duration = dur['end']-dur['start']
duration.describe()


#%% export

df_diabetes.to_excel(path+r"/output/df_diabetes_s2.xlsx")
#df_diabetes.to_csv('df_diabetes.csv', escapechar='|')
df_diabetes.to_pickle(path+r"/output/df_diabetes_s2.pickle")
