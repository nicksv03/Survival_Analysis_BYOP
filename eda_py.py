#%% about

"""
EDA: APLD (anonymised patient level data)

Field                	Description                                                              
claim_id             	Insurance claim number (unique)                                          
patient_id           	Patient unique ID                                                        
service_date         	Date when the claim was adjudicated                                      
claim_type           	Paid, rejected or reversed claim                                         
days_supply          	Number of days drugs was supplied for                                    
quantity             	Quanity (Capsules)                                                       
patient_birth_year   	Date of Birth of Patient                                                 
gender               	Gender of Patient                                                        
disease_type         	T1: Type I or T2: Type II diabetes                                       
ndc                  	Product ID or Unique Drug code                                           
drug_name            	Brand name of the drug                                                   
class                	Category of Drug                                                         
sub_class            	Sub Category of Drug                                                     
mode_of_dispensation 	Way drug is dispensed, oral or injectable                                
treatement_hierarchy 	The order in which drugs are generally prescribed as diabetes intensifies
past_medical_history 	Yes or no                                                                
family_history       	Of diabetes, Yes or no                                                   
is_there_complication	Yes or no                                                                
marital_status       	Married, Unmarried                                                       
educational_status   	Educated, Uneducated                                                     
employee_status      	Employed, Not Employed                                                   
"""


#%% dependencies

import numpy as np
import pandas as pd
import pymysql.cursors
import warnings
warnings.filterwarnings('ignore')
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os
path = r'/Users/nikhil/Library/CloudStorage/GoogleDrive-dcbs17nikhilverma@gmail.com/My Drive/Github/Survival_Analysis_BYOP'
os.chdir(path.replace('\\', '/'))

#%% data.import

apld = pd.read_pickle(path+r'/dataset/output/df_diabetes_s1.pickle')
#%% data.filter

# paid claims only
apld = apld[apld['claim_type'].isin(['pd'])]

# T2D only
apld = apld[apld['disease_type'].isin(['t2'])]

# 4 year period only
apld = apld[apld['service_date'].between('2019-01-01', '2022-12-31')]


#%% eda.data structure

apld.info()
apld.head(2).T


#%% eda.claim_id

apld['claim_id'].head(10)

apld['claim_id'].count()

len_min = apld['claim_id'].astype('str').str.len().min()
len_max = apld['claim_id'].astype('str').str.len().max()
print(' claim_id min length = '+str(len_min)+'\n claim_id max length = '+str(len_max))

apld['dup'] = apld.duplicated('claim_id', keep=False)
apld['dup'].value_counts()
dups = apld[apld['dup'] == 1].sort_values(by=['claim_id'])

# 17 duplicate claim_ids
len(dups['claim_id'].unique())
# ~even though we have duplicate claims they are not duplicate per patient
dups.duplicated(['claim_id', 'patient_id'], keep=False).sum()

# ~claim_id count trend: there is an issue in the claim_id count trend, it is downward
trend = apld.groupby(['service_date']).agg({'service_date': 'count', 'claim_id': 'nunique'})\
			.rename(columns={'service_date': 'count', 'claim_id': 'nunique'})
trend = trend.resample('M').sum()
sns.lineplot(data=trend, palette="tab10", linewidth=2.5).set_title("claim_id count trend")
plt.show()



#%% eda.patient_id

apld['patient_id'].head(10)

# ~7,718 unique patients
apld['patient_id'].nunique()

len_min = apld['patient_id'].astype('str').str.len().min()
len_max = apld['patient_id'].astype('str').str.len().max()
print(' patient_id min length = '+str(len_min)+'\n patient_id max length = '+str(len_max))

# ~trend
trend = apld.groupby(['service_date']).agg({'service_date': 'count', 'patient_id': 'nunique'})\
			.rename(columns={'service_date': 'count', 'patient_id': 'nunique'})
trend = trend.resample('M').sum()
sns.lineplot(data=trend, palette="tab10", linewidth=2.5).set_title("patient_id count trend")
plt.show()


#%% eda.service_date

apld['service_date'].min()
apld['service_date'].max()


#%% eda.claim_type

apld['claim_type'].unique()
apld['claim_type'].nunique()


#%% eda.days_supply

apld['days_supply'].min()
apld['days_supply'].max()
apld['days_supply'].unique()
apld['days_supply'].value_counts()
# ~ age group 15 just 1 patient, no age group 75, rest equally distributed
apld['days_supply'].value_counts(normalize=True).round(2)


#%% eda.quantity

apld['quantity'].min()
apld['quantity'].max()
apld['quantity'].unique()
apld['quantity'].value_counts()
apld['quantity'].value_counts(normalize=True).round(2)




#%% eda.patient_birth_year

apld['patient_birth_year'].min()
apld['patient_birth_year'].max()

trend = apld.groupby(['patient_birth_year']).agg({'patient_id': 'nunique'})\
			.rename(columns={'patient_id': 'patient_id_count'})

# ~ we have total 9,483 unique patients however if we look by patient birth year it comes to 10,065 that means a patient may have multiple birth years
trend.sum()

# ~find patients with multiple birth dates
apld_birth = apld[['patient_id', 'patient_birth_year']].drop_duplicates().reset_index(drop=True)
apld_birth['dup_birth'] = apld_birth.duplicated(['patient_id'], keep=False)
apld_birth['dup_birth'].value_counts()
dups_birth = apld_birth[apld_birth['dup_birth'] == 1].sort_values(by=['patient_id'])
#some patients have multiple birth dates
dups_birth['patient_id'].value_counts()
#check few patient records
dups_birth[dups_birth['patient_id'].isin([67773, 94167, 67985])]

# ~there is a dip in patient count for patients born in the decade of 1980
trend = apld_birth.groupby(['patient_birth_year']).agg({'patient_id': 'nunique'})\
    		.rename(columns={'patient_id': 'patient_id_count'})\
			.reset_index()
trend['patient_birth_year'] = pd.to_datetime(trend['patient_birth_year'], format = '%Y').dt.year
sns.lineplot(data=trend.set_index(['patient_birth_year']), palette="tab10", linewidth=2.5)
plt.show()



#%% eda.gender

apld['gender'].value_counts()
# m: 51%, f: 49%
apld['gender'].value_counts(normalize=True).round(2)

trend = apld.groupby(['service_date', 'gender']).agg({'patient_id': 'nunique'}).reset_index()
trend['service_date'] = pd.to_datetime(trend['service_date'], format = '%Y-%m-%d').dt.year
trend = trend.set_index(['service_date'])
trend = trend.reset_index().rename(columns={'patient_id': 'nunique'})
sns.barplot(data=trend, x="service_date", y="nunique", hue="gender", errorbar=None)
plt.show()



#%% eda.disease_type

apld['disease_type'].value_counts()
apld['disease_type'].value_counts(normalize=True).round(2)

# ~pattern looks fine
trend = apld.groupby(['service_date', 'disease_type']).agg({'patient_id': 'nunique'}).reset_index()
trend['service_date'] = pd.to_datetime(trend['service_date'], format = '%Y-%m-%d').dt.year
trend = trend.set_index(['service_date'])
trend = trend.reset_index().rename(columns={'patient_id': 'nunique'})
sns.barplot(data=trend, x="service_date", y="nunique", hue="disease_type", errorbar=None)
plt.show()



#%% eda.ndc, drug name

apld['drug_name'].value_counts()

# ~some weakness in the model as the drug shares don't represent real world
apld['drug_name'].value_counts(normalize=True).round(2)
apld['drug_name'].value_counts(normalize=True).round(2).plot(kind='bar'); plt.show()

#%% top 5 drugs
# ~can we do a comparison of top 5 drugs vs rest?
apld['drug_name'].value_counts(normalize=True).round(2).head(5)
#plot
apld['drug_name'].value_counts(normalize=True).round(2).head(5).plot(kind='bar'); plt.show()




#%% eda.class, subclass

# ~3 classes and 9 subclass of drugs
apld['class'].value_counts()
# ~can we do comparison in tot of the class of drugs?
apld['class'].value_counts(normalize=True).round(2)
#plot
apld['class'].value_counts(normalize=True).round(2).plot(kind='bar'); plt.show()

apld['sub_class'].value_counts()
apld['sub_class'].value_counts(normalize=True).round(2)
#plot
apld['sub_class'].value_counts(normalize=True).round(2).plot(kind='bar'); plt.show()



#%% eda.mode_of_dispensation

apld['mode_of_dispensation'].value_counts()
# ~can we do comparison in tot in mode_of_dispensation?
apld['mode_of_dispensation'].value_counts(normalize=True).round(2)
#plot
apld['mode_of_dispensation'].value_counts(normalize=True).round(2).plot(kind='bar'); plt.show()



#%% eda.treatement_hierarchy

# ~no number 4
apld['treatement_hierarchy'].value_counts()
apld['treatement_hierarchy'].value_counts(normalize=True).round(2)
# ~higher % of claim counts within early stage of treatement
# ~can we do comparison in tot in treatement_hierarchy?
apld['treatement_hierarchy'].value_counts(normalize=True).round(2).reset_index().sort_values(['index'])
#plot
apld['treatement_hierarchy'].value_counts(normalize=True).round(2).sort_index().plot(kind='bar'); plt.show()



#%% eda.past_medical_history

# ~28% patients have past medical history
apld['past_medical_history'].value_counts()
apld['past_medical_history'].value_counts(normalize=True).round(2)
#plot
apld['past_medical_history'].value_counts(normalize=True).round(2).plot(kind='bar'); plt.show()



#%% eda.family_history

# ~19% patients have family_history
apld['family_history'].value_counts()
apld['family_history'].value_counts(normalize=True).round(2)
#plot
apld['family_history'].value_counts(normalize=True).round(2).plot(kind='bar'); plt.show()


#%% eda.is_there_complication

# ~67% patients have is_there_complication
apld['is_there_complication'].value_counts()
apld['is_there_complication'].value_counts(normalize=True).round(2)
#plot
apld['is_there_complication'].value_counts(normalize=True).round(2).plot(kind='bar'); plt.show()



#%% eda.marital_status

# ~62% patients are married
apld['marital_status'].value_counts()
apld['marital_status'].value_counts(normalize=True).round(2)
#plot
apld['marital_status'].value_counts(normalize=True).round(2).plot(kind='bar'); plt.show()


#%% eda.educational_status

# ~76% patients are educated
apld['educational_status'].value_counts()
apld['educational_status'].value_counts(normalize=True).round(2)
#plot
apld['educational_status'].value_counts(normalize=True).round(2).plot(kind='bar'); plt.show()


#%% eda.employee_status

# ~50% patients are unemployed
apld['employee_status'].value_counts()
apld['employee_status'].value_counts(normalize=True).round(2)
#plot
apld['employee_status'].value_counts(normalize=True).round(2).plot(kind='bar'); plt.show()



#%% feature.create

# calculate patient age
#take max birth date
_age = apld.groupby('patient_id').agg({'patient_birth_year': 'max'}).reset_index()
now = datetime.now()
_age['age'] = now.year - _age['patient_birth_year']

# derive tot
apld['drug_supply_next_dt'] = apld['service_date'] + pd.to_timedelta(apld['days_supply'], unit='d')
#apld[['claim_id', 'patient_id', 'service_date', 'days_supply', 'drug_supply_next_dt']]

# patients prospetive first and last date on therapy
_tot = apld.groupby('patient_id').agg({'service_date': 'min', 'drug_supply_next_dt': 'max'}).reset_index().rename(columns={'service_date': 'drug_supply_first_dt', 'drug_supply_next_dt': 'drug_supply_last_dt'})

#no duplicates
_tot['dup'] = _tot.duplicated('patient_id', keep=False)
_tot['dup'].value_counts()
del _tot['dup']
#dups = _tot[_tot['dup'] == 1].sort_values(by=['patient_id'])

# derive tot
_tot['tot'] = (_tot['drug_supply_last_dt'] - _tot['drug_supply_first_dt']).dt.days
_tot['total_time'] = (pd.Timestamp('2022-12-31') - pd.Timestamp('2019-01-01')).days

# derive survived flag
_tot['survived'] = np.where(_tot['drug_supply_last_dt'] <= '2022-12-31', '1', '0')



# survival summary
_tot['survived'].value_counts()
# overall ~69% of the patients died
_tot['survived'].value_counts(normalize=True)
#plot
_tot['survived'].value_counts(normalize=True).plot(kind='bar'); plt.show()


# derive pdc
#days covered
_dc = apld.groupby('patient_id').agg({'days_supply': 'sum'}).reset_index().rename(columns={'days_supply': 'pdc'})
#apld.head(2).T


# combine
apld = pd.merge(apld, _tot[['patient_id', 'drug_supply_first_dt', 'drug_supply_last_dt', 'tot', 'survived', 'total_time']], on=['patient_id'], how='left')
apld = pd.merge(apld, _dc, on=['patient_id'], how='left')
apld = pd.merge(apld, _age[['patient_id', 'age']], on=['patient_id'], how='left')
#apld.head(2).T





#%% decide on cohort

input = ['patient_id', 'drug_supply_first_dt', 'gender', 'age', 'past_medical_history',
         'family_history', 'is_there_complication', 'marital_status',
         'educational_status', 'employee_status', 'tot', 'survived',
         'total_time', 'pdc']

#filters: claim_type='pd
df_apld = apld[input].drop_duplicates()
df_apld.head(2).T

# duplicates
# ~416 entries need a fix
df_apld['dup'] = df_apld.duplicated('patient_id', keep=False)
df_apld['dup'].value_counts()
dups = df_apld[df_apld['dup'] == 1].sort_values(by=['patient_id'])
#sample
dups[dups['patient_id'].isin([20115])].T

# ~ drop duplicates
df_apld = df_apld[df_apld['dup'] == 0]
del df_apld['dup']

# trend
trend = df_apld.groupby(['drug_supply_first_dt']).agg({'drug_supply_first_dt': 'count', 'patient_id': 'nunique'})\
			.rename(columns={'drug_supply_first_dt': 'count', 'patient_id': 'nunique'})
trend = trend.resample('M').sum()
sns.lineplot(data=trend, palette="tab10", linewidth=2.5).set_title("patient_id count trend")
plt.show()

# take cohort of patients who started journey in Jan 2019 - Jun 2019
df_cohort = df_apld[df_apld['drug_supply_first_dt'].between('2019-01-01', '2019-06-30')].reset_index(drop=True)
df_cohort.head(2).T

df_cohort.age.min()
df_cohort.age.max()

























