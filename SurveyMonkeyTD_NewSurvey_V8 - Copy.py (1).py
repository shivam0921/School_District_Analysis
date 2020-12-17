import os
import subprocess
from subprocess import check_call
import requests # http://docs.python-requests.org/en/latest/user/install/
import json
import pprint
import __future__ #for compatibility with Python 2.6 & 2.7
from time import gmtime, mktime, sleep, time
from datetime import timedelta, datetime
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
#import pyodbc
import pytz
import giraffez
import re
import snowflake.connector
import base64
import urllib3
#from awsbag_pyconns import sf_qdata

urllib3.disable_warnings()
# In[2]:
username = 'Obw984'
pwd = base64.b64decode("RG9sbGFyMTAw")

class rate_wait:
    """Non-threadsafe way to wait until the next time a
       rate-limited function should be called.
       Initialize by passing the maximum calls per second."""

    def __init__(self, maxPerSecond):
        self.last_called = 0.0
        self.min_interval = 1.0 / float(maxPerSecond)

    def wait(self):
        time_to_wait = self.last_called + self.min_interval - time()
        if time_to_wait > 0:
            sleep(time_to_wait)
        self.last_called_is_now()

    def last_called_is_now(self):
        self.last_called = time()


# In[3]:


### Token and survey monkey api set up

client = requests.session()
client.headers = {
    "Authorization": "bearer %s" % 'p6JlvfpYOdVAOiJplyxZBMOolkIVokSGt2JJDMzTW-aeWzcOVrxtzLrLEZMlVATWHopahhcKu7V.AOO-6IRjHAUqNyrU650caoG-ojf-kqg2Vzwlwkcg8UlDtW4wn9s3',
    "Content-Type": "application/json"
}

SURVEY_TITLE = "Post Engagement Survey" #use get_survey_list to find the survey ID
HOST = "https://api.surveymonkey.com"
MAX_REQUESTS_PER_SECOND = 2
POLL_CYCLE_LENGTH_IN_MINUTES = 60.0

# initialize objects used later on
pp = pprint.PrettyPrinter(indent=4)
limiter = rate_wait(MAX_REQUESTS_PER_SECOND)


# In[5]:


# add paths to HOST to create the URLs to survey list
surveys_url = "%s/v3/surveys/" % HOST

# In[6]:

http_proxy="http://"+username+":Dollar100@proxy.kdc.capitalone.com:8099"
https_proxy="http://"+username+":Dollar100@proxy.kdc.capitalone.com:8099"

proxies = {"http" : http_proxy, "https" : https_proxy}

client.proxies = proxies

# find the survey ID by title
limiter.wait() #avoid making requests too quickly
try:
    survey_data = client.get(surveys_url,verify=False)
except requests.exceptions.RequestException as e:
    print("Error finding survey by title: ",SURVEY_TITLE," Exception: ",e)
    exit()
limiter.last_called_is_now() #save the time when request completes
number_of_surveys_found = 0
if survey_data.status_code == 200: #the API responded
    survey_json = survey_data.json()
    for i in range(len(survey_json["data"])):
        if survey_json["data"][i]['title'] == SURVEY_TITLE:
            number_of_surveys_found += 1
            survey_id = survey_json["data"][i]["id"]
            survey_num = i
    if number_of_surveys_found > 1: #title search no specific enough
        print("Number of surveys found matching \"",SURVEY_TITLE,
              "\" is ",number_of_surveys_found,".")
        print("Please configure a more specific title.")
        print("note: searches are case insensitive.")
        print()
        for survey in survey_json["data"]["surveys"]:
            print("Found: ",survey["title"])
        exit()
    elif number_of_surveys_found == 0: #title search too specific or wrong account
        print("No survey was found matching the title: \"",SURVEY_TITLE,
              "\" for the authorized SurveyMonkey account.")
        exit()
else: #got an error before reaching the API. message below should help determine why.
    print("Error finding suvery by title: \"",SURVEY_TITLE,"\"")
    print(" Response code: ",survey_data.response_code," Message: ", survey_data.text)
    exit()
    
# In[7]:


# add paths to HOST to create the URLs to survey_id
surveys_id_url = "%s/v3/surveys/%s" % (HOST,survey_id)

# start checking for respondents at the time the survey was created
survey_id_data = client.get(surveys_id_url) 
survey_id_json = survey_id_data.json()

last_date_checked = survey_id_json["date_created"]
print("Found survey title: ",SURVEY_TITLE," survey_id: ",survey_id," created: ",last_date_checked)


# In[8]:


# add paths to HOST to create the URLs to details of the survey
surveys_id_detail_url = "https://api.surveymonkey.com/v3/surveys/%s/details" % (survey_id)
survey_id_detail_data = client.get(surveys_id_detail_url)
survey_id_detail_json = survey_id_detail_data.json()
survey_id_detail_json


# In[9]:


### create question list with question_id and textable question
question_list = []
question_id_list = []
for page_num in range(len(survey_id_detail_json["pages"])):
    for question_i in range(len(survey_id_detail_json["pages"][page_num]["questions"])):
        question_list.append(BeautifulSoup(survey_id_detail_json["pages"][page_num]["questions"][question_i]["headings"][0]["heading"], 'html.parser').text.replace(u'\xa0', u' '))
        question_id = survey_id_detail_json["pages"][page_num]["questions"][question_i]["id"]
        question_id_list.append(question_id)

df_q_l = pd.DataFrame()
df_q_l = pd.DataFrame({'question_id':question_id_list, 'question':question_list})   
df_q_l.head()


# In[10]:


### create answer list with question_id, answers_id, and texable answer choices
question_id_list = []
answers_id_list = []
answers_list = []
for page_i in range(len(survey_id_detail_json["pages"])):
        for question_i in range(len(survey_id_detail_json["pages"][page_i]["questions"])):
                if 'answers' in survey_id_detail_json["pages"][page_i]["questions"][question_i].keys():
                    if 'choices' in survey_id_detail_json["pages"][page_i]["questions"][question_i]["answers"].keys():
                        for answers_id_i in range(len(survey_id_detail_json["pages"][page_i]["questions"][question_i]["answers"]["choices"])):
                            answers_id = survey_id_detail_json["pages"][page_i]["questions"][question_i]["answers"]["choices"][answers_id_i]["id"]
                            answers_id_list.append(answers_id)
                            answers = survey_id_detail_json["pages"][page_i]["questions"][question_i]["answers"]["choices"][answers_id_i]["text"]
                            answers_list.append(answers)
                            question_id = survey_id_detail_json["pages"][page_i]["questions"][question_i]["id"]
                            question_id_list.append(question_id)
                    if 'other' in survey_id_detail_json["pages"][page_i]["questions"][question_i]["answers"].keys():
                        answers_id = survey_id_detail_json["pages"][page_i]["questions"][question_i]["answers"]["other"]["id"]
                        answers_id_list.append(answers_id)
                        answers = survey_id_detail_json["pages"][page_i]["questions"][question_i]["answers"]["other"]["text"]
                        answers_list.append(answers)
                        question_id = survey_id_detail_json["pages"][page_i]["questions"][question_i]["id"]
                        question_id_list.append(question_id)
                        

df_a_l = pd.DataFrame()
df_a_l = pd.DataFrame({'question_id':question_id_list, 'answers_id':answers_id_list, 'answers':answers_list})   
df_a_l.head()


# In[11]:


# add paths to HOST to create the two API endpoint URIs
respondent_url = "https://api.surveymonkey.com/v3/surveys/%s/responses/bulk" % (survey_id)


# In[12]:


###create responses dataframe using for loop
appended_data_l = []
text_data_l = []
appended_rid_dt_l = []
respondents_cur_page = 1 #start at page 1
while True: #get pages of respondents until there are no more
        page_num = {"page": respondents_cur_page}
        limiter.wait() #avoid making requests too quickly
        respondent_data = client.get(respondent_url, params=page_num)
        limiter.last_called_is_now()
        respondent_json = respondent_data.json() #decode JSON in respondent_data
        if len(respondent_json["data"]) == 0:
            break #all respondents gotten, break out of "while True:" loop
        
        #for respondent in respondent_json["data"]:
            # only want finished responses
            # discover the meaning of finished surveys http://goo.gl/JoRWb5
            #if respondent["response_status"] == "completed":
                #respondent_ids.append(respondent["id"])
            #respondent_ids.append(respondent["id"])
        response_id_l = []
        page_id_l = []
        question_id_l = []
        answers_id_l = []
        
        response_id_t_l = []
        question_id_t_l = []
        answers_id_t_l = []
        data = respondent_json["data"]
        #loop for questions and answers
        for call_i in range(len(data)):
            for page_i in range(len(data[call_i]["pages"])):
                for question_i in range(len(data[call_i]["pages"][page_i]["questions"])):
                    if 'other_id' in list(data[call_i]["pages"][page_i]["questions"][question_i]["answers"][0]):
                        for answers_i in range(len(data[call_i]["pages"][page_i]["questions"][question_i]["answers"])):
                            response_id = data[call_i]["id"]
                            response_id_l.append(response_id)
                            page_id = list(data[call_i]["pages"][page_i].values())[0]
                            page_id_l.append(page_id)
                            question_id = list(data[call_i]["pages"][page_i]["questions"][question_i].values())[0]
                            question_id_l.append(question_id)
                            answers_id = data[call_i]["pages"][page_i]["questions"][question_i]["answers"][answers_i]["other_id"]
                            answers_id_l.append(answers_id) 
                            
                            response_id_t_l.append(response_id)
                            question_id_t_l.append(question_id)
                            answers_id_t = data[call_i]["pages"][page_i]["questions"][question_i]["answers"][answers_i]["text"]
                            answers_id_t_l.append(answers_id_t)
                    else:
                        for answers_i in range(len(data[call_i]["pages"][page_i]["questions"][question_i]["answers"])):
                            response_id = data[call_i]["id"]
                            response_id_l.append(response_id)
                            page_id = list(data[call_i]["pages"][page_i].values())[0]
                            page_id_l.append(page_id)
                            question_id = list(data[call_i]["pages"][page_i]["questions"][question_i].values())[0]
                            question_id_l.append(question_id)
                            answers_id = list(data[call_i]["pages"][page_i]["questions"][question_i]["answers"][answers_i].values())[0]
                            answers_id_l.append(answers_id)                    
                    
        abc = pd.DataFrame()
        abc = pd.DataFrame({'response_id':response_id_l, 'page_id':page_id_l, 'question_id':question_id_l, 'answers_id':answers_id_l})   
        
        abc_text = pd.DataFrame()
        abc_text = pd.DataFrame({'response_id':response_id_t_l, 'question_id':question_id_t_l, 'answers_id':answers_id_t_l})   
                
        #loop for dates
        response_id_l = []
        start_dt_l = []
        end_dt_l = []
        for call_i in range(len(data)):
            response_id = data[call_i]["id"]
            response_id_l.append(response_id)
            start_dt = data[call_i]["date_created"]
            start_dt_l.append(start_dt)
            end_dt = data[call_i]["date_modified"]
            end_dt_l.append(end_dt)
        rid_dt = pd.DataFrame()
        rid_dt = pd.DataFrame({'response_id':response_id_l, 'start_dt':start_dt_l, 'end_dt':end_dt_l})   
        
        
        #add 1 to respondents_cur_page
        respondents_cur_page += 1 
        #create data list and concatenate to dataframe
        appended_data_l.append(abc)
        appended_data= pd.concat(appended_data_l)

        text_data_l.append(abc_text)
        text_data= pd.concat(text_data_l)
        
        appended_rid_dt_l.append(rid_dt)
        appended_rid_dt= pd.concat(appended_rid_dt_l)   
        
appended_data.head(5)


# In[13]:


appended_rid_dt['start_dt'] = pd.to_datetime(appended_rid_dt['start_dt'])
appended_rid_dt['end_dt'] = pd.to_datetime(appended_rid_dt['end_dt'])
appended_rid_dt['start_dt_EST'] = appended_rid_dt['start_dt'].dt.tz_localize('GMT').dt.tz_convert('US/Eastern').dt.date
appended_rid_dt['end_dt_EST'] = appended_rid_dt['end_dt'].dt.tz_localize('GMT').dt.tz_convert('US/Eastern').dt.date
appended_rid_dt.info()


# In[14]:


appended_rid_dt = appended_rid_dt.drop(['start_dt', 'end_dt'], axis=1)
appended_rid_dt.head()


# In[15]:


appended_data = appended_data.drop_duplicates(['response_id','question_id'])


# In[16]:


### Append question and answers to appended_data 
#append answers
result = pd.merge(appended_data, df_a_l, how='left', on=['question_id', 'answers_id'])

result_v2 = pd.merge(result, df_q_l, how='left', on=['question_id'])

result_v2.loc[:, 'answers_new'] = np.where(result_v2.answers.notnull(), result_v2.answers, result_v2.answers_id)

result_v3 = result_v2[['response_id', 'question', 'answers_new']]

result_v3.head()


# In[17]:


text_data_v2 = pd.merge(text_data, df_q_l, how='left', on=['question_id'])
text_data_v2.head(5)
text_data_pivot = text_data_v2.pivot(index='response_id', columns='question', values='answers_id')
text_data_pivot = text_data_pivot.reset_index()
text_data_pivot.rename_axis('', axis=1)
text_data_pivot = text_data_pivot.rename(columns={'Name of Activity': 'Name of Activity Other', 'Name of Workshop': 'Name of Workshop Other'})
text_data_pivot.head(5)


# In[18]:


result_pivot = result_v3.pivot(index='response_id', columns='question', values='answers_new')

q_order = df_q_l.question.tolist()

result_pivot = result_pivot.reset_index()[['response_id'] + q_order]

result_pivot.rename_axis('', axis=1)

result_pivot.head()


# In[19]:


result_final_v1 = pd.merge(result_pivot, appended_rid_dt, how='left', on=['response_id'])

result_final_v1 = result_final_v1.reset_index()[['response_id'] + ['start_dt_EST'] + ['end_dt_EST'] + q_order]

result_final_v1.rename_axis('', axis=1)

def clean_dataframe_column_names(df):
    cols = df.columns
    new_column_names = []

    for col in cols:
        new_col = col.lstrip().rstrip().lower().replace (" ", "_")
        new_col = re.sub(r'[^\w]', '', new_col).replace("__", "_")
        new_column_names.append(new_col)
        #df[col] = df[col].str[:4000]

    df.columns = new_column_names

result_final_v2 = pd.merge(result_final_v1, text_data_pivot, how='left', on=['response_id'])

clean_dataframe_column_names(result_final_v2)

#result_final_v2.info()


# In[20]:


final_column_names = [
'response_id',
'start_dt_est',
'end_dt_est',
'select_your_cafe_or_branch',
'enter_your_eid',
'engagement_date_time',
'engagement_inside_or_outside',
'what_engagement_did_you_do',
'which_activity_did_you_do',
'name_of_activity',
'activity_length_in_minutes',
'people_participated_activity',
'recommend_this_activity_again',
'anything_else_activity',
'what_event_did_you_host',
'name_of_event',
'event_length_in_minutes',
'people_participated_event',
'recommend_this_event_again',
'event_description',
'anything_else_event',
'which_workshop_you_facilitate',
'name_of_workshop',
'workshop_length_in_minutes',
'people_attended_the_workshop',
'recommend_this_workshop_again',
'anything_else_workshop',
'name_of_activity_other',
'name_of_workshop_other'
]
import pdb; pdb.set_trace()
if "name_of_event_y" in result_final_v2.columns:
    result_final_v2 = result_final_v2.drop('name_of_event_y', axis=1)

result_final_v2.columns = final_column_names




# In[21]:


result_final_v3 = result_final_v2.where((pd.notnull(result_final_v2)), None)

result_final_v3.head()


# ### Connect to Snowflake

# In[23]:

os.environ['http_proxy']="http://aws-proxy-prod.cloud.capitalone.com:8099"
os.environ['https_proxy']="http://aws-proxy-prod.cloud.capitalone.com:8099"
os.environ['no_proxy']="169.254.169.254,s3.amazonaws.com,.s3.amazonaws.com,.kdc.capitalone.com,.cloud.capitalone.com,.clouddqt.capitalone.com"
os.environ['NO_PROXY']="169.254.169.254,s3.amazonaws.com,.s3.amazonaws.com,.kdc.capitalone.com,.cloud.capitalone.com,.clouddqt.capitalone.com"


# Method Definitions
def _get_ftppwd_password(hostname, username):
    """
    Pull the password for a given username and hostname key

    hostname -- Historically a 3 digit code corresponding to the platform you want the password for
    username -- The username that is used to log into the above platform
    """
    try:
        password = subprocess.check_output(["ftppwd", hostname, username], stderr=subprocess.STDOUT)
        password = password.decode('UTF-8').rstrip("\n")
        return password
    except subprocess.CalledProcessError as thrown_error:
        error_msg = "ftppwd failed with message: %s" % thrown_error.output.rstrip("\n")
        raise EnvironmentError(thrown_error.returncode, error_msg)


# In[24]:


def snowflake_connection(username, password, account):
   ctx = snowflake.connector.connect(
       user=username,
       password=password,
       account=account
   )
   return ctx

ctx=snowflake_connection('OBW984',_get_ftppwd_password("SFK", "OBW984"),'prod.us-east-1.capitalone')

cursor=ctx.cursor()

cursor.execute('USE WAREHOUSE BARC_Q_MX')


# In[24]:


#Pull old survey table to dataframe (Old method)
#cursor.execute('select * from SB.lab_mx.SM_ACE_Engage_acx457_old1')
#names = [ x[0].lower() for x in cursor.description]
#rows = cursor.fetchall()

#old = pd.DataFrame( rows, columns=names)


# In[25]:


old = pd.read_sql('select * from SB.lab_mx.SM_ACE_Engage_acx457_old1', ctx)
old.columns = [x.lower() for x in old.columns]


# In[26]:


old.info()


# In[27]:


result_final_v3.info()


# In[28]:


final_table = pd.concat([result_final_v3,old])


# In[29]:


final_table.info()


# In[30]:


final_column_order = [
'response_id'
,'start_dt_est'
,'end_dt_est'
,'select_your_cafe_or_branch'
,'enter_your_eid'
,'engagement_date_time'
,'engagement_inside_or_outside'
,'what_engagement_did_you_do'
,'which_activity_did_you_do'
,'name_of_activity'
,'name_of_activity_other'
,'activity_length_in_minutes'
,'people_participated_activity'
,'recommend_this_activity_again'
,'anything_else_activity'
,'what_event_did_you_host'
,'name_of_event'
,'event_length_in_minutes'
,'people_participated_event'
,'recommend_this_event_again'
,'event_description'
,'anything_else_event'
,'which_workshop_you_facilitate'
,'name_of_workshop'
,'name_of_workshop_other'
,'workshop_length_in_minutes'
,'people_attended_the_workshop'
,'recommend_this_workshop_again'
,'anything_else_workshop'
,'what_engage_did_you_do'
,'engage_length_in_minutes'
,'people_participated_engage'
,'recommend_this_engage_again'
,'anything_else_engage'
]

final_table_v2 = final_table.reset_index()[final_column_order]

final_table_v2.rename_axis('', axis=1)

final_table_v3 = final_table_v2.where((pd.notnull(final_table_v2)), None)

final_table_v3.head()


# In[31]:


cursor.execute("""create or replace table SB.LAB_MX.SM_ACE_ENGAGE_OBW984_PV_ALL
(
response_id VARCHAR(200), 
start_dt_est DATE, 
end_dt_est DATE, 
select_your_cafe_or_branch VARCHAR(200), 
enter_your_eid VARCHAR(200), 
engagement_date_time VARCHAR(200), 
engagement_inside_or_outside VARCHAR(200), 
what_engagement_did_you_do VARCHAR(200), 
which_activity_did_you_do VARCHAR(200), 
name_of_activity VARCHAR(200), 
name_of_activity_other VARCHAR(1000), 
activity_length_in_minutes VARCHAR(200), 
people_participated_activity VARCHAR(200), 
recommend_this_activity_again VARCHAR(200), 
anything_else_activity VARCHAR(2000), 
what_event_did_you_host VARCHAR(200), 
name_of_event VARCHAR(200), 
event_length_in_minutes VARCHAR(200), 
people_participated_event VARCHAR(200), 
recommend_this_event_again VARCHAR(200), 
event_description VARCHAR(2000), 
anything_else_event VARCHAR(2000), 
which_workshop_you_facilitate VARCHAR(200), 
name_of_workshop VARCHAR(200), 
name_of_workshop_other VARCHAR(2000), 
workshop_length_in_minutes VARCHAR(200), 
people_attended_the_workshop VARCHAR(200), 
recommend_this_workshop_again VARCHAR(200), 
anything_else_workshop VARCHAR(2000),
what_engage_did_you_do VARCHAR(200),
engage_length_in_minutes VARCHAR(200),
people_participated_engage VARCHAR(200),
recommend_this_engage_again VARCHAR(200),
anything_else_engage VARCHAR(2000)
)
cluster by(response_id);
""")


# In[32]:


params = [list(x) for x in final_table_v3.values]


# In[33]:


insert_query = """INSERT INTO SB.LAB_MX.SM_ACE_ENGAGE_OBW984_PV_ALL (
response_id,
start_dt_est,
end_dt_est,
select_your_cafe_or_branch,
enter_your_eid,
engagement_date_time,
engagement_inside_or_outside,
what_engagement_did_you_do,
which_activity_did_you_do,
name_of_activity,
name_of_activity_other,
activity_length_in_minutes,
people_participated_activity,
recommend_this_activity_again,
anything_else_activity,
what_event_did_you_host,
name_of_event,
event_length_in_minutes,
people_participated_event,
recommend_this_event_again,
event_description,
anything_else_event,
which_workshop_you_facilitate,
name_of_workshop,
name_of_workshop_other,
workshop_length_in_minutes,
people_attended_the_workshop,
recommend_this_workshop_again,
anything_else_workshop,
what_engage_did_you_do,
engage_length_in_minutes,
people_participated_engage,
recommend_this_engage_again,
anything_else_engage
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
);"""


# In[34]:


cursor.executemany(insert_query, params)


# In[30]:


#grant access to PIQ657,NAJ933,UYD670


# In[ ]:


#writer = pd.ExcelWriter('qa_final.xlsx')
#final_table_v3.to_excel(writer,'Sheet1')
#writer.save()

