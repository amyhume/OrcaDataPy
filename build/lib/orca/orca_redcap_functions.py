#ORCA Redcap Functions
#all these functions are used for working with orca data projects in REDCap 
#e.g. pulling, cleaning, importing data
url = 'https://redcap.nyu.edu/api/'

#1-----------------------
def get_all_data(token):
    """
    Pulls all data within a redcap project

    Args:
        token (str): The API token for the project.

    Returns:
        pandas.DataFrame: A DataFrame with the retrieved data.
    """
    import requests
    import pandas as pd
    import io
   

    
    data = {
    'token': token,
    'content': 'record',
    'action': 'export',
    'format': 'csv',
    'type': 'flat',
    'csvDelimiter': '',
    'rawOrLabel': 'raw',
    'rawOrLabelHeaders': 'raw',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
    }
    r = requests.post(url,data=data)
    print('HTTP Status: ' + str(r.status_code))

    df = pd.read_csv(io.StringIO(r.text))

    return df
#-----------------------

#2-----------------------
def get_orca_data(token, form, raw_v_label = 'raw', form_complete = True):
    """
    Retrieve any ORCA form from a REDCap project using the API.

    Args:
        token (str): The API token for the project.
        form (str): The name of the REDCap form to retrieve data from.
        raw_v_label (str): The label for raw data fields (default is 'raw').
        form_complete (bool): Indicating whether to return all responses or just ones marked as complete (default is True).

    Returns:
        pandas.DataFrame: A DataFrame with the retrieved data.
    """
    import requests
    import pandas as pd
    import io
   
    if form_complete:
        record_filter = f"[{form}_complete]=2"
    else:
        record_filter = ""
    
    data = {
    'token': token,
    'content': 'record',
    'action': 'export',
    'format': 'csv',
    'type': 'flat',
    'csvDelimiter': '',
    'fields[0]': 'record_id',
    'forms[0]': form,
    'events[0]': 'orca_4month_arm_1',
    'rawOrLabel': raw_v_label,
    'rawOrLabelHeaders': 'raw',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'true',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
    }
    r = requests.post(url,data=data)
    print('HTTP Status: ' + str(r.status_code))

    df = pd.read_csv(io.StringIO(r.text))
    df = df[~df['record_id'].str.contains('TEST')]

    if form_complete:
        record_filter = f"{form}_complete"
        filtered_df = df[df[record_filter] == 2]
        return filtered_df
    else:
        return df
#-----------------------

#3-----------------------
def get_orca_field(token, field, raw_v_label = 'raw'):
    """
    Retrieve any ORCA field from a REDCap project using the API.

    Args:
        token (str): The API token for the project.
        field (str): The name of the REDCap field to retrieve data from.
        raw_v_label (str): The label for raw data fields (default is 'raw').

    Returns:
        pandas.DataFrame: A DataFrame with the retrieved record id, redcap event name and field.
    """
    import requests
    import pandas as pd
    import io

    #!/usr/bin/env python
    data = {
        'token': token,
        'content': 'record',
        'action': 'export',
        'format': 'csv',
        'type': 'flat',
        'csvDelimiter': '',
        'fields[0]': 'record_id',
        'fields[1]': field,
        'rawOrLabel': 'raw',
        'rawOrLabelHeaders': 'raw',
        'exportCheckboxLabel': 'false',
        'exportSurveyFields': 'false',
        'exportDataAccessGroups': 'false',
        'returnFormat': 'json'
    }
    r = requests.post('https://redcap.nyu.edu/api/',data=data)
    print('HTTP Status: ' + str(r.status_code))
    
    df = pd.read_csv(io.StringIO(r.text))
    df = df[~df['record_id'].str.contains('TEST')]
    df = df[df[field].notna()]
    return df
#-----------------------

#4-----------------------
def get_task_timestamps(token, record_id = None, transposed = False):
    """
    Retrieve task timestamps for a particular ID in real time (EST).

    Args:
        token (str): The API token for the project.
        record_id (str): the record id you wish to pull (e.g. '218'). Default is 'none' and will pull the whole dataset
        transposed: Whether you want it in long format (for just one id) - default is False. Can only mark as True if you also specify a record id

    Returns:
        pandas.DataFrame: A DataFrame with the retrieved record id, task marker, timestamp in est.
    """
    import requests
    import pandas as pd
    import io
    import pytz

    visit_notes = get_orca_data(token, form = "visit_notes_4m", form_complete=False)

    if record_id != None:
        visit_notes = visit_notes[visit_notes['record_id'] == record_id]
        visit_notes.reset_index(drop=True, inplace=True)
        visit_date = str(visit_notes['visit_date_4m'])
        visit_date = visit_date.split()[1]

        markers = visit_notes[['richards_start_4m', 'richards_end_4m', 'vpc_start_4m', 'vpc_end_4m','srt_start_4m', 'srt_end_4m', 'cecile_start_4m', 'cecile_end_4m','relational_memory_start_4m', 'relational_memory_end_4m', 'notoy_start_real_4m','notoy_end_real_4m', 'toy_start_real_4m', 'toy_end_real_4m']]
    else:
        markers = visit_notes[['record_id', 'richards_start_4m', 'richards_end_4m', 'vpc_start_4m', 'vpc_end_4m','srt_start_4m', 'srt_end_4m', 'cecile_start_4m', 'cecile_end_4m','relational_memory_start_4m', 'relational_memory_end_4m', 'notoy_start_real_4m','notoy_end_real_4m', 'toy_start_real_4m', 'toy_end_real_4m']]

    if transposed == True and record_id != None:
        markers = markers.transpose()
        markers = markers.rename_axis('marker').reset_index()
        markers.columns = ['marker', 'timestamp_est']
        markers['record_id'] = record_id
        markers['timestamp_est'] = pd.to_datetime(visit_date + ' ' + markers['timestamp_est'])
        markers = markers[['record_id', 'marker', 'timestamp_est']]
        markers['timestamp_est'] = markers['timestamp_est'].dt.tz_localize('America/New_York')  

    elif transposed == True and record_id == None:
        print('cannot transpose without selecting a record id')

    return markers
#-----------------------

#5-----------------------
def get_task_data(token, record_id = None, transposed = False):
    """
    Retrieve task data existence status for a particular ID/all ids.

    Args:
        token (str): The API token for the project.
        record_id (str): the record id you wish to pull (e.g. '218'). Default is 'none' and will pull the whole dataset
        transposed: Whether you want it in long format (for just one id) - default is False. Can only mark as True if you also specify a record id

    Returns:
        pandas.DataFrame: A DataFrame with the retrieved record id, task marker, timestamp in est.
    """
    import requests
    import pandas as pd
    import io
    visit_notes = get_orca_data(token, form = "visit_notes_4m", form_complete=False)

    if record_id != None:
        visit_notes = visit_notes[visit_notes['record_id'] == record_id]
        visit_notes.reset_index(drop=True, inplace=True)
        data_existence = visit_notes.loc[:,'richards_data_4m___cg_ecg':'freeplay_data_4m___video']
    else:
        record_ids = visit_notes['record_id']
        data_existence1 = visit_notes.loc[:,'richards_data_4m___cg_ecg':'freeplay_data_4m___video']
        data_existence = pd.concat([record_ids, data_existence1], ignore_index=True)
        data_existence = data_existence.rename(columns={data_existence.columns[0]: 'record_id'})

    if transposed == True and record_id != None:
        data_existence = data_existence.transpose()
        data_existence = data_existence.rename_axis('task_data').reset_index()
        data_existence.columns = ['task_data', 'present']
        data_existence['record_id'] = record_id
        data_existence = data_existence[['record_id', 'task_data', 'present']]
    elif transposed == True and record_id == None:
        print('cannot transpose without selecting a record id')
    
    return data_existence
#-----------------------

#6-----------------------
def get_task_completion(token, record_id = None, transposed = False):
    """
    Retrieve task data existence status for a particular ID/all ids.

    Args:
        token (str): The API token for the project.
        record_id (str): the record id you wish to pull (e.g. '218'). Default is 'none' and will pull the whole dataset
        transposed: Whether you want it in long format (for just one id) - default is False. Can only mark as True if you also specify a record id

    Returns:
        pandas.DataFrame: A DataFrame with the retrieved record id, task, completion status and reason why it's not fully complete.
    """
    import requests
    import pandas as pd
    import io
    visit_notes = get_orca_data(token, form = "visit_notes_4m", form_complete=False)

    if record_id != None:
        visit_notes = visit_notes[visit_notes['record_id'] == record_id]
        visit_notes.reset_index(drop=True, inplace=True)
        task_comp = visit_notes[['richards_comp_4m', 'vpc_comp_4m', 'srt_comp_4m', 'cecile_comp_4m', 'relational_memory_comp_4m', 'freeplay_comp_4m']]
        whys = visit_notes[['richards_why_4m', 'vpc_why_4m', 'srt_why_4m', 'cecile_why_4m', 'relational_memory_why_4m', 'freeplay_why_4m']]
    else:
        task_comp = visit_notes[['record_id','richards_comp_4m', 'richards_why_4m','vpc_comp_4m','vpc_why_4m', 'srt_comp_4m', 'srt_why_4m','cecile_comp_4m', 'cecile_why_4m','relational_memory_comp_4m', 'relational_memory_why_4m','freeplay_comp_4m', 'freeplay_why_4m']]
    
    if transposed == True and record_id != None:
        task_comp = task_comp.transpose()
        task_comp = task_comp.rename_axis('task').reset_index()
        task_comp.columns = ['task', 'completion_status']
        whys = whys.transpose()
        whys = whys.rename_axis('task').reset_index()
        whys.columns = ['task', 'incomplete_reason']
        whys = whys.drop(columns = ['task'])

        task_comp = pd.concat([task_comp, whys], axis=1)
        task_comp['record_id'] = record_id
        task_comp = task_comp[['record_id', 'task', 'completion_status', 'incomplete_reason']]
    elif transposed == True and record_id == None:
        print('cannot transpose without selecting a record id')

    task_comp['completion_status'] = task_comp['completion_status'].astype('Int64')
    task_comp['incomplete_reason'] = task_comp['incomplete_reason'].astype('Int64')
    return task_comp
#-----------------------

#7-----------------------
def get_task_info(token, record_id = None, transposed = False):
    """
    Retrieves 3 data frames - task timestamps, completion status, and data presence for all ids or a particular id.
    Returns in order of: task_completion, task_data, task_timestamps

    Args:
        token (str): The API token for the project.
        record_id (str): the record id you wish to pull (e.g. '218'). Default is 'none' and will pull the whole dataset
        transposed: Whether you want it in long format (for just one id) - default is False. Can only mark as True if you also specify a record id

    Returns:
        pandas.DataFrame: 3 DataFrames (runs get_task_completion, get_task_timestamps,get_task_data).

    """
    import requests
    import pandas as pd
    import io

    task_completion = get_task_completion(token, record_id, transposed)
    task_timestamps = get_task_timestamps(token, record_id, transposed)
    task_data = get_task_data(token, record_id, transposed)

    return task_completion, task_data, task_timestamps
#-----------------------

#8-----------------------
def get_movesense_numbers(token, record_id = None):
    """
    Pulls child and caregiver movesense device numbers for 4 months.

    Args:
        token (str): The API token for the project.
        record_id (str): the record id you wish to pull (e.g. '218'). Default is 'none' and will pull the whole dataset

    Returns:
        pandas.DataFrame: A DataFrame with record id, child device number and caregiver device number. If a single record id is specified, will return cg device number first, then child

    """
    import requests
    import pandas as pd
    import io
    visit_notes = get_orca_data(token, form = "visit_notes_4m", form_complete=False)

    if record_id != None:
        visit_notes = visit_notes[visit_notes['record_id'] == record_id]
        visit_notes.reset_index(drop=True, inplace=True)
        child_number = str(int(visit_notes['hr_device_child_4m']))
        parent_number = str(int(visit_notes['hr_device_cg_4m']))
        return parent_number, child_number
    else:
        visit_notes = visit_notes[['record_id', 'hr_device_cg_4m', 'hr_device_child_4m']]
        visit_notes['hr_device_cg_4m'] = visit_notes['hr_device_cg_4m'].astype('Int64')
        visit_notes['hr_device_child_4m'] = visit_notes['hr_device_child_4m'].astype('Int64')
        return visit_notes
#-----------------------

#9-----------------------
def check_timestamps(token, record_id):
    """
    Pulls task info, and checks to see if there's any incorrectly missing timestamps.

    Args:
        token (str): The API token for the project.
        record_id (str): the record id you wish to pull (e.g. '218')

    Returns:
        pandas.DataFrame: A character vector with the tasks that have incorrect/missing timestamps

    """
    import requests
    import pandas as pd
    import io
    comp, data, timestamps = get_task_info(token, record_id=record_id, transposed=True)

    missing_timestamps = []
    #richards
    if data.iloc[0,2] == 1 | data.iloc[1,2]:
        if pd.isna(timestamps.iloc[0, 2]) or pd.isna(timestamps.iloc[1, 2]):
            missing_timestamps.append('richards')
    #vpc
    if data.iloc[3,2] == 1 | data.iloc[4,2]:
        if pd.isna(timestamps.iloc[2, 2]) or pd.isna(timestamps.iloc[3, 2]):
            missing_timestamps.append('vpc')
    #srt
    if data.iloc[6,2] == 1 | data.iloc[7,2]:
        if pd.isna(timestamps.iloc[4, 2]) or pd.isna(timestamps.iloc[5, 2]):
            missing_timestamps.append('srt')
    #cecile
    if data.iloc[9,2] == 1 | data.iloc[10,2]:
        if pd.isna(timestamps.iloc[6, 2]) or pd.isna(timestamps.iloc[7, 2]):
            missing_timestamps.append('cecile')
    #relational memory
    if data.iloc[12,2] == 1 | data.iloc[13,2]:
        if pd.isna(timestamps.iloc[8, 2]) or pd.isna(timestamps.iloc[9, 2]):
            missing_timestamps.append('relational_memory')
    
    #freeplay
    visit_notes = get_orca_data(token, form = "visit_notes_4m", form_complete=False)
    visit_notes = visit_notes[visit_notes['record_id'] == record_id]
    visit_notes.reset_index(drop=True, inplace=True)
    no_toy = visit_notes['freeplay_conditions_4m___1'].iloc[0]
    toy = visit_notes['freeplay_conditions_4m___2'].iloc[0]

    #freeplay
    if data.iloc[15,2] == 1 | data.iloc[16,2]:
        if no_toy == 1 and toy == 1:
            if pd.isna(timestamps.iloc[10, 2]) or pd.isna(timestamps.iloc[11, 2]) or pd.isna(timestamps.iloc[12, 2]) or pd.isna(timestamps.iloc[13, 2]):
                missing_timestamps.append('freeplay')
        elif no_toy == 1 and toy != 1:
            if pd.isna(timestamps.iloc[10, 2]) or pd.isna(timestamps.iloc[11, 2]):
                missing_timestamps.append('notoy_freeplay')
        elif no_toy != 1 and toy == 1:
            if pd.isna(timestamps.iloc[12, 2]) or pd.isna(timestamps.iloc[13, 2]):
                missing_timestamps.append('toy_freeplay')

    return missing_timestamps
#-----------------------

#10-----------------------
def get_movesense_times(token, record_id, who):
    """
    Pulls test recording times for each movesense device in order parent, child.

    Args:
        token (str): The API token for the project.
        record_id (str): the record id you wish to pull (e.g. '218')
        who (str): 'cg' or 'child' 

    Returns:
        a character vector with the on time followed by off time

    """
    import requests
    import pandas as pd
    import io
    from datetime import datetime
    import numpy as np
    import pytz

    visit_notes = get_orca_data(token, form = "visit_notes_4m", form_complete=False)
    movesense_times = visit_notes[['record_id', 'cg_movesense_on_4m',"child_movesense_on_4m", "cg_movesense_off_4m", "child_movesense_off_4m"]]
    movesense_times = movesense_times[movesense_times['record_id'] == record_id]

    date = get_orca_field(token, field = 'visit_date_4m')
    date = date[date['record_id'] == record_id]
    date = str(date['visit_date_4m'])
    date = date.split()[1]

    if who == 'cg':
        on_time = str(movesense_times['cg_movesense_on_4m']).split()[1]
        off_time = str(movesense_times['cg_movesense_off_4m']).split()[1]
        if on_time != 'NaN' and off_time != 'NaN':
            on_time = pd.to_datetime(date+ ' ' + on_time)
            off_time = pd.to_datetime(date+ ' ' + off_time)
        elif on_time != 'NaN' and off_time == 'NaN':
            on_time = pd.to_datetime(date+ ' ' + on_time)
        elif on_time == 'NaN' and off_time != 'NaN':
            off_time = pd.to_datetime(date+ ' ' + off_time)
        
    elif who == 'child':
        on_time = str(movesense_times['child_movesense_on_4m']).split()[1]
        off_time = str(movesense_times['child_movesense_off_4m']).split()[1]
        if on_time != 'NaN' and off_time != 'NaN':
            on_time = pd.to_datetime(date+ ' ' + on_time)
            off_time = pd.to_datetime(date+ ' ' + off_time)
        elif on_time != 'NaN' and off_time == 'NaN':
            on_time = pd.to_datetime(date+ ' ' + on_time)
        elif on_time == 'NaN' and off_time != 'NaN':
            off_time = pd.to_datetime(date+ ' ' + off_time)

    on_time = pytz.timezone('America/New_York').localize(on_time) if on_time != 'NaN' else pd.NaT
    off_time = pytz.timezone('America/New_York').localize(off_time) if off_time != 'NaN' else pd.NaT

    return on_time, off_time
#-----------------------

#11-----------------------
def get_visit_datetime(token, record_id = None, merged = True):
    """
    Pulls the visit start date time for all ids / a specified ID

    Args:
        token (str): The API token for the project.
        record_id (str): record_id you want to specify. Default is None and will pull all IDs
        merged (bool): Whether to return the merged datetime or date and time as separate columns. Default is True

    Returns:
        pandas.DataFrame: if record_id = None or merged = False containing record_id, date, time, datetime
        datetime value: a tz-conscious datetime variable representing the start of the visit, if record_id != None and merged = True
    """
    import requests
    import pandas as pd
    import io
    from datetime import datetime
    import pytz

    visit_notes = get_orca_data(token, form = "visit_notes_4m", form_complete=False)
    data = visit_notes[['record_id', 'visit_date_4m', 'visit_time_4m']]
    data = data[data['visit_date_4m'].notna() | data['visit_time_4m'].notna()]

    data['visit_date_4m'] = pd.to_datetime(data['visit_date_4m'])
    data['visit_time_4m'] = pd.to_datetime(data['visit_time_4m'], format='%H:%M').dt.time

    if record_id == None and merged == False:
        return data
    elif record_id == None and merged == True:
        data['visit_datetime_4m'] = data['visit_date_4m'] + pd.to_timedelta(data['visit_time_4m'].astype(str))
        data['visit_datetime_4m'] = data['visit_datetime_4m'].dt.tz_localize('America/New_York')
        return data
    elif record_id != None and merged == False:
        data = data[data['record_id'] == record_id]
        return data
    elif record_id != None and merged == True:
        data = data[data['record_id'] == record_id]
        data['visit_datetime_4m'] = data['visit_date_4m'] + pd.to_timedelta(data['visit_time_4m'].astype(str))
        data['visit_datetime_4m'] = data['visit_datetime_4m'].dt.tz_localize('America/New_York')
        value = data['visit_datetime_4m'].iloc[0]
        return value
#-----------------------

#12-----------------------
def import_data(token, data):
    """
    Imports pandas dataframe into redcap

    Args:
        token (str): The API token for the project.
        data (pandas.DataFrame): Data you want to import. Column names must be same as field names in codebook. redcap event name must be included.

    """
    import pandas as pd
    from redcap import Project, RedcapError
    unique_events = data['redcap_event_name'].unique()
    all = get_all_data(token)

    all = all[all['redcap_event_name'].isin(unique_events)]

    all = all[data.columns.intersection(all.columns)]
    test = pd.merge(all, data, on=['record_id', 'redcap_event_name'], how='right')
    columns = [col for col in all.columns if col not in ['record_id', 'redcap_event_name']]

    #checking conflicts
    conflicts_for = []
    for column in columns:
        column_data = test[['record_id', 'redcap_event_name', column+'_x', column+'_y']]
        column_data = column_data.dropna(subset=[column_data.columns[2], column_data.columns[3]])
        column_data = column_data[column_data.iloc[:, 2] != column_data.iloc[:, 3]]

        if len(column_data) >= 1:
            print('\n','conflict found for field: ', column)
            print('\n', column_data.to_string())
            print("\n")
            conflicts_for.append(column)
    
    if len(conflicts_for) >= 1:
        print("Check the datasets above carefully. columns x represent the existing data contents, column y represents the data that will overwrite\n",
        "If column x contains data, this import will OVERWRITE that existing data\n",
        "If the cell contents are the same, there is no new data to import")
    else:
        print("\n", 'no conflicts found. No data will be overwritten. check the import data carefully:',"\n\n",data, "\n")

    response = input("Do you want to continue? (y/n): ")

    if response.lower() == 'y':
        project = Project(url, token)
        # Convert DataFrame to a list of dictionaries (one dictionary per row)
        records = data.to_dict(orient='records')

        try:
            import_status = project.import_records(records)
            print("Data import completed for ", import_status['count'], " record(s)")
        except RedcapError as e:
            print(f"Error importing data: {e}")
    else:
        print('\n','Data import terminated')
#-----------------------


