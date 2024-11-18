url = 'https://redcap.nyu.edu/api/'
#ORCA Redcap Functions
#all these functions are used for working with orca data projects in REDCap 
#e.g. pulling, cleaning, importing data


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

def get_orca_data(token, form, raw_v_label = 'raw', timepoint = 'all',form_complete = True):
    """
    Retrieve any ORCA form from a REDCap project using the API.

    Args:
        token (str): The API token for the project.
        form (str): The name of the REDCap form to retrieve data from.
        raw_v_label (str): The label for raw data fields (default is 'raw').
        timepoint(str): The redcap event name for the event you wish to pull. Default is all
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
        df = df[df[record_filter] == 2]

    if timepoint != 'all':
        df = df[df['redcap_event_name'] == timepoint]
    
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

token = '25120FD84FFDA3B220617BDF23B680CD'
record_id = '209'
#4-----------------------
def get_task_timestamps(token, record_id = None, transposed = False, timepoint = 'orca_4month_arm_1'):
    """
    Retrieve task timestamps for a particular ID in real time (EST).

    Args:
        token (str): The API token for the project.
        record_id (str): the record id you wish to pull (e.g. '218'). Default is 'none' and will pull the whole dataset
        transposed: Whether you want it in long format (for just one id) - default is False. Can only mark as True if you also specify a record id
        timepoint (str): the redcap event name of the timepoint you wish to pull. Default is orca_4month_arm_1

    Returns:
        pandas.DataFrame: A DataFrame with the retrieved record id, task marker, timestamp in est.
    """
    import requests
    import pandas as pd
    import io
    import pytz
    if timepoint == 'orca_4month_arm_1':
        visit_notes = get_orca_data(token, form = "visit_notes_4m", timepoint=timepoint,form_complete=False)

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
    
    elif timepoint == 'orca_8month_arm_1':
        visit_notes = get_orca_data(token, form="visit_notes_8m", timepoint=timepoint,form_complete=False)
        visit_notes = visit_notes[visit_notes['redcap_event_name'] == 'orca_8month_arm_1']

        if record_id != None:
            visit_notes = visit_notes[visit_notes['record_id'] == record_id]
            visit_notes.reset_index(drop=True, inplace=True)
            visit_date = str(visit_notes['visit_date_8m'])
            visit_date = visit_date.split()[1]

            markers = visit_notes[['richards_start_8m', 'richards_end_8m', 'vpc_start_8m', 'vpc_end_8m','srt_start_8m', 'srt_end_8m', 'pa_start_8m', 'pa_end_8m','relational_memory_start_8m', 'relational_memory_end_8m','cecile_start_8m', 'cecile_end_8m', 'notoy_start_real_8m','notoy_end_real_8m', 'toy_start_real_8m', 'toy_end_real_8m']]
        else:
            markers = visit_notes[['record_id','richards_start_8m', 'richards_end_8m', 'vpc_start_8m', 'vpc_end_8m','srt_start_8m', 'srt_end_8m', 'pa_start_8m', 'pa_end_8m','relational_memory_start_8m', 'relational_memory_end_8m','cecile_start_8m', 'cecile_end_8m', 'notoy_start_real_8m','notoy_end_real_8m', 'toy_start_real_8m', 'toy_end_real_8m']]
        
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
def get_task_data(token, record_id = None, transposed = False, timepoint='orca_4month_arm_1'):
    """
    Retrieve task data existence status for a particular ID/all ids.

    Args:
        token (str): The API token for the project.
        record_id (str): the record id you wish to pull (e.g. '218'). Default is 'none' and will pull the whole dataset
        transposed: Whether you want it in long format (for just one id) - default is False. Can only mark as True if you also specify a record id
        timepoint (str): the redcap event name of the timepoint you wish to pull. Default is orca_4month_arm_1
    Returns:
        pandas.DataFrame: A DataFrame with the retrieved record id, task marker, timestamp in est.
    """
    import requests
    import pandas as pd
    import io

    if timepoint == 'orca_4month_arm_1':
        visit_notes = get_orca_data(token, form = "visit_notes_4m", form_complete=False, timepoint=timepoint)

        if record_id != None:
            visit_notes = visit_notes[visit_notes['record_id'] == record_id]
            visit_notes.reset_index(drop=True, inplace=True)
            data_existence = visit_notes.loc[:,'richards_ecg_cg_data_4m':'fp_video_data_4m']
        else:
            record_ids = visit_notes['record_id']
            data_existence1 = visit_notes.loc[:,'richards_ecg_cg_data_4m':'fp_video_data_4m']
            data_existence = pd.concat([record_ids, data_existence1], ignore_index=True)
            data_existence = data_existence.rename(columns={data_existence.columns[0]: 'record_id'})
    elif timepoint == 'orca_8month_arm_1':
        visit_notes = get_orca_data(token, form = "visit_notes_8m", form_complete=False, timepoint=timepoint)

        if record_id != None:
            visit_notes = visit_notes[visit_notes['record_id'] == record_id]
            visit_notes.reset_index(drop=True, inplace=True)
            data_existence = visit_notes.loc[:,'richards_ecg_cg_data_8m':'fp_video_data_8m']
        else:
            record_ids = visit_notes['record_id']
            data_existence1 = visit_notes.loc[:,'richards_ecg_cg_data_8m':'fp_video_data_8m']
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
def get_task_completion(token, record_id = None, transposed = False, timepoint='orca_4month_arm_1'):
    """
    Retrieve task data existence status for a particular ID/all ids.

    Args:
        token (str): The API token for the project.
        record_id (str): the record id you wish to pull (e.g. '218'). Default is 'none' and will pull the whole dataset
        transposed: Whether you want it in long format (for just one id) - default is False. Can only mark as True if you also specify a record id
        timepoint (str): the redcap event name of the timepoint you wish to pull. Default is orca_4month_arm_1
    Returns:
        pandas.DataFrame: A DataFrame with the retrieved record id, task, completion status and reason why it's not fully complete.
    """
    import requests
    import pandas as pd
    import io

    if timepoint == 'orca_4month_arm_1':
        visit_notes = get_orca_data(token, form = "visit_notes_4m", form_complete=False, timepoint=timepoint)

        if record_id != None:
            visit_notes = visit_notes[visit_notes['record_id'] == record_id]
            visit_notes.reset_index(drop=True, inplace=True)
            task_comp = visit_notes[['richards_comp_4m', 'vpc_comp_4m', 'srt_comp_4m', 'cecile_comp_4m', 'relational_memory_comp_4m', 'freeplay_comp_4m']]
            whys = visit_notes[['richards_why_4m', 'vpc_why_4m', 'srt_why_4m', 'cecile_why_4m', 'relational_memory_why_4m', 'freeplay_why_4m']]
        else:
            task_comp = visit_notes[['record_id','richards_comp_4m', 'richards_why_4m','vpc_comp_4m','vpc_why_4m', 'srt_comp_4m', 'srt_why_4m','cecile_comp_4m', 'cecile_why_4m','relational_memory_comp_4m', 'relational_memory_why_4m','freeplay_comp_4m', 'freeplay_why_4m']]
    elif timepoint == 'orca_8month_arm_1':
        visit_notes = get_orca_data(token, form = "visit_notes_8m", form_complete=False, timepoint=timepoint)

        if record_id != None:
            visit_notes = visit_notes[visit_notes['record_id'] == record_id]
            visit_notes.reset_index(drop=True, inplace=True)
            task_comp = visit_notes[['richards_comp_8m', 'vpc_comp_8m', 'srt_comp_8m', 'pa_comp_8m','relational_memory_comp_8m', 'cecile_comp_8m', 'freeplay_comp_8m']]
            whys = visit_notes[['richards_why_8m', 'vpc_why_8m', 'srt_why_8m', 'pa_why_8m','relational_memory_why_8m', 'cecile_why_8m', 'freeplay_why_8m']]
        else:
            task_comp = visit_notes[['record_id','richards_comp_8m', 'richards_why_8m','vpc_comp_8m','vpc_why_8m', 'srt_comp_8m', 'srt_why_8m','pa_comp_8m', 'pa_why_8m','relational_memory_comp_8m', 'relational_memory_why_8m','cecile_comp_8m', 'cecile_why_8m','freeplay_comp_8m', 'freeplay_why_8m']]

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
def get_task_info(token, record_id = None, transposed = False, timepoint='orca_4month_arm_1'):
    """
    Retrieves 3 data frames - task timestamps, completion status, and data presence for all ids or a particular id.
    Returns in order of: task_completion, task_data, task_timestamps

    Args:
        token (str): The API token for the project.
        record_id (str): the record id you wish to pull (e.g. '218'). Default is 'none' and will pull the whole dataset
        transposed: Whether you want it in long format (for just one id) - default is False. Can only mark as True if you also specify a record id
        timepoint (str): the redcap event name of the timepoint you wish to pull. Default is orca_4month_arm_1

    Returns:
        pandas.DataFrame: 3 DataFrames (runs get_task_completion, get_task_timestamps,get_task_data).

    """
    import requests
    import pandas as pd
    import io

    task_completion = get_task_completion(token, record_id, transposed, timepoint)
    task_timestamps = get_task_timestamps(token, record_id, transposed, timepoint)
    task_data = get_task_data(token, record_id, transposed, timepoint)

    return task_completion, task_data, task_timestamps
#-----------------------

#8-----------------------
def get_movesense_numbers(token, record_id = None, timepoint = 'orca_4month_arm_1'):
    """
    Pulls child and caregiver movesense device numbers for a given timepoint.

    Args:
        token (str): The API token for the project.
        record_id (str): the record id you wish to pull (e.g. '218'). Default is 'none' and will pull the whole dataset
        timepoint (str): the redcap event name of the timepoint you wish to pull. Default is orca_4month_arm_1

    Returns:
        pandas.DataFrame: A DataFrame with record id, child device number and caregiver device number. If a single record id is specified, will return cg device number first, then child

    """
    import requests
    import pandas as pd
    import io
    import numpy as np

    form_name = "visit_notes_" + timepoint[5:7]
    visit_notes = get_orca_data(token, form=form_name, form_complete=False, timepoint=timepoint)
    child_column_name = [col for col in visit_notes.columns if 'hr_device_child' in col][0]
    parent_column_name = [col for col in visit_notes.columns if 'hr_device_cg' in col][0]

    if record_id != None:
        visit_notes = visit_notes[visit_notes['record_id'] == record_id]
        visit_notes.reset_index(drop=True, inplace=True)
        child_number = str(int(visit_notes[child_column_name])) if not visit_notes[child_column_name].empty else np.nan
        parent_number = str(int(visit_notes[parent_column_name])) if not visit_notes[parent_column_name].empty else np.nan
        return parent_number, child_number
    else:
        visit_notes = visit_notes[['record_id', parent_column_name, child_column_name]]
        visit_notes[parent_column_name] = visit_notes[parent_column_name].astype('Int64')
        visit_notes[child_column_name] = visit_notes[child_column_name].astype('Int64')
        return visit_notes

#-----------------------

#9-----------------------
record_id = '209'
timepoint = 'orca_4month_arm_1'
def check_timestamps(token, record_id, timepoint='orca_4month_arm_1'):
    """
    Pulls task info, and checks to see if there's any incorrectly missing timestamps.

    Args:
        token (str): The API token for the project.
        record_id (str): the record id you wish to pull (e.g. '218')
        timepoint (str): the redcap event name of the timepoint you wish to pull. Default is orca_4month_arm_1

    Returns:
        pandas.DataFrame: A character vector with the tasks that have incorrect/missing timestamps

    """
    import requests
    import pandas as pd
    import io
    comp, data, timestamps = get_task_info(token, record_id=record_id, transposed=True, timepoint=timepoint)

    missing_timestamps = []
    #richards
    if data.iloc[0,2] < 4 or data.iloc[1,2] < 4:
        if pd.isna(timestamps.iloc[0, 2]) or pd.isna(timestamps.iloc[1, 2]):
            missing_timestamps.append('richards')
    #vpc
    if data.iloc[3,2] < 4 or data.iloc[4,2] < 4:
        if pd.isna(timestamps.iloc[2, 2]) or pd.isna(timestamps.iloc[3, 2]):
            missing_timestamps.append('vpc')
    #srt
    if data.iloc[6,2] < 4 or data.iloc[7,2] < 4:
        if pd.isna(timestamps.iloc[4, 2]) or pd.isna(timestamps.iloc[5, 2]):
            missing_timestamps.append('srt')
    #cecile
    if data.iloc[9,2] < 4 or data.iloc[10,2] < 4:
        if pd.isna(timestamps.iloc[6, 2]) or pd.isna(timestamps.iloc[7, 2]):
            missing_timestamps.append('cecile')
    #relational memory
    if data.iloc[12,2] < 4 or data.iloc[13,2] < 4:
        if pd.isna(timestamps.iloc[8, 2]) or pd.isna(timestamps.iloc[9, 2]):
            missing_timestamps.append('relational_memory')
    
    #freeplay
    visit_notes = get_orca_data(token, form = "visit_notes_4m", form_complete=False)
    visit_notes = visit_notes[visit_notes['record_id'] == record_id]
    visit_notes.reset_index(drop=True, inplace=True)
    no_toy = visit_notes['freeplay_conditions_4m___1'].iloc[0]
    toy = visit_notes['freeplay_conditions_4m___2'].iloc[0]

    #freeplay
    if data.iloc[15,2] < 4 or data.iloc[16,2] < 4:
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
def get_movesense_times(token, record_id, who, timepoint='orca_4month_arm_1'):
    """
    Pulls test recording times for each movesense device in order parent, child.

    Args:
        token (str): The API token for the project.
        record_id (str): the record id you wish to pull (e.g. '218')
        who (str): 'cg' or 'child' 
        timepoint (str): the redcap event name of the timepoint you wish to pull. Default is orca_4month_arm_1

    Returns:
        a character vector with the on time followed by off time

    """
    import requests
    import pandas as pd
    import io
    from datetime import datetime
    import numpy as np
    import pytz

    form_name = "visit_notes_" + timepoint[5:7]
    visit_notes = get_orca_data(token, form=form_name, form_complete=False, timepoint=timepoint)
    child_on = [col for col in visit_notes.columns if 'child_movesense_on' in col][0]
    child_off = [col for col in visit_notes.columns if 'child_movesense_off' in col][0]
    parent_on = [col for col in visit_notes.columns if 'cg_movesense_on' in col][0]
    parent_off = [col for col in visit_notes.columns if 'cg_movesense_off' in col][0]

    movesense_times = visit_notes[['record_id', parent_on,child_on, parent_off, child_off]]
    movesense_times = movesense_times[movesense_times['record_id'] == record_id]

    date = get_orca_field(token, field = "visit_date_"+timepoint[5:7])
    date = date[date['record_id'] == record_id]
    date = str(date["visit_date_"+timepoint[5:7]])
    date = date.split()[1]

    if who == 'cg':
        on_time = str(movesense_times[parent_on]).split()[1]
        off_time = str(movesense_times[parent_off]).split()[1]
        if on_time != 'NaN' and off_time != 'NaN':
            on_time = pd.to_datetime(date+ ' ' + on_time)
            off_time = pd.to_datetime(date+ ' ' + off_time)
        elif on_time != 'NaN' and off_time == 'NaN':
            on_time = pd.to_datetime(date+ ' ' + on_time)
        elif on_time == 'NaN' and off_time != 'NaN':
            off_time = pd.to_datetime(date+ ' ' + off_time)
        
    elif who == 'child':
        on_time = str(movesense_times[child_on]).split()[1]
        off_time = str(movesense_times[child_off]).split()[1]
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
def get_visit_datetime(token, record_id = None, merged = True, timepoint = 'orca_4month_arm_1'):
    """
    Pulls the visit start date time for all ids / a specified ID

    Args:
        token (str): The API token for the project.
        record_id (str): record_id you want to specify. Default is None and will pull all IDs
        merged (bool): Whether to return the merged datetime or date and time as separate columns. Default is True
        timepoint (str): the redcap event name of the timepoint you wish to pull. Default is orca_4month_arm_1

    Returns:
        pandas.DataFrame: if record_id = None or merged = False containing record_id, date, time, datetime
        datetime value: a tz-conscious datetime variable representing the start of the visit, if record_id != None and merged = True
    """
    import requests
    import pandas as pd
    import io
    from datetime import datetime
    import pytz

    if timepoint == 'orca_4month_arm_1':
        visit_notes = get_orca_data(token, form = "visit_notes_4m", form_complete=False, timepoint=timepoint)
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
        
    elif timepoint == 'orca_8month_arm_1':
        visit_notes = get_orca_data(token, form = "visit_notes_8m", form_complete=False, timepoint=timepoint)
        data = visit_notes[['record_id', 'visit_date_8m', 'visit_time_8m']]
        data = data[data['visit_date_8m'].notna() | data['visit_time_8m'].notna()]

        data['visit_date_8m'] = pd.to_datetime(data['visit_date_8m'])
        data['visit_time_8m'] = pd.to_datetime(data['visit_time_8m'], format='%H:%M').dt.time

        if record_id == None and merged == False:
            return data
        elif record_id == None and merged == True:
            data['visit_datetime_8m'] = data['visit_date_8m'] + pd.to_timedelta(data['visit_time_8m'].astype(str))
            data['visit_datetime_8m'] = data['visit_datetime_8m'].dt.tz_localize('America/New_York')
            return data
        elif record_id != None and merged == False:
            data = data[data['record_id'] == record_id]
            return data
        elif record_id != None and merged == True:
            data = data[data['record_id'] == record_id]
            data['visit_datetime_8m'] = data['visit_date_8m'] + pd.to_timedelta(data['visit_time_8m'].astype(str))
            data['visit_datetime_8m'] = data['visit_datetime_8m'].dt.tz_localize('America/New_York')
            value = data['visit_datetime_8m'].iloc[0]
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

        for id in data['record_id']:
            filtered = data[data['record_id'] == id]
            filtered = filtered.dropna(axis=1)

        # Convert DataFrame to a list of dictionaries (one dictionary per row)
            records = filtered.to_dict(orient='records')

            try:
                import_status = project.import_records(records)
                print("Data import completed for ", import_status['count'], " record(s)")
            except RedcapError as e:
                print(f"Error importing data: {e}")
    else:
        print('\n','Data import terminated')
#-----------------------



#ORCA ECG Processing Functions
#all these functions are used for processing ECG data from ORCA
#e.g. timestamp calculation, segmenting, etc. 



#1-----------------------
def find_closest_timestamp(timestamp, timestamps, type = 'numeric'):
    """
    Takes a single timestamp and finds its closest match within a time series column/vector

    Args:
        timestamp (datetime): The single timestamp you wish to match
        timestamps (datetime): The time series you wish to find the match within. Must be the same format as timestamp
        method (str): numeric time values (e.g. seconds, milliseconds) or datetime

    Returns:
        A single value from timestamps which is the closest match to timestamp
    """
    if type == 'numeric':
        closest_timestamp = min(timestamps, key=lambda x: abs(timestamp - x))
    elif type == 'datetime':
        closest_timestamp = min(timestamps, key=lambda x: abs((timestamp - x).total_seconds()))

    return closest_timestamp
#-----------------------

#2-----------------------
def segment_full_ecg(ecg_file, marker_column, start_marker, end_marker):
    import pandas as pd
    from datetime import datetime
    pd.options.mode.chained_assignment = None  # default='warn'
    """
    Segments a file based on markers within a particular column

    Args:
        ecg_file (pandas.DataFrame): the name of a pandas dataframe in your environment to segment
        marker_column (str): the name of the column in ecg_file containing your markers. Must be a string
        start_marker (str): the marker of the row you wish to segment after. Must be a string
        end_marker (str): the marker of the row you wish to segment until. Must be a string

    Returns:
        pandas.DataFrame: DF containing all rows between your two markers
    """
    segmented_signals = None
    if marker_column in ecg_file.columns:
        if start_marker in ecg_file[marker_column].values and end_marker in ecg_file[marker_column].values:
            first_index = ecg_file.index[ecg_file[marker_column] == start_marker][0]
            last_index = ecg_file.index[ecg_file[marker_column] == end_marker][0]
            segmented_signals = ecg_file.iloc[first_index:last_index+1]
        elif start_marker not in ecg_file[marker_column].values and end_marker in ecg_file[marker_column].values:
            print('cannot segment file: ' + start_marker + ' not present in file')
        elif start_marker in ecg_file[marker_column].values and end_marker not in ecg_file[marker_column].values:
            print('cannot segment file: ' + end_marker + ' not present in file')
        else:
            print('cannot segment file: neither marker present in file')
    else:
        print('cannot segment file: ' + marker_column + " is not present in the file")
    

    if segmented_signals is not None:
        segmented_signals['timestamp_relative'] = (segmented_signals['timestamp_est_corrected'] - segmented_signals['timestamp_est_corrected'].min()).dt.total_seconds()
        return segmented_signals
    else:
        empty_data = pd.DataFrame(columns=[])
        return empty_data

#-----------------------

#3-----------------------
def check_ecg_recording_n(ecg_data, column_name = 'timestamp_est'):
    """
    Checks number of ecg recordings within a file

    Args:
        ecg_file (pandas.DataFrame): 
        column_name (str): column name of the timestamp column you wish to check

    Returns:
        int: integer reflecting the number of recordings in the file
    """
    import pandas as pd

    ecg_data['time_diff'] = ecg_data[column_name].diff()
    max_gap = pd.Timedelta(seconds=1)
    ecg_data['new_recording'] = ecg_data['time_diff'] > max_gap
    ecg_data['recording_id'] = (ecg_data['new_recording'].cumsum() + 1).astype(int)
    ecg_data.drop(columns=['new_recording', 'time_diff'], inplace=True)
    unique_recording_ids = ecg_data['recording_id'].nunique()
    
    return unique_recording_ids
#-----------------------

#4-----------------------
def checking_multiple_recordings(ecg_data, column_name = 'recording_id'):
    """
    For ecg files with multiple recordings, it will print out the start time of each recording and ask the user to choose which one is the visit recording. It will filter the dataframe based on that.

    Args:
        ecg_file (pandas.DataFrame): 
        column_name (str): column name of the recording id column

    Returns:
        pandas.DataFrame: Your ecg_data filtered by user_response
    """
    import pandas as pd
    unique_recording_ids_n = ecg_data['recording_id'].nunique()
    unique_recording_ids = ecg_data['recording_id'].unique()
    if unique_recording_ids_n > 1:
        for recording_id in unique_recording_ids:
            first_row = ecg_data.loc[ecg_data['recording_id'] == recording_id].iloc[0]
            timestamp_value = first_row['timestamp_est_uncorrected']
            test = ecg_data[ecg_data['recording_id'] == int(recording_id)]
            duration = max(test['timestamp_est_uncorrected']) - min(test['timestamp_est_uncorrected'])
            print("Start time for recording " + str(recording_id) + ": " + str(timestamp_value) + "; Duration: " + str(duration))
        print("\n")
        user_response = input("Enter correct recording number here as an integer. If you do not know, enter '0' and then go away and check: ")   

        if user_response == '0':
            print("\n")
            print('dataset left unfiltered. Go and check the recording and then come back and filter')
        else:
            ecg_data = ecg_data[ecg_data['recording_id'] == int(user_response)]

    else:
        print('only one recording present in the dataset')
    
    return ecg_data
#-----------------------

#5-----------------------
def calculate_ecg_timestamps(ecg_data, start_time, end_time, sample_rate=256):
    """
    Calculates timestamps of a time series ecg dataframe according to either the start time or end time, and sampling rate

    Args:
        ecg_data (pandas.DataFrame): 
        start_time (datetime.datetime, optional): Datetime object of the start time of the ecg recording
        end_time (datetime.datetime, optional): Datetime object of the end time of the ecg recording
        sample_rate (int): Sampling rate of your ecg recording. Default is 256

    Returns:
        pandas.DataFrame: Your original ecg dataframe with a column 'timestamp_est_corrected' reflecting the new timestamps
        timedelta object: Number or seconds different between the new end_time of timestamp_est_corrected and the end_time provided. Only returned if both start_time and end_time != None
    """
    from datetime import datetime, timedelta
    import pandas as pd

    #calculating number of samples in df, duration of file and subsequent time incrememnt per sample
    num_samples = len(ecg_data)
    duration = num_samples / sample_rate
    time_increment_per_sample = duration / num_samples

    #Calculating timestamps based on num_samples, sample_rate and start time
    if pd.notna(start_time):
        timestamps = [start_time + i * timedelta(seconds=time_increment_per_sample) for i in range(num_samples)]
        ecg_data['timestamp_est_corrected'] = timestamps
    #if there is no start time, only end time, end time will be used and timestamp calculated backwards
    elif pd.isna(start_time) and pd.notna(end_time):
        timestamps = [end_time - (num_samples - i) * timedelta(seconds=time_increment_per_sample) for i in range(num_samples)]
        ecg_data['timestamp_est_corrected'] = timestamps
    
    #if both start and end time present, the new end time is compared to expected end time and 'margin of error' calculated
    if pd.notna(start_time) and pd.notna(end_time):
        new_max = max(ecg_data['timestamp_est_corrected'])
        margin_of_error = abs(end_time-new_max)
        #setting threshold for MOE check
        threshold = timedelta(seconds = 1)
        #if MOE is less than 1s, ecg data & moe is returned. If it is more, they are returned with warning to check the file
        if margin_of_error < threshold:
            print('Successfully corrected timestamps for this file')
            return ecg_data, margin_of_error
        else:
            print('There is more than a 1 second difference between the last sample and expected last sample. Check!')
            return ecg_data, margin_of_error
    else:
        margin_of_error = None
        print('No margin of error can be returned as only start time or end time was provided')
        return ecg_data, margin_of_error
#-----------------------

#6-----------------------
def calculate_ecg_drift(ecg_data, incorrect_times = 'timestamp_est_uncorrected', correct_times = 'timestamp_est_corrected'):
    """
    Calculates drift within a dataframe between 2 timeseries columns

    Args:
        ecg_data (pandas.DataFrame): dataframe with timeseries data
        incorrect_times (datetime.datetime): Datetime series 1
        correct_times (datetime.datetime): Datetime series 2

    Returns:
        drift_at_start (timedelta): time difference between the first samples of each time column
        drift_at_end (timedelta): time difference between the last samples of each time column
        drift_change (timedelta): change in drift between start and end of file
    """
    incorrect_start = min(ecg_data[incorrect_times])
    correct_start = min(ecg_data[correct_times])
    drift_at_start = abs(incorrect_start - correct_start)

    incorrect_end = max(ecg_data[incorrect_times])
    correct_end = max(ecg_data[correct_times])
    drift_at_end = abs(incorrect_end - correct_end)

    drift_change = abs(drift_at_start - drift_at_end)

    return drift_at_start, drift_at_end, drift_change
#-----------------------

#7-----------------------
def extract_ibi(file,method='interpolated'):
    """
    Extracts ibi (ms) and time (ms and s) from a matlab file 

    Args:
        file (str): A file path of your matlab file
        method(str): 'interpolated' or 'raw',whether you want to pull raw data or interpolated. raw has noise REMOVED and interpolated has noise interpolated

    Returns:
        data (pandas.DataFrame): data frame with time_s and time_ms of beat times, ibi in ms, and differenced IBIs 
    """
    import h5py
    import pandas as pd
    import numpy as np

    #pulling interpolated data
    if method == 'interpolated':
        with h5py.File(file, 'r') as hdf_file:
        # Read the datasets
            time_s = hdf_file['/Res/HRV/Data/T_RRi'][:]
            ibi_ms = hdf_file['/Res/HRV/Data/RRi'][:]*1000  # Convert to milliseconds
            dt = hdf_file['/Res/HRV/Data/RRdti'][:]

        # Create a DataFrame
        data = pd.DataFrame({
            'time_s': time_s[0],
            'time_ms': time_s[0] * 1000,  # Create time_ms directly
            'ibi_ms': ibi_ms[0],
            'dt': dt[0]
        })
        return data
    elif method == 'raw':
        with h5py.File(file, 'r') as hdf_file:
        # Read the datasets
            time_s = hdf_file['/Res/HRV/Data/T_RR'][:]
            ibi_ms = hdf_file['/Res/HRV/Data/RR'][:]  # Convert to milliseconds
            dt = hdf_file['/Res/HRV/Data/RRdt'][:]

        # Create a DataFrame
        data = pd.DataFrame({
            'time_s': time_s[0],
            'time_ms': time_s[0] * 1000,  # Create time_ms directly
            'ibi_ms': ibi_ms[0],
            'dt': dt[0]
        })

        return data
    else:
        print('please indicate whether you want to extract raw or interpolated IBI')

#-----------------------

#8-----------------------
def extract_task_ibi(token, task):
    """
    Batch processes IBI matlab files for a given task

    Args:
        task (str): The task you want to process: 'Richards', 'VPC', 'SRT', 'Cecile', 'Relational Memory', 'Freeplay'
        token (str): The API token for the project.
    Returns:
        temp_log (pandas.DataFrame): A logbook of each file processed and the mean / sd ibi values 
        task_import (pandas.DataFrame): The same info as temp_log but in wide format and with columns renamed for redcap import
    """
    import h5py
    import pandas as pd
    import os
    import numpy as np
    from datetime import date as dt
    #pulling task matlab files
    task_matlab_path = os.path.join("/Volumes/ISLAND/Projects/ORCA/ORCA 2.0/Data/4 Months/Heart Rate Data", task, "Beat Corrected Matlab Files")
    files = [file for file in os.listdir(task_matlab_path) if 'processed' not in file and '.DS_Store' not in file and '.mat' in file]
    
    print("\n", "The following ", task, " files will be processed:", "\n", "\n", files)
    response = input("\n"+'Continue to process (y/n):')
    temp_log = pd.DataFrame()
    task_import_cg = pd.DataFrame()
    task_import_child = pd.DataFrame()

    if response == 'y' and len(files) >= 1:
        for file in files:
            #File Info
            who = 'child' if 'child' in file else 'cg'
            id = file.split("_")[0]
            data = extract_ibi(os.path.join(task_matlab_path, file), method='interpolated')
            
            #finding ecg markers
            ecg_path = os.path.join("/Volumes/ISLAND/Projects/ORCA/ORCA 2.0/Data/4 Months/Heart Rate Data", task, "Raw ECG Data")
            ecg_file = [ecg_file for ecg_file in os.listdir(ecg_path) if id in ecg_file and "_"+who in ecg_file][0]
            ecg_data = pd.read_csv(os.path.join(ecg_path, ecg_file))

            ecg_data = (ecg_data
                        .loc[:, ['marker', 'timestamp_relative']]
                        .dropna(subset=['marker'])
                        .reset_index(drop=True))

            closest_timestamps = []
            for index in ecg_data.index:
                closest_timestamp = find_closest_timestamp(ecg_data.iloc[index, 1], data['time_s'], type='numeric')
                closest_timestamps.append(closest_timestamp)

            ecg_data['time_s'] = closest_timestamps
            ecg_data = ecg_data[['time_s', 'marker']]
            data = pd.merge(data,ecg_data, on='time_s', how='left')

            #adding continuous conditions column to the ibi_file
            markers = data[pd.notna(data['marker'])].reset_index(drop=True)
            markers = markers[['time_s', 'marker']]
            markers['condition'] = markers['marker'].str.split('_').str[0]

            condition_list = []

            for i, time in enumerate(data['time_s']):
                for i2, marker_time in enumerate(markers['time_s']):
                    if i2 != (markers.index.stop-1):
                        if time >= marker_time and time < markers['time_s'][i2+1]:
                            condition = markers['condition'][i2]
                            break
                        else:
                            condition = np.nan
                    else:
                        if time >= marker_time:
                            condition = markers['condition'][i2]
                            break
                        else:
                            condition = np.nan
                
                condition_list.append(condition)


            data['condition'] = condition_list

            #saving IBI csv
            ibi_path = os.path.join("/Volumes/ISLAND/Projects/ORCA/ORCA 2.0/Data/4 Months/Heart Rate Data", task, "IBI Files")
            ibi_file_name = id+"_4m_" + who + "_ibi_" + task.lower() + ".csv" 
            data.to_csv(os.path.join(ibi_path, ibi_file_name), index=False)

            #renaming original matlab file
            old_matlab = os.path.join(task_matlab_path, file)
            new_matlab = os.path.join(task_matlab_path, id+"_4m_"+who+"_ecg_"+task.lower()+"_hrv_processed.mat")
            os.rename(old_matlab, new_matlab)
            print('extracted ibi and saved csv for ', file)

            #Calculating Descriptives

            if task.lower() != 'freeplay':
                temp_data = pd.DataFrame([{
                    'record_id': id,
                    'who': who,
                    'date': dt.today(),
                    'ibi_mean': np.nanmean(data['ibi_ms']),
                    'ibi_sd': np.nanstd(data['ibi_ms']),
                    'max_ibi': np.nanmax(data['ibi_ms']),
                    'min_ibi': np.nanmin(data['ibi_ms'])
                }])

                temp_log = temp_data if temp_log.empty else temp_log.merge(temp_data, how='outer')

                #creating import file
                import_file = temp_data[['record_id', 'date', 'ibi_mean', 'ibi_sd']]
                import_file = import_file.copy()
                import_file.rename(columns={
                    'date': who + "_" + task.lower() + "_ibi_date_4m", 
                    'ibi_mean': who + "_" + task.lower() + "_ibi_m_4m", 
                    'ibi_sd': who + "_" + task.lower() + "_ibi_sd_4m"}, inplace=True)
                import_file['redcap_event_name'] = 'orca_4month_arm_1'

                import_file[who + "_" + task.lower() + "_ibi_date_4m"] =import_file[who + "_" + task.lower() + "_ibi_date_4m"].astype(str)

                if who == 'cg':
                    task_import_cg = import_file if task_import_cg.empty else task_import_cg.merge(import_file, how = 'outer')
                elif who == 'child':
                    task_import_child = import_file if task_import_child.empty else task_import_child.merge(import_file, how = 'outer')

            elif task.lower() == 'freeplay':
                nt_data = data[data['condition'] == 'notoy']
                t_data = data[data['condition'] == 'toy']

                temp_data = pd.DataFrame([{
                    'record_id': id,
                    'who': who,
                    'date': dt.today(),
                    'ibi_mean': np.nanmean(data['ibi_ms']),
                    'ibi_sd': np.nanstd(data['ibi_ms']),
                    'max_ibi': np.nanmax(data['ibi_ms']),
                    'min_ibi': np.nanmin(data['ibi_ms']),
                    'ibi_mean_nt': np.nanmean(nt_data['ibi_ms']) if not nt_data.empty else np.nan,
                    'ibi_sd_nt': np.nanstd(nt_data['ibi_ms']) if not nt_data.empty else np.nan,
                    'max_ibi_nt': np.nanmax(nt_data['ibi_ms']) if not nt_data.empty else np.nan,
                    'min_ibi_nt': np.nanmin(nt_data['ibi_ms']) if not nt_data.empty else np.nan,
                    'ibi_mean_t': np.nanmean(t_data['ibi_ms']) if not t_data.empty else np.nan,
                    'ibi_sd_t': np.nanstd(t_data['ibi_ms']) if not t_data.empty else np.nan,
                    'max_ibi_t': np.nanmax(t_data['ibi_ms']) if not t_data.empty else np.nan,
                    'min_ibi_t': np.nanmin(t_data['ibi_ms']) if not t_data.empty else np.nan,
                }])

                temp_log = temp_data if temp_log.empty else temp_log.merge(temp_data, how='outer')

                #creating import file
                import_file = temp_data[['record_id', 'date', 'ibi_mean', 'ibi_sd', 'ibi_mean_nt', 'ibi_sd_nt', 'ibi_mean_t', 'ibi_sd_t']]
                import_file = import_file.copy()
                import_file.rename(columns={
                    'date': who + "_" + task.lower() + "_ibi_date_4m", 
                    'ibi_mean': who + "_" + task.lower() + "_ibi_m_4m", 
                    'ibi_sd': who + "_" + task.lower() + "_ibi_sd_4m",  
                    'ibi_mean_nt': who + "_notoy_ibi_m_4m", 
                    'ibi_sd_nt': who +"_notoy_ibi_sd_4m", 
                    'ibi_mean_t': who + "_toy_ibi_m_4m", 
                    'ibi_sd_t': who +"_toy_ibi_sd_4m"}, 
                    inplace=True)
                
                import_file['redcap_event_name'] = 'orca_4month_arm_1'
                import_file[who + "_" + task.lower() + "_ibi_date_4m"] =import_file[who + "_" + task.lower() + "_ibi_date_4m"].astype(str)

                if who == 'cg':
                    task_import_cg = import_file if task_import_cg.empty else task_import_cg.merge(import_file, how = 'outer')
                elif who == 'child':
                    task_import_child = import_file if task_import_child.empty else task_import_child.merge(import_file, how = 'outer')
                
        task_import = task_import_cg.merge(task_import_child, on=['record_id', 'redcap_event_name'], how="outer")
        return temp_log, task_import
    else:
        print('batch ibi extraction terminated')
        return None, None
#-----------------------

#8-----------------------
def create_epochs(data, size=3, time_column = 'time_s', value_column='ibi_ms', conditions=True, condition_column='condition'):
    """
    Creates time epochs for time series data

    Args:
        data (pandas.DataFrame): The time series data you wish to epoch
        size (int): The size of your epochs in seconds (default is 3)
        time_column (str): the name of the column containing your time values (in seconds)
        value_column (str): the name of the column containing your value (e.g. ibi_ms)
        conditions (boolean): Whether your data contains a continuous conditions column you wish to keep in your returned df. default is True
        condition_column: The name of the column containing your conditions
    Returns:
        epoched_data (pandas.DataFrame): Df containing epoch_num, the epoch_s (max of range), mean value and condition (if condition = True)
    """
    import pandas as pd

    data['timedelta'] = pd.to_timedelta(data[time_column], unit = 's')
    data['epoch_range'] = pd.cut(data[time_column], bins=pd.interval_range(start=0, end=data[time_column].max()+3, freq=3, closed='right'), include_lowest=False, labels=False)
    data['epoch_s'] = data['epoch_range'].apply(lambda x: x.right)
    epoched_data = data.groupby('epoch_s', observed=False)[value_column].mean().reset_index()
    epoched_data['epoch_num'] = range(1, len(epoched_data) + 1)

    if conditions:
        epoch_conditions = data[['epoch_s', 'condition']].drop_duplicates(subset=['epoch_s'])
        epoched_data = pd.merge(epoched_data, epoch_conditions, how='left', on='epoch_s')
        epoched_data = epoched_data[['epoch_num', 'epoch_s', 'ibi_ms', 'condition']]
    else:
        epoched_data = epoched_data[['epoch_num', 'epoch_s', 'ibi_ms']]


    return(epoched_data)
#-----------------------

#OWLET SPECIFIC FUNCTIONS

#9-----------------------
def clean_video_times(file, id, visit_date, timepoint = 4):
    """
    Reads video times csv from OWLET, converts to eastern time and formats into redcap-compatible format for data import

    Args:
        file (str): The file path for video times csv
        id (str): the record if you are processing (e.g. '319')
        timepoint (int): the numeric value for the timepoint you are processing (default = 4)
    Returns:
        times_data (pandas.DataFrame): Df containing record_id, redcap_event_name, and timestamp for start and end of each video in timepoint
    """
    import os
    import pandas as pd
    import pytz
    from datetime import timedelta
    import numpy as np

    #reading csv
    times_csv = pd.read_csv(file)
    times_data = times_csv.melt(id_vars=['data:text/csv;charset=utf-8'], var_name='Video', value_name='Time')
    times_data['Video2'] = times_data['Video'] + "_" + times_data.iloc[:, 0]

    #renaming to redcap field names
    times_data['Video2'] = times_data['Video2'].str.replace('Video1', 'richards').str.replace('Video2', 'vpc').str.replace('Video3', 'srt').str.replace('Video4', 'cecile').str.replace('Video5', 'relational_memory')
    times_data['Video2'] = times_data['Video2'].str.replace('Start', 'start_4m').str.replace('End', 'end_4m')

    times_data['Time'] = pd.to_datetime(visit_date + ' ' + times_data['Time'], utc=True)
    times_data['Time'] = times_data['Time'].dt.tz_convert('America/New_York').dt.strftime('%H:%M:%S')
    times_data['Time']

    display(times_data)
    user_response = input("Check the times in the displayed dataset. Enter 'N' to terminate or 'Y' to continue: ") 

    if user_response.lower() == 'n':
        print("\n")
        print('Process aborted. Try again')
    elif user_response.lower() == 'y':
        times_data = times_data[['Video2', 'Time']]
        times_data = times_data.set_index('Video2').T

        times_data['record_id'] = id
        times_data['redcap_event_name'] = 'orca_4month_arm_1' if timepoint == 4 else 'orca_8month_arm_1'

        print("\n")
        print('Video times data prepared for redcap import. check before importing')

        return times_data
#-----------------------

#10-----------------------
def clean_survey_data(file, timepoint='4', study='orca'):
    """
    Reads survey data csv from OWLET, and formats into redcap-compatible format for data import

    Args:
        file (str): The file path for video times csv
        timepoint (str): the numeric value for the timepoint you are processing (default = 4)
        study (str): study code, default is orca. Change to 'mice_baseline' or 
    Returns:
        survey_data (pandas.DataFrame): Df containing record_id, redcap_event_name, and survey data fields
    """
        
    import os
    import pandas as pd
    import pytz
    from datetime import timedelta
    import numpy as np

    survey_data = pd.read_csv(file)

    if study == 'orca':
        if 'orca' not in survey_data['subject_id'][0]:
            survey_data['empty_column'] = ''
            survey_data = survey_data.shift(axis=1)
            survey_data.iloc[:, 0] = survey_data.index
            survey_data.reset_index(drop=True, inplace=True)

            survey_data['feedback'] = survey_data['feedback'] + survey_data['empty_column']

    survey_data.rename(columns={
    'subject_id': 'record_id',
    'feedback': 'owlet_feedback'
    }, inplace=True)

    if study == 'orca':
        survey_data['redcap_event_name'] = 'orca_' + timepoint + 'month_arm_1'
        survey_data = survey_data[['record_id', 'redcap_event_name', 'internet_connection','instructions_ease','website_ease','owlet_feedback']]
        survey_data['record_id'] = survey_data['record_id'].str.replace('orca_', '')
    elif study == 'mice_baseline':
        survey_data['redcap_event_name'] = 'mb_surveys_arm_3'
        survey_data = survey_data[['record_id', 'redcap_event_name', 'internet_connection','instructions_ease','website_ease','owlet_feedback']]
        survey_data['record_id'] = survey_data['record_id'].str.replace('vpc_', 'mb_')

    return survey_data
#-----------------------

#11-----------------------
def overlay_real_time(video_path,output_path, start_time):
    """
    Reads an mp4 file, and uses frame rate and start date time to overlay real time for each frame, and saves it to output path

    Args:
        video_path (str): The file path for mp4
        output_path (str): The file path to save the new mp4
        start_time (datetime): Real start datetime for the mp4. Must be in format '%Y-%m-%d %H:%M:%S.%f'. If it is in str, function will convert to datetime
    Returns:
        Success / error message: Success message if mp4 is successfully saved
    """
        
    import cv2
    import pandas as pd
    from datetime import datetime, timedelta

    #Step 1: Try to load video
    print('loading video...')
    cap = cv2.VideoCapture(video_path)

    if cap.isOpened():
        print('Video file successfully opened')
    elif not cap.isOpened():
        return "Error: could not open video file. Please check the video path"
    
    
    #Step 2: Calculate frame rate
    print('calculating frame rate...')
    fps = cap.get(cv2.CAP_PROP_FPS)

    if fps != 0:
        print('Frame rate calculated: ', fps)
    else:
        return('Error: could not calculate frame rate. Process terminated')

    #Step 3: Specify output and verify dimensions
    try:
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        out = cv2.VideoWriter(output_path, fourcc, fps, (width, height))
    except Exception as e:
        return f"Error: could not initialize the video writer. {e}"
    
    # Convert start_time if it's in string format
    if isinstance(start_time, str):
        try:
            start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            return "Error: Start time format should be 'YYYY-mm-dd HH:MM:SS.ms'."

    #Step 4: Process Frames
    print('processing frame timestamps')
    try:
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break
        
            #calculate the current timestamp based on frame position
            frame_index = int(cap.get(cv2.CAP_PROP_POS_FRAMES))
            current_time = device_on_start + timedelta(seconds=frame_index / fps)

            #format timestamp and overlay on video frame
            timestamp_str = current_time.strftime("%H:%M:%S.%f")[:-3]  # Shows to milliseconds
            cv2.putText(frame, timestamp_str, (10, 60), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2, cv2.LINE_AA)


            #write the frame
            out.write(frame)

    except Exception as e:
        return f"Error: an error occurred while processing frames. {e}"

    # Step 5: Release resources and return success message
    try:
        out.release()
        cap.release()
        cv2.destroyAllWindows()
    except Exception as e:
        return f"Error: Failed to release resources properly. {e}"
    
    return f"Video processing complete! Saved output to {output_path}"
#-----------------------

#12----------------------- #pulls PEACH ema data
def get_ema_data(token, am_or_pm = 'am'):
    """
    Pulls PEACH am or pm ema survey averages for each event and domain

    Args:
        token (str): The API token for the PEACH redcap project. 
        am_or_pm (str): whether you want to pull daytime or evening surveys. default is am

    Returns:
        pandas.DataFrame: A DataFrame with the retrieved data.
    """
    if am_or_pm == 'am':
        ema = get_orca_data(token, form='ema_am_survey')
        ema = ema[ema['record_id'].str.contains('pch')]
        ema = ema[['record_id', 'redcap_event_name', 'anxiety_am', 'attention_am', 'stress_am', 'depression_am', 'loneliness_am']]
    elif am_or_pm == 'pm':
        ema = get_orca_data(token, form='ema_pm_survey')
        ema = ema[ema['record_id'].str.contains('pch')]
        ema = ema[['record_id', 'redcap_event_name', 'anxiety_pm', 'attention_pm', 'stress_pm', 'depression_pm']]

    return ema
#-----------------------

#13-----------------------

#-----------------------

#14-----------------------

#-----------------------