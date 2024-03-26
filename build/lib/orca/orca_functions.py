#get_orca_data

url = 'https://redcap.nyu.edu/api/'

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

    visit_notes = get_orca_data(token, form = "visit_notes_4m", form_complete=False)
    if record_id != None:
        visit_notes = visit_notes[visit_notes['record_id'] == record_id]
        visit_notes.reset_index(drop=True, inplace=True)
        markers = visit_notes[['richards_start_4m', 'richards_end_4m', 'vpc_start_4m', 'vpc_end_4m','srt_start_4m', 'srt_end_4m', 'cecile_start_4m', 'cecile_end_4m','relational_memory_start_4m', 'relational_memory_end_4m', 'notoy_start_real_4m','notoy_end_real_4m', 'toy_start_real_4m', 'toy_end_real_4m']]
    else:
        markers = visit_notes[['record_id', 'richards_start_4m', 'richards_end_4m', 'vpc_start_4m', 'vpc_end_4m','srt_start_4m', 'srt_end_4m', 'cecile_start_4m', 'cecile_end_4m','relational_memory_start_4m', 'relational_memory_end_4m', 'notoy_start_real_4m','notoy_end_real_4m', 'toy_start_real_4m', 'toy_end_real_4m']]

    if transposed == True and record_id != None:
        markers = markers.transpose()
        markers = markers.rename_axis('marker').reset_index()
        markers.columns = ['marker', 'timestamp_est']
        markers['record_id'] = record_id
        markers = markers[['record_id', 'marker', 'timestamp_est']]
    elif transposed == True and record_id == None:
        print('cannot transpose without selecting a record id')

    return markers


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


def get_task_info(token, record_id = None, transposed = False):
    """
    Retrieves 3 data frames - task timestamps, completion status, and data presence for all ids or a particular id.
    Returns in order of: task_completion, task_timestamps, task_data

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
        visit_notes = visit_notes[visit_notes['record_id'] == id]
        visit_notes.reset_index(drop=True, inplace=True)
        child_number = str(int(visit_notes['hr_device_child_4m']))
        parent_number = str(int(visit_notes['hr_device_cg_4m']))
        return parent_number, child_number
    else:
        visit_notes = visit_notes[['record_id', 'hr_device_cg_4m', 'hr_device_child_4m']]
        visit_notes['hr_device_cg_4m'] = visit_notes['hr_device_cg_4m'].astype('Int64')
        visit_notes['hr_device_child_4m'] = visit_notes['hr_device_child_4m'].astype('Int64')
        return visit_notes

    



