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
    print(r.text)

    df = pd.read_csv(io.StringIO(r.text))

    if form_complete:
        record_filter = f"{form}_complete"
        filtered_df = df[df[record_filter] == 2]
        return filtered_df
    else:
        return df