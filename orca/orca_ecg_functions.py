#ORCA ECG Processing Functions
#all these functions are used for processing ECG data from ORCA
#e.g. timestamp calculation, segmenting, etc. 

url = 'https://redcap.nyu.edu/api/'

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
    if type == 'relative':
        closest_timestamp = min(timestamps, key=lambda x: abs(timestamp - x))
    elif type == 'absolute':
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
        print('cannot return file as was unable to segment')
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