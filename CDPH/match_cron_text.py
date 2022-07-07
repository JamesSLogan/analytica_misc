#!/usr/bin/env python3
# 
# Rough script to validate the cagov/Cron repo. Only looks in the current
# directory.
#
import os
import re

TIMING_FILE = 'function.json'
TEXT_FILE = 'index.js'

TZ_OFFSET = 7 # hours

SLACK_LINE = 'slackPostTS = ('

day_map = {
    'Sun': 'Sunday',
    'Mon': 'Monday',
    'Tue': 'Tuesday',
    'Wed': 'Wednesday',
    'Thu': 'Thurday',
    'Fri': 'Friday',
    'Sat': 'Saturday',
}

yellow = '\033[33m'
cyan = '\033[36m'
clear = '\033[0m'

# returns list of correlated files to be looked at
def get_pairs(search_dir):
    ret = []
    for root, _, files in os.walk(search_dir):
        if TIMING_FILE in files and TEXT_FILE in files:
            ret.append((os.path.join(root, TIMING_FILE), os.path.join(root, TEXT_FILE)))
    return ret


# Converts cron strings into time strings - input is not validated so errors can occur
# ex: ('14', '5')    -> 7:05am
# ex: ('14', '0')    -> 7am
# ex: ('14,15', '5') -> 7:05am,8:05am
# ex: ('14-15', '5') -> 7:05am,8:05am
# ex: ('14', '0/5')  -> 7am,7:05am,7;10am ...
def get_times(hours, minutes):
    # Get list of hours to work with
    if '-' in hours:
        hour_split = hours.split('-')
        hour_list = range(int(hour_split[0])-TZ_OFFSET, int(hour_split[1])-TZ_OFFSET+1)

    elif ',' in hours:
        hour_list = [int(h)-TZ_OFFSET for h in hours.split(',')]

    elif hours == '*':
        hour_list = range(0,24)

    else: # should just be one hour
        hour_list = [int(hours)-TZ_OFFSET]


    # Get list of minutes to work with
    if ',' in minutes:
        min_list = [int(m) for m in minutes.split(',')]

    elif '/' in minutes:
        min_start, min_inc = (int(m) for m in minutes.split('/'))
        min_list = range(min_start, 60, min_inc)

    else: # once during the hour
        min_list = [int(minutes)]


    # Generate text
    ret = []
    for hour in hour_list:
        meridiem = 'am'
        if hour > 12:
            hour -= 12
            meridiem = 'pm'

        for minute in min_list:
            extra = ''
            if minute < 10:
                extra = '0'
            ret.append(f'{hour}:{extra}{minute}{meridiem}')

    return ','.join(ret)


# Searches through file for hardcoded line.
# Input should be the location of a function.json file.
def get_expected_text(filename):
    with open(filename) as f:
        for line in f:
            if '"schedule"' in line:
                schedule_line = line
                break
        else:
            raise ValueError(f'file {filename} has no "schedule" line')

    # ex: "schedule": "0 5 14 * * Fri"
    schedule = schedule_line.split(':')[1].strip().strip('"')
    _, minutes, hours, _, _, days = schedule.split()

    if days == '*':
        text = f'Every day @ '
    elif ',' in days:
        text = f'Every {days} @ '
    else:
        text = f'Every {day_map[days]} @ '

    text += get_times(hours, minutes)

    return text


# Searches through file for hardcoded line.
# Input should be the location of an index.js file
def get_actual_text(filename):
    with open(filename) as f:
        for line in f:
            if SLACK_LINE in line:
                code_line = line
                break
        else:
            raise ValueError(f'file {filename} has no valid slack code')
    
    match = re.search(r'\((Every.*?)\)', code_line)
    if not match:
        raise ValueError(f'file {filename} has no valid slack text')
    return match.group(1)


def main(search_dir):
    file_pairs = get_pairs(search_dir)
    for timing_file, text_file in file_pairs:
        try:
            actual_text = get_actual_text(text_file)
        except ValueError:
            # skip file since there's nothing to do
            continue

        expected_text = get_expected_text(timing_file)
        if expected_text != actual_text:
            print(f'{cyan}{text_file}{clear}:')
            print(f'{actual_text}\n{yellow}should be{clear}\n{expected_text}\n')


if __name__ == '__main__':
    main('.')
