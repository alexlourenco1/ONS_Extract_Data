import requests as rq
import json
import os
from datetime import datetime as dt, date
from glob import glob
import shutil
import pandas as pd
from dateutil.relativedelta import relativedelta as delta, FR
from dateutil.rrule import rrule, DAILY
from calendar import monthrange


def ena_calculate(month: int, year: int, groupby: str = 'submarket', file: str = None, data: dict = None) -> pd.DataFrame:
    payload = {'month': str(month),
               'year': str(year),
               'groupby': groupby}
    files = [('file', open(file, 'rb'))]
    response = rq.request("POST", url=os.environ["ENA_CALCULATE_API"], params=payload, files=files, json=data)

    dict_response = dict(json.loads(response.text))
    df = pd.DataFrame(data=dict_response).transpose()

    return df


def prevs_to_ena(month: int, year: int, groupby: str = 'submarket', file: str = None, data: dict = None) -> pd.DataFrame:
    payload = {'month': str(month),
               'year': str(year),
               'groupby': groupby}
    files = [('file', open(file, 'rb'))]
    response = rq.request("POST", url=os.environ["ENA_CALCULATE_API"], params=payload, files=files, json=data)

    dict_response = dict(json.loads(response.text))
    df = pd.DataFrame(data=dict_response).transpose()

    return df


def next_friday(ref_date: date) -> date:
    output = ref_date + delta(weekday=FR)
    return output


def operative_week(ref_date: date) -> int:
    d = ref_date + delta(weekday=FR)
    d_begin = date(d.year, 1, 1) + delta(weekday=FR)
    output = (d - d_begin).days // 7 + 1
    return output


def operative_week_month(ref_date: date) -> int:
    d = ref_date + delta(weekday=FR)
    dt_start = d.replace(day=1)
    dt_end = date(d.year, d.month, monthrange(d.year, d.month)[1])
    fridays = [d.date() for d in rrule(freq=DAILY, dtstart=dt_start, until=dt_end) if d.weekday() == 4]
    output = fridays.index(d) + 1
    return output


def day_by_operative_week(week: int, year: int) -> dict:
    end = (date(year, 1, 1) + delta(weekday=FR)) + delta(days=7 * (week - 1))
    start = end - delta(days=6)
    output = {'data_fim': end, 'data_inicio': start}
    return output


def rvs_in_month(year, month) -> int:
    dt_start = date(year, month, 1)
    dt_end = date(year, month, monthrange(year, month)[1])
    fridays = [d.date() for d in rrule(freq=DAILY, dtstart=dt_start, until=dt_end) if d.weekday() == 4]
    output = len(fridays)
    return output


def nr_days_rvs(year, month) -> list:
    output = list()
    dt_start = date(year, month, 1)
    dt_end = dt_start + delta(weekday=FR)
    nr_days = (dt_end-dt_start).days + 1
    output.append({"daysInRev": nr_days, "rev": 0})
    rv = 1
    while True:
        dt_start = dt_end + delta(days=1)
        dt_end = dt_start + delta(weekday=FR)
        days = [d.date() for d in rrule(freq=DAILY, dtstart=dt_start, until=dt_end)
                if d.month == month and d.year == year]
        nr_days = len(days)
        if nr_days == 0:
            break
        output.append({"daysInRev": nr_days, "rev": rv})
        rv += 1
    return output


def last_friday(date):
    output = date + delta(weekday=FR) - delta(days=7)
    return output

def recursive_copy_files(source_path, destination_path, override=False):
    files_count = 0
    if not os.path.exists(destination_path):
        os.mkdir(destination_path)
    items = glob(source_path + '/*')
    for item in items:
        if os.path.isdir(item):
            path = os.path.join(destination_path, item.split(os.sep)[-1])
            files_count += recursive_copy_files(source_path=item, destination_path=path, override=override)
        else:
            file = os.path.join(destination_path, item.split(os.sep)[-1])
            if not os.path.exists(file) or override:
                shutil.copyfile(item, file)
                files_count += 1
    return files_count


def generate_file(name: str, file_extension: str, path: str, content: bytes = None, encoding: str = 'UTF-8', append: bool = False) -> str:

    # Building a full path to save. It can be a local path or GCS Path
    # full_path = '{0}/{1}.{2}'.format(path, name, file_extension)
    full_path = os.path.join(path, f'{name}.zip')

    try:
        # Verify if directory exist and than create the file
        if not os.path.exists(os.path.dirname(full_path)):
            os.makedirs(os.path.dirname(full_path))
        output = open(full_path, ('a' if append else 'wb'))

        # Verify if content is not empty
        if content is not None:

            # if not, verify if content is string type and if true
            # convert it to byte
            if type(content) is str:
                content = content.encode(encoding=encoding)

            output.write(content)

        output.close()

        return full_path

    except Exception as e:
        print('Error write file', full_path, '.\nError Description: ', str(e))
