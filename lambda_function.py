import uuid
from datetime import datetime, timedelta, timezone
from itertools import groupby
import os

import boto3

import pymysql
from pytz import reference

JST = timezone(timedelta(hours=+9), 'JST')

ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')
AWS_REGION = os.environ.get('AWS_REGION_DEFAULT')

DATABASE_NAME = os.environ.get('DATABASE_NAME')
DATABASE_HOST = os.environ.get('DATABASE_HOST')
DATABASE_USER = os.environ.get('DATABASE_USER')
DATABASE_PASS = os.environ.get('DATABASE_PASS')


dynamo = boto3.resource('dynamodb', AWS_REGION, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
table = dynamo.Table('ORIENTATION_DATA_DEV')


WEEKDAY = {'Monday': '月', 'Tuesday': '火', 'Wednesday': '水', 'Thursday': '木', 'Friday': '金', 'Saturday': '土',
           'Sunday': '日', 'None': ''}


QUERY = """select id, publication_start_date, 
               date, date_end_1, limit_1, publication_status_1, is_accept_reservation_1, reserve_start_1, reserve_end_1,
               date_2, date_end_2, limit_2, publication_status_2, is_accept_reservation_2, reserve_start_2, reserve_end_2,
               date_3, date_end_3, limit_3, publication_status_3, is_accept_reservation_3, reserve_start_3, reserve_end_3,
               date_4, date_end_4, limit_4, publication_status_4, is_accept_reservation_4, reserve_start_4, reserve_end_4,
               date_5, date_end_5, limit_5, publication_status_5, is_accept_reservation_5, reserve_start_5, reserve_end_5,
               date_6, date_end_6, limit_6, publication_status_6, is_accept_reservation_6, reserve_start_6, reserve_end_6,
               date_7, date_end_7, limit_7, publication_status_7, is_accept_reservation_7, reserve_start_7, reserve_end_7
           from orientation
           where manuscript_id is not null 
               and publication_start_date <= NOW() and (
                date_end_1 >= NOW() or
                date_end_2 >= NOW() or 
                date_end_3 >= NOW() or 
                date_end_4 >= NOW() or 
                date_end_5 >= NOW() or 
                date_end_6 >= NOW() or
                date_end_7 >= NOW()
               ) or (date >= NOW() or
               date_1 >= NOW() or
               date_2 >= NOW() or
               date_3 >= NOW() or
               date_4 >= NOW() or
               date_5 >= NOW() or
               date_6 >= NOW() or
               date_7 >= NOW() or)"""

RESERVED_COUNTER = """
                    select orientation_id, datetime_no, count(datetime_no)
                    from reserved_orientation
                    where status in (1,2) and orientation_id in ({}) group by orientation_id , datetime_no order by orientation_id
                    """

status_disable_reserve = ['受付終了と表示する', '新卒ナビで予約を受け付けない']


def lambda_handler(event, context):
    print("start", datetime.now())
    orientations = get_list_orientation()
    print("get data", datetime.now())
    extracted_data = extract_data(orientations)
    delete_item()
    print("delete", datetime.now())
    update_dynamo(extracted_data)
    print("end", datetime.now())


def extract_data(orientations):
    ids = [str(o[0]) for o in orientations]
    ids_str = ','.join(ids)
    reserved_data = get_reserved(ids_str)
    date_data = []
    current_date = datetime.now(JST)
    date_now = current_date.date()

    for orientation in orientations:
        # unpack data
        id, publication_start_date, \
          date_1, date_end_1, limit_1, publication_status_1, is_accept_reservation_1, reserve_start_1, reserve_end_1, \
          date_2, date_end_2, limit_2, publication_status_2, is_accept_reservation_2, reserve_start_2, reserve_end_2, \
          date_3, date_end_3, limit_3, publication_status_3, is_accept_reservation_3, reserve_start_3, reserve_end_3, \
          date_4, date_end_4, limit_4, publication_status_4, is_accept_reservation_4, reserve_start_4, reserve_end_4, \
          date_5, date_end_5, limit_5, publication_status_5, is_accept_reservation_5, reserve_start_5, reserve_end_5, \
          date_6, date_end_6, limit_6, publication_status_6, is_accept_reservation_6, reserve_start_6, reserve_end_6, \
          date_7, date_end_7, limit_7, publication_status_7, is_accept_reservation_7, reserve_start_7, reserve_end_7 = orientation

        result_date = []
        get_date(id, publication_start_date, reserve_start_1, reserve_end_1, date_1, is_accept_reservation_1, \
            publication_status_1, limit_1, 1, reserved_data, date_now, result_date)
        get_date(id, publication_start_date, reserve_start_2, reserve_end_2, date_2, is_accept_reservation_2, \
            publication_status_2, limit_2, 2, reserved_data, date_now, result_date)
        get_date(id, publication_start_date, reserve_start_3, reserve_end_3, date_3, is_accept_reservation_3, \
            publication_status_3, limit_3, 3, reserved_data, date_now, result_date)
        get_date(id, publication_start_date, reserve_start_4, reserve_end_4, date_4, is_accept_reservation_4, \
            publication_status_4, limit_4, 4, reserved_data, date_now, result_date)
        get_date(id, publication_start_date, reserve_start_5, reserve_end_5, date_5, is_accept_reservation_5, \
            publication_status_5, limit_5, 5, reserved_data, date_now, result_date)
        get_date(id, publication_start_date, reserve_start_6, reserve_end_6, date_6, is_accept_reservation_6, \
            publication_status_6, limit_6, 6, reserved_data, date_now, result_date)
        get_date(id, publication_start_date, reserve_start_7, reserve_end_7, date_7, is_accept_reservation_7, \
            publication_status_7, limit_7, 7, reserved_data, date_now, result_date)

        is_active = any([x['is_active'] for x in result_date])
        is_seven_day = any([x['is_seven_day'] for x in result_date])

        result_date = group_data(result_date)
        result = ranger_date_index(id, result_date, date_now)

        data = {
            'orientation_id': id,
            'dates': result,
            'is_active': is_active,
            'is_seven_day': is_seven_day
        }

        date_data.append(data)

    return date_data


def replace_date_in_actual_date(actual_date):
    result = []
    for item in actual_date:
        if not item['is_active']:
            continue
        item['date'] = str(item['date_str'])
        result.append(item)
    return result


def group_data(data):
    groups = []
    result = []
    for k, g in groupby(data, lambda x: x['date_str']):
        groups.append(list(g))

    for g in groups:
        if len(g) > 0:
            selected = [lg['selected'] for lg in g]
            if '◯' in selected:
                selected = '◯'
            elif '△' in selected:
                selected = '△'
            elif '－' in selected:
                selected = '－'
            elif '✖️' in selected:
                selected = '✖️'
            else:
                selected = '受付終了'
            g[0]['selected'] = selected
            result.append(g[0])
    return result


def ranger_date_index(id, date_data, date_now):
    result = []

    old_date = [x['date_str'] for x in date_data]

    for i in range(0, 7):
        date_index = date_now + timedelta(days=i)
        week_day = WEEKDAY[date_index.strftime('%A')]
        month = str(date_index.month) if date_index.month >= 10 else '0{}'.format(date_index.month)
        day = str(date_index.day) if date_index.day >= 10 else '0{}'.format(date_index.day)
        date = '{}/{}'.format(month, day)
        date_index = datetime.strptime(date_index.strftime('%Y%m%d'), '%Y%m%d')
        date_index = '{}/{}/{}'.format(date_index.month, date_index.day, date_index.year)
        if date_index in old_date:
            index = old_date.index(date_index)
            if 'date' in date_data[index]:
                month = date_data[index]['date'].month if date_data[index]['date'].month >= 10 else '0{}'.format(
                    date_data[index]['date'].month)
                day = date_data[index]['date'].day if date_data[index]['date'].day >= 10 else '0{}'.format(
                    date_data[index]['date'].day)
                date = '{}/{}'.format(month, day)
                date_data[index]['date'] = date
            result.append(date_data[index])
        else:
            result.append({
                'orientation_id': id,
                'date_no': i,
                'date': date,
                'selected': '－',
                'date_str': date_index,
                'week_day': week_day,
                'is_seven_day': False
            })
    return result


def get_date(id, publication_start_date, reserve_start, reserve_end, date, is_accept_reservation, publication_status,
        limit, date_no, reserved_data, date_now, date_data):

    if publication_status == 2 or not date:
        return

    is_seven_day = False
    selected = '◯'
    is_active = True

    try:
        limit = int(limit)
    except Exception as _:
        limit = None

    reserved_number = get_reserved_number(id, reserved_data, date_no)

    date_datetime = datetime.strptime(date.strftime('%Y%m%d'), '%Y%m%d')
    date_now_datetime = datetime.strptime(date_now.strftime('%Y%m%d'), '%Y%m%d')
    if not reserve_start:
        reserve_start = publication_start_date
    if not reserve_end:
        reserve_end = date - timedelta(days=1)
    reserve_start = datetime.strptime(reserve_start.strftime('%Y%m%d'), '%Y%m%d')
    reserve_end = datetime.strptime(reserve_end.strftime('%Y%m%d'), '%Y%m%d')

    date_after_7_day = date_now_datetime + timedelta(days=7)

    if date_datetime >= date_now_datetime:
        if date_datetime <= date_after_7_day:
            is_seven_day = True
        if limit is not None:
            if reserved_number >= limit:
                selected = '✖️'
                is_active = True
            elif reserved_number < limit:
                if 0 <= limit - reserved_number <= 5:
                    selected = '△'
                    is_active = True
                else:
                    selected = selected
        else:
            selected = selected
        #if date_datetime == date_now_datetime:
        #    selected = '✖️'
        #    is_active = True
        #else:
        #    selected = selected

        if is_accept_reservation == '受付終了と表示する':
            selected = '受付終了'
            is_active = True
        else:
            selected = selected
            is_active = is_active

        if not reserve_start <= date_now_datetime <= reserve_end:
            selected = '－'

        data = {
            'orientation_id': id,
            'date_no': date_no,
            'is_active': is_active,
            'date_str': f'{date.month}/{date.day}/{date.year}',
            'date': date,
            'selected': selected,
            'week_day': WEEKDAY[date.strftime('%A')],
            'is_seven_day': is_seven_day
        }
        date_data.append(data)


def get_reserved(orientation_id):
    connection = get_rds_connection()
    data = {}

    with connection.cursor() as cur:
        query = RESERVED_COUNTER.format(orientation_id)
        cur.execute(query)
        cur._do_get_result()
        fetch_result = cur._result
        items = fetch_result.rows
        for item in items:
            if data.get(str(item[0])) is None:
                data[str(item[0])] = {str(item[1]): str(item[2])}
            else:
                data[str(item[0])][str(item[1])] = item[2]
        return data


def get_reserved_number(orientation_id, reserved_data, date_no):
    try:
        reserved_number = reserved_data[str(orientation_id)][str(date_no)]
        return int(reserved_number)
    except KeyError:
        return 0


def get_list_orientation():
    connection = get_rds_connection()
    with connection.cursor() as cur:
        cur.execute(QUERY)
        cur._do_get_result()
        fetch_result = cur._result
        items = fetch_result.rows
        return items


def update_dynamo(extracted_data):
    items = []
    for data in extracted_data:
        data['id'] = str(uuid.uuid4())
        items.append(data)
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)


def get_rds_connection():
    return pymysql.connect(DATABASE_HOST, user=DATABASE_USER, passwd=DATABASE_PASS, db=DATABASE_NAME, connect_timeout=5)


def get_current_date():
    return datetime.now().replace(tzinfo=reference.LocalTimezone())


def delete_item():
    scan = table.scan()
    items = scan['Items']
    with table.batch_writer() as batch:
        for item in items:
            batch.delete_item(Key={'id': item['id']})
