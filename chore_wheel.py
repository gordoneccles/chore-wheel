from abc import ABC
from dataclasses import dataclass
import datetime
from io import BytesIO
from functools import cached_property
import json
import logging
import os
import sys
from typing import Dict, Iterable

import boto3
from dateutil import rrule


logger = logging.getLogger(__name__)
root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s][%(name)s][%(levelname)s] %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)


@dataclass
class Person:
    name: str
    email: str
    total_work: int = 0


@dataclass
class Chore:
    name: str
    rrule: str
    work: int


class _S3DAL(ABC):

    def __init__(self, bucket_name, key_name):
        self._bucket_name = bucket_name
        self._key_name = key_name
        self._client = boto3.client('s3', region_name='us-east-1')

    def _read(self) -> str:
        logger.info(f'Reading from s3://{self._bucket_name}/{self._key_name}')
        data = self._client.get_object(
            Bucket=self._bucket_name, Key=self._key_name
        )['Body']
        return data.read()

    def _readlines(self) -> Iterable[str]:
        logger.info(f'Reading from s3://{self._bucket_name}/{self._key_name}')
        data = self._client.get_object(
            Bucket=self._bucket_name, Key=self._key_name
        )['Body']
        for line in BytesIO(data.read()):
            yield line.decode('utf8').strip()

    def _upload(self, data: bytes):
        logger.info(f'Writing to s3://{self._bucket_name}/{self._key_name}')
        res = self._client.put_object(
            Body=data, Bucket=self._bucket_name, Key=self._key_name
        )


class PeopleDAL(_S3DAL):

    @cached_property
    def people(self):
        rows = [json.loads(row.strip()) for row in self._readlines()]
        return [Person(row['name'], row['email'], row['total_work']) for row in rows]

    def store(self):
        f = BytesIO()
        for p in self.people:
            person_json = json.dumps({
                'name': p.name, 'email': p.email, 'total_work': p.total_work
            })
            f.write(person_json.encode('utf8') + b'\n')
        self._upload(f.getvalue())


class ChoreDAL(_S3DAL):

    @cached_property
    def chores(self):
        rows = [json.loads(row.strip()) for row in self._readlines()]
        return [Chore(row['name'], row['rrule'], row['work']) for row in rows]


class ChoreBlacklistDAL(_S3DAL):

    @cached_property
    def blacklist(self):
        return json.loads(self._read())


BUCKET_NAME = os.environ.get('BUCKET')
PEOPLE_KEY = os.environ.get('PEOPLE')
CHORES_KEY = os.environ.get('CHORES')
BLACKLIST_KEY = os.environ.get('BLACKLIST')
FROM_EMAIL = os.environ.get('FROM_EMAIL')
START_DATE = datetime.datetime(2021, 6, 21)

people_dal = PeopleDAL(BUCKET_NAME, PEOPLE_KEY)
chore_dal = ChoreDAL(BUCKET_NAME, CHORES_KEY)
blacklist_dal = ChoreBlacklistDAL(BUCKET_NAME, BLACKLIST_KEY)



def _todays_chores():
    today = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    end_rrule = f'UNTIL={today.strftime("%Y%m%d")}'
    for chore in chore_dal.chores:
        rrule_str = ';'.join([chore.rrule.rstrip(';'), end_rrule])
        chore_rrule = rrule.rrulestr(
            rrule_str, cache=True, ignoretz=True, dtstart=START_DATE
        )
        occurrences = list(chore_rrule)
        if not occurrences:
            continue

        if occurrences[-1].date() == today.date():
            yield chore
        else:
            logger.info(f'Today is not occurrence for {chore}')


def _next_person_for(chore):
    for p in sorted(people_dal.people, key=lambda p: p.total_work):
        if p.email not in blacklist_dal.blacklist.get(chore.name, []):
            return p

    raise Exception(f'{chore} was blacklisted for every person')


def _alert_person_to_chore(person, chore):
    client = boto3.client('ses', region_name='us-east-1')
    body = f'Hey {person.name}, time to {chore.name}'
    subject = f'Chore today: {chore.name}'
    html_body = f'<div>{body}</div>'
    logger.info(f'Emailing {person.email} to {chore.name}')
    client.send_email(
        Source=FROM_EMAIL,
        Destination={'ToAddresses': [person.email]},
        Message={
            'Subject': {'Data': subject},
            'Body': {
                'Text': {'Data': body},
                'Html': {'Data': html_body},
            },
        },
    )


def alert_todays_chores():
    logger.info(f"Peoples' work before: {people_dal.people}")
    for chore in _todays_chores():
        person = _next_person_for(chore)
        logger.info(f'Assigned {person.name} to chore "{chore.name}"')
        _alert_person_to_chore(person, chore)
        person.total_work += chore.work

    people_dal.store()
    logger.info(f"Peoples' work after: {people_dal.people}")


def lambda_handler(event, context):
    alert_todays_chores()



if __name__ == '__main__':
    alert_todays_chores()
