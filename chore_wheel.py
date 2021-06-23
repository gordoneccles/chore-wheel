from abc import ABC
from dataclasses import dataclass
import datetime
import json
from typing import Dict

from dateutil import rrule


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


class PeopleDAL:
    _FNAME = 'people.json'

    def __init__(self):
        self._people = None

    @property
    def people(self):
        if self._people is None:
            rows = [
                json.loads(row.strip()) for row in
                open(self._FNAME, 'r').readlines()
            ]
            self._people = [
                Person(row['name'], row['email']) for row in rows
            ]
        return self._people

    def store(self):
        with open(self._FNAME, 'w') as f:
            for p in self.people:
                person_json = json.dumps({
                    'name': p.name, 'email': p.email, 'total_work': p.total_work
                })
                f.write(person_json + '\n')


class ChoreDAL:

    _FNAME = 'chores.json'

    def __init__(self):
        self._chores = None

    @property
    def chores(self):
        if self._chores is None:
            rows = [
                json.loads(row.strip()) for row in
                open(self._FNAME, 'r').readlines()
            ]
            self._chores = [
                Chore(row['name'], row['rrule'], row['work']) for row in rows
            ]
        return self._chores


class ChoreBlacklistDAL:

    _FNAME = 'chore_blacklist.json'

    def __init__(self):
        self._blacklist = None

    @property
    def blacklist(self):
        if self._blacklist is None:
            self._blacklist = json.loads(open(self._FNAME, 'r').read().strip())
        return self._blacklist


people_dal = PeopleDAL()
chore_dal = ChoreDAL()
blacklist_dal = ChoreBlacklistDAL()
START_DATE = datetime.datetime(2021, 6, 21)


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
            print(f'Today is not occurrence for {chore}')


def _next_person_for(chore):
    for p in sorted(people_dal.people, key=lambda p: p.total_work):
        if p.email not in blacklist_dal.blacklist.get(chore.name, []):
            return p

    raise Exception(f'{chore} was blacklisted for every person')


def _alert_person_to_chore(person, chore):
    msg = f'Hey {person.name}, time to {chore.name}'
    print('******************EMAIL************************')
    print(msg)
    print('***********************************************')


def alert_todays_chores():
    print(f"Peoples' work before: {people_dal.people}")
    for chore in _todays_chores():
        person = _next_person_for(chore)
        print(f'Assigned {person.name} to chore "{chore.name}"')
        _alert_person_to_chore(person, chore)
        person.total_work += chore.work

    people_dal.store()
    print(f"Peoples' work after: {people_dal.people}")


if __name__ == '__main__':
    alert_todays_chores()
