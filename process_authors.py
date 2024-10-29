from lyterati_utils.elements_types import ElementsPersonList
from lyterati_utils.name_parser import AuthorParser
import click
import multiprocess as mp
import pandas as pd
import logging
from typing import Iterator, List, Tuple
from tqdm import tqdm

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
warnings_logger = logging.getLogger("py.warnings")
logging.captureWarnings(True)

ELEMENTS_FIELD_MAP = {'authors': 'authors'} # Map from Lyterati column name to Elements underlying field
TIMEOUT = 25 # in seconds

def _parse_process(conn: mp.Pipe): 
    '''
    Runs a loop (intended for a separate process) to parse strings with an instance of ElementsPersonList. Sends results over the provided instance of multiprocess.Pipe
    data sent over the pipe should be a tuple containing a dict mapping an Elements field to author names and a dict with the user's information
    '''
    parser = AuthorParser()
    while True:
        data = conn.recv()
        match data:
            case True:
                #logger.info('Starting new process!')
                continue
            case False:
                return
            case (persons, user):
                person_list = ElementsPersonList(persons, parser, user)
                conn.send(list(person_list))

def load_author_user_data(file: str, author_col) -> Iterator[tuple]:
    '''Loads preprocessed data for migration from a CSV, which should include the elements_id (object ID) as a column. Yields tuples of the relevant columns.'''
    df = pd.read_csv(file)
    df = df[[author_col, 'first_name', 'middle_name', 'last_name', 'elements_id']]
    p_bar = tqdm(df.itertuples(index=False), total=len(df))
    p_bar.set_description(f'Processing persons from {file}')
    for t in p_bar:
        yield t
    
def user_to_person(user: dict[str, str], fixed_data: dict[str, str]) -> dict[str, str]:
    user_data = {'first-name': user['first_name'], 
            'surname': user['last_name'], 
            'full': f'{user["first_name"]} {user["last_name"]}'
            }
    user_data.update(fixed_data)
    return user_data

def parse_persons(file: str, key_column: str='authors', category: str='publication') -> Tuple[List[dict], List[str]]:
    '''Runs the parser as a process that can be timed out to prevent the bug that consumes all available memory.
    key_column should match a key in ELEMENTS_FIELD_MAPPING, corresponding to a column in the Lyterati report data.'''
    timeouts = []
    succeeded = []
    main, worker = mp.Pipe()
    proc = mp.Process(target=_parse_process, args=(worker,))
    proc.start()
    main.send(True)
    for data in load_author_user_data(file, key_column):
        data = data._asdict()
        # Skip rows without an ID
        if not data['elements_id'] or pd.isna(data['elements_id']):
            continue
        el_key = ELEMENTS_FIELD_MAP[key_column]
        user = { k:v for k,v in data.items() if k in ['first_name', 'middle_name', 'last_name'] if v and not pd.isna(v)}
        fixed_data = {'id': data['elements_id'],
                     'category': category,
                     'field-name': el_key}
        # If no author string to parse, just add the user and move on
        if not data[key_column] or pd.isna(data[key_column]):
            succeeded.append(user_to_person(user, fixed_data))
            continue
        persons = { el_key: data[key_column] }
        main.send((persons, user))
        if main.poll(TIMEOUT):
            person_rows = main.recv()
            for person in person_rows:
                person.update(fixed_data)
            if not person:
                person_rows = [user_to_person(user, fixed_data)]
            succeeded.extend(person_rows)
        else:
            proc.terminate()
            timeouts.append(persons)
            succeeded.append(user_to_person(user, fixed_data))
            proc = mp.Process(target=_parse_process, args=(worker,))
            proc.start()
            main.send(True)
    main.send(False)
    return succeeded, timeouts

@click.command()
@click.option('--data-source', required=True)
@click.option('--target', required=True)
def main(data_source, target):
    succeeded, timeouts = parse_persons(data_source)
    pd.DataFrame.from_records(succeeded).to_csv(target, index=False)
    with open('timedout_from_process_authors.log', 'w') as f:
        for t in timeouts:
            f.write(f'{t}\n')

if __name__ == '__main__':
    main()