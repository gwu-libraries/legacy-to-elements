from lyterati_utils import Lyterati
from external_sources import OpenAlexClient
from orcid import ORCiDWork, ORCiDBatch
import click
import json
import pandas as pd

@click.command()
@click.argument('path_to_files')
@click.argument('user_id')
@click.argument('orcid')
def lyterati_works_to_csv(path_to_files, user_id, orcid):
    '''Given a path to XML Lyterati data files and a user ID/ORCiD, retrieves records for that user ID from the XML files, queries OpenAlex for external identifiers, and prepares a CSV of the data.'''
    gwu_ror = 'https://ror.org/00y4zzh67'
    email_address = 'dsmith@gwu.edu'
    lyterati_works = Lyterati(user_id, path_to_files, subset=['work_files'])
    open_alex = OpenAlexClient(gwu_ror, email_address)
    author_data = open_alex.get_author_ids(' '.join(lyterati_works.user_name.values()))
    # TO DO: 
    # If only one result, use the OpenAlex ID
    # Check ORCiD in results against ORCiD linked to account
    # Flag if they don't match
    # If more than one result, look for a matching ORCiD and use that OpenAlex ID
    # If none match, don't use
    author_ids = [ { 'id_type': 'openalex_id',
                    'id_value': author_data['results'][0]['id'] },
                   { 'id_type': 'orcid',
                     'id_value': author_data['results'][0].get('orcid') }
                ]
    lyterati_works.update_with_author_ids(author_ids)
    # filter out conference presentations and papers (likely not to have DOI's)
    lyterati_for_doi = [ record for record in lyterati_works.user_data 
                      if lyterati_works.inverted_file_map[record['file_name']]['use_doi'] ]

    titles, years, indices = zip(*[ ( record['title'], record['start_year'], record['_index'] ) for record in lyterati_for_doi ])
    #oa_results = [ (index, result) for result, index in zip(open_alex.get_works(author_id=lyterati.author_ids['openalex_id'],
    #                                                                            titles=titles,
    #                                                                            years=years), indices) if result ]
    with open('works_data.json') as f:
        data = json.load(f)
    oa_results = [ (index, result) for result, index in zip(data, indices) if result ]
    oa_works = []
    for index, result in oa_results:
        work = open_alex.extract_metadata(result)
        if work:
            work['_index'] = index
            oa_works.append(ORCiDWork.create_from_source(work, 
                                                         source='open_alex', 
                                                         user_id=user_id, 
                                                         orcid=orcid))
    orcid_works = [ORCiDWork.create_from_source(work, 
                                                   source='lyterati', 
                                                   user_id=user_id, 
                                                   orcid=orcid, 
                                                   user_name=lyterati_works.user_name)
                                                   for work in lyterati_works.user_data]
    orcid_works.extend(oa_works)

    orcid_batch = ORCiDBatch(user_id, orcid)
    orcid_batch.add_works(orcid_works)
    with open(f'{orcid_batch.batch_id}.csv', 'w') as f:
        f.write(orcid_batch.to_csv().getvalue())
 

if __name__ == '__main__':
    lyterati_works_to_csv()