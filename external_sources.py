import requests
from typing import Iterator, Iterable
from urllib.error import HTTPError
import logging
import re
from itertools import tee, filterfalse
from datetime import datetime as dt
from orcid import ORCiDFuzzyDate, ORCiDContributor

logger = logging.getLogger(__name__)
logging.basicConfig(encoding='utf-8', level=logging.DEBUG)

def partition(predicate, iterable):
    """Partition entries into false entries and true entries.

    If *predicate* is slow, consider wrapping it with functools.lru_cache().
    """
    # partition(is_odd, range(10)) → 0 2 4 6 8   and  1 3 5 7 9
    t1, t2 = tee(iterable)
    return list(filterfalse(predicate, t1)), list(filter(predicate, t2))


class OpenAlexClient:
    '''
    Class for querying OpenAlex for authors and works.
    '''

    OPEN_ALEX_URL = 'https://api.openalex.org'

    def __init__(self, ror: str, email_address: str):
        '''
        :param ror: the ROR for the institution with which the authors are associated.
        :param email_address: for use with the OpenAlex API to gain access to the "polite" pool
        '''
        self.polite_param = {'mailto': email_address}
        self.ror = ror

    def get_author_ids(self, author_name: str) -> dict:
        '''Given an author's name (and the ROR, as defined above), retrieve the author's record from OpenAlex. Uses the last_known_institutions paramater as a filter.'''
        params = {'filter': f'display_name.search:{author_name},last_known_institutions.ror:{self.ror}'}
        params.update(self.polite_param)
        try: 
            response = requests.get(f'{OpenAlexClient.OPEN_ALEX_URL}/authors', params=params)
            response.raise_for_status()
            return response.json()
        except HTTPError:
            logger.error(response.text)
            return None

    def get_works(self, author_id: str, titles: Iterable[str], years: Iterable[str]) -> Iterator[list]:
        '''
        Retieves OpenAlex metadata, given an OpenAlex author ID and a list of titles and years of publication.
        :param years: should have None or empty string where no year supplied
        '''
        session = requests.Session()
        session.params = self.polite_param
        for title, year in zip(titles, years):
            # Need to remove colons and commas from titles for querying
            logger.debug(f'Getting info for {title}')
            title_param = re.sub(r'[:,]', ' ', title)
            if year:
                params = {f'filter': f'display_name.search:{title_param},author.id:{author_id},publication_year:{int(year)}'}
            else:
                params = {f'filter': f'display_name.search:{title_param},author.id:{author_id}'}
            try:
                response = session.get(f'{OpenAlexClient.OPEN_ALEX_URL}/works', params=params)
                response.raise_for_status()
                yield response.json()
            except HTTPError:
                logger.error(response.text)
                yield None



class OpenAlexMapping:

    # https://api.openalex.org/works?group_by=type
    OPENALEX_TYPE_MAPPING = { 'article': 'journal-article',
                             'book-chapter': 'book-chapter',
                             'book': 'book',
                             'dataset': 'data-set',
                             'dissertation': 'disseration',
                             'preprint': 'preprint',
                             'reference-entry': 'encyclopaedia-entry',
                             'review': 'book-review',
                             'report': 'report',
                             'other': 'other',
                             'peer-review': 'review',
                             'standard': 'standards-and-policy',
                             'editorial': 'other',
                             'erratum': 'other',
                             'letter': 'other',
                             'supplementary-materials': 'other'   }

    
    def resolve_duplicates(self, items: list[dict]) -> dict:
        '''
        In the case of multiple matches returned by the OpenAlex API, returns one based on the following logic:
            - prefer published type to preprint
            - choose the (non-preprint) item with the highest score
        '''
        # Separate out preprints
        preprints, other = partition(lambda x: x['type'] == 'preprint', items)
        if other:
            # return the first entry: OpenAlex results are returned in descending order of relevance
            return other[0] 
        return preprints[0]
    
    def extract_authors(self, authorships: list[dict]) -> Iterator[dict[str, str]]:
        '''Extracts ORCiD-relevant information from the authorships object of an OpenAlex work.'''
        for authorship in authorships:
            yield { 'credit_name': authorship['author']['display_name'], 
                    'contributor_orcid': authorship['author'].get('orcid')
                  } 

    def to_orcid_work(self, works: dict[str, list]) -> dict[str, str]:
        '''
        Extract work identifiers and other metadata from a set of OpenAlex API works, converting fields to those used by ORCiD
        '''
        if works and (works['meta']['count'] > 0):
            if len(works['results']) > 2:
                work = self.resolve_duplicates(works['results'])
            else:
                work = works['results'][0]
            # If it's missing a DOI, don't use it
            # OpenAlex uses the DOI as the canonical ID for works, so the field should be populated if a DOI is available
            if not work.get('doi'):
                return
            source = work.get('primary_location', {}).get('source')
            if source:
                source = source.get('display_name', None)
            
            orcid_work = { 'title': work['title'],
                        '_type': self.OPENALEX_TYPE_MAPPING[work['type']],
                        'journal_title': source,
                        'publication_date': ORCiDFuzzyDate.create_from_date(work['publication_date']),
                        'doi': work['doi'],
                        'url': work['primary_location'].get('landing_page_url') }
            orcid_work['contributors'] = ORCiDContributor.add_contributors([contributor for contributor in self.extract_authors(work['authorships'])])
            return orcid_work