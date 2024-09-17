import pytest
from lyterati_utils import Lyterati, LyteratiMapping
from orcid import ORCiDWork, ORCiDBatch, ORCiDFuzzyDate, ORCiDContributor
import json
from dataclasses import asdict
import requests_mock 
from external_sources import OpenAlexClient, OpenAlexMapping


@pytest.fixture()
def date():
    return {'2022-01-02': ('2022', '01', '02')}

@pytest.fixture()
def date_parts():
    return { ('2022', '1', '2'): ('2022', '01', '02'),
            (2022, 1, 2):('2022', '01', '02'),
            ('2022', '4'): ('2022', '04', None),
            ('2022', 'July', '10'): ('2022', None, None),
            ('2022', '20', '4'): ('2022', None, None),
            ('22', '4', '1'): (None, None, None)   }

@pytest.fixture
def user_id():
    return 'G123456789'

@pytest.fixture
def orcid():
    return '9999-9999-9999-9999'
    
@pytest.fixture
def file_path():
    return './tests'

@pytest.fixture
def lyterati_works(user_id, file_path):
    return Lyterati(user_id=user_id, file_path=file_path, subset='work_files')

@pytest.fixture
def openalex_result():
    with open('./tests/oa_work.json') as f:
        return json.load(f)

@pytest.fixture
def openalex_work():
    return { 'title': 'Progress toward more fascinaing journal articles on topics of recurrent fascination',
             'journal_title': 'arXiv (Cornell University)',
             '_type': 'preprint',
             'publication_date': ORCiDFuzzyDate.create_from_date('2014-01-01'),
             'doi': 'https://doi.org/10.9999/999999',
             'url': 'https://arxiv.org/abs/99999999999',
             'contributors': [ORCiDContributor(**{ 'credit_name': 'Beatrix Potter', 
                               'contributor_sequence': 'first', 
                               'contributor_orcid': 'https://orcid.org/0000-0000-0000-0000'
                               })]
            }

@pytest.fixture
def orcid_works_from_l(orcid, lyterati_works):
    mapping = LyteratiMapping()
    return (ORCiDWork(orcid=orcid, **mapping.to_orcid_work(work, lyterati_works.user_name, orcid)) 
            for work in lyterati_works.user_data)

@pytest.fixture
def orcid_work_from_oa(openalex_result):
    oa_mapping = OpenAlexMapping()
    return oa_mapping.to_orcid_work(openalex_result)

@pytest.fixture
def author_names():
    return ['Beatrix Potter', 'Frank Chateaubriand']

@pytest.fixture
def contributors():
    return [{'credit_name': 'Beatrix Potter',
             'contributor_orcid': '0000-0000-0000-0000'},
             {'credit_name': 'Kenneth Grahame',
              'contributor_orcid': '0000-0000-0000-0001'}]

@pytest.fixture
def works():
    return [
            { 'title': 'A fascinating monograph',
            'journal_title': 'Oxfurred University Press',
            '_type': 'book',
            'publication_date': ORCiDFuzzyDate(year='2000'),
            'orcid': '9999-9999-9999-9999',
             '_metadata_source': 'lyterati' },
            { 'title': 'A fascinating article',
             'journal_title': 'Journal of Fascinating Ideas',
             '_type': 'article',
             'publication_date': ORCiDFuzzyDate(year='1994'),
             'doi': 'fake-doi-9999',
             'orcid': '9999-9999-9999-9999',
              '_metadata_source': 'open_alex' }
            ]


@pytest.fixture
def orcid_works(works, contributors):
    orcid_works = []
    for i, work in enumerate(works):
        work.update({ 'contributors': contributors, '_index': i })
        orcid_works.append(ORCiDWork(**work))
    return orcid_works

@pytest.fixture
def orcid_batch(lyterati_works, openalex_result, orcid, user_id):
    orcid_batch = ORCiDBatch(orcid, user_id)
    orcid_batch.register_mapping(LyteratiMapping, 'lyterati')
    orcid_batch.register_mapping(OpenAlexMapping, 'open_alex')
    for i, work in enumerate(lyterati_works.user_data):
        orcid_batch.add_work(work, mapping='lyterati', index=i, user_name=lyterati_works.user_name, orcid=orcid)
    orcid_batch.add_work(openalex_result, mapping='open_alex', index=i)
    return orcid_batch

@pytest.fixture
def oa_client():
    gwu_ror = 'https://ror.org/00y4zzh67'
    email_address = 'dsmith@gwu.edu'
    return OpenAlexClient(gwu_ror, email_address)

@pytest.fixture
def orcid_csv():
    with open('./tests/orcid_to_review.csv') as f:
        return f.read()

class TestLyterati:

    def test_load_work_files(self, lyterati_works, user_id):
        assert lyterati_works.user_data[0]['user_id'] == user_id
        assert len(lyterati_works.user_data) == 3
        assert lyterati_works.user_data[1]["title"] == 'Fascinating problems in fascinating literature'
        assert { k for record in lyterati_works.user_data for k in record.keys() } == {'user_id', 'title', 'publisher', 'authors', 'start_year','file_name', '_index'}
        assert lyterati_works.user_name == {'first_name': 'Jeremy', 'last_name': 'Fisher'}        
    
    def test_load_employment_files(self, user_id, file_path):
        lyterati = Lyterati(user_id=user_id, file_path=file_path, subset='employment_files')
        assert lyterati.user_data[0]['user_id'] == user_id
        assert len(lyterati.user_data) == 2
        assert lyterati.user_data[1]["title"] == 'Professor'
        assert { k for record in lyterati.user_data for k in record.keys() } == {'user_id', 'title', 'college', 'department', 'start_term', 'end_term', 'rank', 'file_name', '_index'}
        assert lyterati.user_name == {'first_name': 'Jeremy', 'last_name': 'Fisher'}
    
class TestLyteratiMapping:

    def test_to_orcid_work(self, orcid_works_from_l):
        work = next(orcid_works_from_l)
        assert work.title == 'A fascinating article'
        assert work.journal_title == 'Journal of Fascinating Literature'
        assert work.contributors[0].credit_name == 'Jeremy Fisher'
        assert work.publication_date.year == '2003'

class TestOPenAlexClient:
   
    def test_open_alex_get_works(self, oa_client, works, openalex_result):
        with requests_mock.Mocker() as m:
            m.get(f'{OpenAlexClient.OPEN_ALEX_URL}/works', text=json.dumps(openalex_result))
            results = [r for r in oa_client.get_works(author_id='12345', titles=[works[0]['title']], years=['2014'])]
        assert len(results) == 1

class TestOpenAlexMapping:

    def test_to_orcid_work(self, openalex_work, orcid_work_from_oa):
        for key in ['title', 'journal_title', '_type', 'doi', 'url']:
            assert orcid_work_from_oa[key] == openalex_work[key]
        assert orcid_work_from_oa['publication_date'].year == openalex_work['publication_date'].year
        assert orcid_work_from_oa['contributors'][0].credit_name == openalex_work['contributors'][0].credit_name
        assert orcid_work_from_oa['contributors'][0].contributor_orcid == openalex_work['contributors'][0].contributor_orcid
    
class TestORCiDFuzzyDate:

    def test_create_from_date(self, date):
        _in, _out = date.popitem()
        ofd = ORCiDFuzzyDate.create_from_date(_in)
        assert (ofd.year, ofd.month, ofd.day) == _out
    
    def test_validate_dates(self, date_parts):
        for _in, _out in date_parts.items():
            ofd = ORCiDFuzzyDate(*_in)
            assert (ofd.year, ofd.month, ofd.day) == _out

class TestORCiDContributor:

    def test_add_contributors(self, contributors):
        orcid_contribtors = ORCiDContributor.add_contributors(contributors)
        assert orcid_contribtors[0].credit_name == 'Beatrix Potter'
        assert orcid_contribtors[0].contributor_orcid == '0000-0000-0000-0000'
        assert orcid_contribtors[0].contributor_sequence == 'first'
        assert orcid_contribtors[1].contributor_sequence == 'additional'

class TestORCidBatch:

    def test_add_works(self, orcid_batch):
        assert len(orcid_batch.works) == 4
        assert max([work._index for work in orcid_batch.works]) == 2
        assert orcid_batch.works[3].external_id == 'https://doi.org/10.9999/999999'
        assert orcid_batch.works[2].external_id_type == 'source-work-id'

    def test_flatten_works(self, orcid_batch):
        df = orcid_batch.flatten()
        assert len(df.columns) == 11
        assert len(df) == 4
        assert list(df.work_number.values) == [2, 2, 1, 0]
    
    def test_to_csv(self, orcid_batch, orcid_csv):
        assert orcid_batch.to_csv().getvalue() == orcid_csv
