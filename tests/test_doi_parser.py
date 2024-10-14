import pytest
from lyterati_utils.doi_parser import Parser

@pytest.fixture()
def dois():
    with open('tests/test_dois.txt') as f:
        return [d for d in f]

   
@pytest.fixture()
def patent():
    return 'Base Patent Application filed, Attorney Docket No. 108807-011401/US. All inventors have provided equal intellectual contribution to this technology, September 22, 2012.'
            

@pytest.fixture()
def dois():
    with open('tests/test_dois.txt') as f:
        return extract_test_lines(f)

@pytest.fixture()
def doi_urls():
    with open('tests/test_doi_urls.txt') as f:
        return extract_test_lines(f)

@pytest.fixture()
def isbns():
    with open('tests/test_standard_numbers.txt') as f:
        return extract_test_lines(f)

@pytest.fixture()
def pmid_urls():
    return {'http://www.ncbi.nlm.nih.gov/pubmed/24041719': ('24041719', None),
            'http://www.ncbi.nlm.nih.gov/pubmed/24033717': ('24033717', None),
            'https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7847297/': (None, 'PMC7847297'),
            'https://pubmed.ncbi.nlm.nih.gov/34340017/': ('34340017', None),
            'http://www.ncbi.nlm.nih.gov/pmc/articles/pmid/1954927': ('1954927', None),
            'https://pubmed.ncbi.nlm.nih.gov/35681979/#:~:text=Results%3A%20The%20number%20of%20older,for%20spousal%20care%20(164%25).': ('35681979', None)
    }

@pytest.fixture()
def pmids_text():
    return {'2020;10(1):010307. doi: 10.7189/jogh.10.010307. PubMed PMID: 32257135; PMCID: PMC7100867.': ('32257135', 'PMC7100867'),
            'pii: S0020-7489(18)30270-0. doi: 10.1016/j.ijnurstu.2018.12.010. PMID:30660444': ('30660444', None),
            '28(3): pp. 265-277. PMID:16106155': ('16106155', None),
            'PMID: 22341995': ('22341995', None),
            '60(1), 87-94. DOI: 10.1016/j.amepre.2020.07.005. PMID: 33341182. PMCID: PMC7755027.': ('33341182','PMC7755027'),
            'doi: 10.1289/ehp.1003284, PMID:21543284, 2011.': ('21543284', None),
            '2012 May 15:1-10. [Epub ahead of print]. PMID: 22583563, PMCID: PMC3442149_x000D_': ('22583563', 'PMC3442149') }

def extract_test_lines(file):
    lines = [l for l in file]
    return [(text, id_) for text, id_ in zip(lines[:-1:2], lines[1::2])]

class TestDOIParser:
  
    def test_parse_patent_number(self, patent):
        assert Parser().extract_doi(patent) is None
    
    def test_parse_dois(self, dois):
        for text, doi in dois:
            doi = doi.strip()
            if doi == '':
                assert Parser().extract_doi(text) is None
            else:
                assert Parser().extract_doi(text) == doi
    
    def test_parse_doi_as_url(self, doi_urls):
        for url, doi in doi_urls:
            doi = doi.strip()
            if doi == '':
                assert Parser().extract_doi(url, is_url=True) is None
            else:
                assert Parser().extract_doi(url, is_url=True) == doi
    
    def test_parse_isbns(self, isbns):
        for txt, id_ in isbns:
            format, id_type, number = id_.strip().split(',')
            if not number:
                number = None
            if format == 'url':
                assert Parser().extract_isbn(txt, True) == number
            else:
                assert Parser().extract_isbn(txt) == number
    
    def test_parse_pubmed_pmc_urls(self, pmid_urls):
        for url, result in pmid_urls.items():
            assert Parser.extract_pmids(url, is_url=True) == result
    
    def test_parse_pubmid_pmc_text(self, pmids_text):
        for txt, result in pmids_text.items():
            assert Parser.extract_pmids(txt) == result