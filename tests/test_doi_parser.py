import pytest
from lyterati_utils.doi_parser import Parser

@pytest.fixture()
def dois():
    with open('tests/test_dois.txt') as f:
        return [d for d in f]

@pytest.fixture()
def multiline_doi():
    with open('tests/multiline_doi.txt') as f:
        return f.read()
    
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

def extract_test_lines(file):
    lines = [l for l in file]
    return [(text, id_) for text, id_ in zip(lines[:-1:2], lines[1::2])]

class TestDOIParser:

    def test_parse_multiline(self, multiline_doi):
        assert Parser(multiline_doi).doi == '10.1097/NNE.0000000000000571'
    
    def test_parse_patent_number(self, patent):
        assert Parser(patent).doi is None
    
    def test_parse_dois(self, dois):
        for text, doi in dois:
            doi = doi.strip()
            if doi == '':
                assert Parser(text).doi is None
            else:
                assert Parser(text).doi == doi
    
    def test_parse_doi_as_url(self, doi_urls):
        for url, doi in doi_urls:
            doi = doi.strip()
            if doi == '':
                assert Parser(url, is_url=True).doi is None
            else:
                assert Parser(url, is_url=True).doi == doi
    
    def test_parse_isbns(self, isbns):
        for txt, id_ in isbns:
            format, id_type, number = id_.strip().split(',')
            if not number:
                number = None
            if format == 'url':
                assert Parser(txt, True).isbn == number
            else:
                assert Parser(txt).isbn == number