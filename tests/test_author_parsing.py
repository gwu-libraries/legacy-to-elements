import pytest
from lyterati_utils.name_parser import AuthorParser
import json

@pytest.fixture()
def author_tests():
    with open('tests/author-test-cases.json') as f:
        yield json.load(f)

@pytest.fixture()
def author_parser():
    return AuthorParser()

class TestAuthorParser:

    def test_parse_many(self, author_tests, author_parser):
        for test in author_tests:
            result, error = author_parser.parse_one(test['original_string'])
            if result:
                result = author_parser._post_clean(result)
                assert ';'.join([f'{i+1}_{author.name}' for i, author in enumerate(result)]) == test['parsed_result']
            assert error is None, error