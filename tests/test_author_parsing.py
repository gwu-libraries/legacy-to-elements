import pytest
from name_parser import AuthorParser
import json

@pytest.fixture()
def author_tests():
    with open('tests/author-test-cases.json') as f:
        yield json.load(f)

class TestAuthorParser:

    def test_parse_many(self, author_tests):
        for test in author_tests:
            result, error = AuthorParser().parse_one(test['original_string'])
            if result:
                assert ';'.join([f'{i+1}_{author.name}' for i, author in enumerate(result)]) == test['parsed_result']
            assert error is None, error