import pytest
from lyterati_utils.elements_types import SourceHeading, ElementsObjectID, ElementsMapping, ElementsMetadataRow, ElementsPersonList
from lyterati_utils.name_parser import AuthorParser
import pandas as pd

@pytest.fixture()
def mapping():  
    return ElementsMapping('./tests/test_mapping.csv')

@pytest.fixture()
def row():
    return {'college_name': 'School of Pub Hlth & Hlth Serv',
            'department_name': 'Biostatistics&Bioinformatics',
            'last_name': 'Krandall',
            'first_name': 'Heath',
            'heading_type': pd.NA,
            'contribution_year': '2001.0',
            'additional_details': 'Invited Participant: 2001',
            'url': pd.NA,
            'school_code': 'old_SPH',
            'report_code': 'Presentations',
            'category': 'Service',
            'service_heading': 'Presentations',
            'name': 'National Human Genome Research Institute',
            'collaborators': 'Ledger H, Bar H, and CE Heath',
            'middle_name': 'A',
            'gw_id': '9999999'}

@pytest.fixture()
def seed():
    return {'college_name': 'School of Pub Hlth & Hlth Serv',
            'department_name': 'Biostatistics&Bioinformatics',
            'last_name': 'Krandall',
            'first_name': 'Heath',
            'heading_type': 'Digital Media',
            'contribution_year': '2014',
            'additional_details': pd.NA,
            'url': 'https://www.genomeweb.com/informatics/uniconnect-donates-lab-management-software-gw-comp-bio-institute',
            'school_code': 'old_SPH',
            'report_code': 'Media Contributions',
            'category': 'Service',
            'service_heading': 'Media Contributions',
            'name': 'GenomeWeb',
            'collaborators': pd.NA,
            'middle_name': 'A',
            'gw_id': '9999999'}

@pytest.fixture()
def user():
    return {'first_name': 'Heath', 
            'last_name': 'Krandall',
            'middle_name': 'A'}

@pytest.fixture()
def minter(seed):
    minter = ElementsObjectID()
    minter.mint_id(seed.values())
    return minter


@pytest.fixture()
def parser():
    return AuthorParser()

@pytest.fixture()
def metadata_row(row, mapping, minter, parser):
    return ElementsMetadataRow(row, SourceHeading.SERVICE, mapping, minter, parser), minter

@pytest.fixture()
def person_list(metadata_row):
    return metadata_row[0]._persons


class TestElementsMapping:

    def test_object_mapping(self, mapping):

        assert mapping.object_type_map == {'Media Contributions': 'broadcast-interview',
                                                'Committees': 'committee-membership',
                                                'Community Service & Other Service': 'community-service',
                                                'Consulting, Clinical Practice, and Other Engagements': 'consulting-advisory',
                                                'Awards and Honors': 'distinction',
                                                'Editorial Boards & Reviews': 'editorial',
                                                'Professional Memberships': 'membership',
                                                'Presentations': 'non-research-presentation'}
    
    def test_field_type_mapping(self, mapping):

        assert mapping.field_type_map == {'title': 'text',
                                            'description': 'text',
                                            'start-date': 'date',
                                            'end-date': 'date',
                                            'membership-type': 'choice',
                                            'employee-type': 'choice',
                                            'office-type': 'choice',
                                            'event-type': 'choice',
                                            'publication-type': 'choice',
                                            'service-type': 'choice',
                                            'org-type': 'choice',
                                            'review-type': 'choice',
                                            'assessment-type': 'choice',
                                            'event-start-date': 'date',
                                            'event-end-date': 'date',
                                            'co-contributors': 'person-list',
                                            'institution': 'address-list',
                                            'organisation': 'address-list',
                                            'department': 'text',
                                            'location': 'address-list',
                                            'event-name': 'text',
                                            'invited': 'boolean',
                                            'keynote': 'boolean',
                                            'competitive': 'boolean',
                                            'amount': 'integer',
                                            'person': 'person',
                                            'url': 'url',
                                            'committee-role': 'choice',
                                            'administrative-role': 'choice',
                                            'service-role': 'choice',
                                            'supervisory-role': 'choice',
                                            'awarded-amount': 'money',
                                            'distinction-type': 'choice',
                                            'country': 'choice'}
        
    def test_column_mapping(self, mapping):

        assert mapping.column_map == {'Media Contributions': {'name': 'title',
                                        'contribution_year': 'start-date',
                                        'heading_type': 'department',
                                        'url': 'url'},
                                        'Committees': {'name': 'title',
                                        'contribution_year': 'start-date',
                                        'heading_type': 'membership-type',
                                        'url': 'url'},
                                        'Community Service & Other Service': {'contribution_year': 'start-date',
                                        'heading_type': 'service-type',
                                        'name': 'organisation',
                                        'url': 'url'},
                                        'Consulting, Clinical Practice, and Other Engagements': {'heading_type': 'description',
                                        'contribution_year': 'start-date',
                                        'name': 'organisation',
                                        'url': 'url'},
                                        'Awards and Honors': {'heading_type': 'title',
                                        'contribution_year': 'start-date',
                                        'name': 'institution',
                                        'url': 'url'},
                                        'Editorial Boards & Reviews': {'name': 'title',
                                        'contribution_year': 'start-date',
                                        'heading_type': 'publication-type',
                                        'url': 'url'},
                                        'Professional Memberships': {'heading_type': 'description',
                                        'contribution_year': 'start-date',
                                        'name': 'institution',
                                        'url': 'url'},
                                        'Presentations': {'contribution_year': 'start-date',
                                        'collaborators': 'co-contributors',
                                        'name': 'organisation',
                                        'url': 'url'}}

class TestElementsObjectID:

    def test_id_minter(self, metadata_row):
        assert len(set(metadata_row[1].used.values())) == 2


class TestElementsMetadataRow:

    def test_row(self, metadata_row):
        assert dict(metadata_row[0]) == {'category': 'activity', 'type': 'non-research-presentation', 'start-date': '2001-01-01', 'url': '', 'organisation': 'National Human Genome Research Institute', 'id': '150520'}
    
    def test_persons(self, metadata_row):
        assert list(metadata_row[0].extract_person_list()) == [{'category': 'activity', 'first-name': 'H', 'surname': 'Ledger', 'full': 'H Ledger', 'field-name': 'co-contributors', 'id': '150520'}, {'category': 'activity', 'first-name': 'H', 'surname': 'Bar', 'full': 'H Bar', 'field-name': 'co-contributors', 'id': '150520'}, {'category': 'activity', 'first-name': 'CE', 'surname': 'Heath', 'full': 'CE Heath', 'field-name': 'co-contributors', 'id': '150520'}]
    
class TestElementsPersonList:

    def test_persons(self, person_list, user):
        assert person_list.parse_names(person_list.persons['co-contributors']) == [{'first-name': 'H', 'surname': 'Ledger', 'full': 'H Ledger'}, {'first-name': 'H', 'surname': 'Bar', 'full': 'H Bar'}, {'first-name': 'CE', 'surname': 'Heath', 'full': 'CE Heath'}]
