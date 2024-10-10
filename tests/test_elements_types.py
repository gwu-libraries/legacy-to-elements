import pytest
from lyterati_utils.elements_types import SourceHeading, ElementsObjectID, ElementsMapping, LinkType
from lyterati_utils.name_parser import AuthorParser
import pandas as pd
from tests.rows_fixtures import ACTIVITIES



@pytest.fixture()
def activity_inputs():
    return ACTIVITIES

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
            'gw_id': 'G9999991'}

@pytest.fixture()
def user():
    return {'first_name': 'Heath', 
            'last_name': 'Krandall',
            'middle_name': 'A',
            'gw_id': 'G99999991'}

@pytest.fixture()
def minter(seed):
    minter = ElementsObjectID()
    minter.mint_id(seed.values())
    return minter

@pytest.fixture()
def parser():
    return AuthorParser()

@pytest.fixture()
def activity_mapping(minter, parser):  
    return ElementsMapping('./tests/activity-mapping.csv', minter, parser, user_id_field='gw_id')

@pytest.fixture()
def activity_rows(activity_inputs, activity_mapping):
    return [ activity_mapping.make_mapped_row(_input, SourceHeading.SERVICE) for _input in activity_inputs ]


class TestElementsMapping:

    def test_object_mapping(self, activity_mapping):

        assert activity_mapping.object_type_map['Committees'] == 'committee-membership'
    
    def test_field_type_mapping(self, activity_mapping):

        assert activity_mapping.field_type_map['c-additional-details'] == 'text'

        
    def test_column_mapping(self, activity_mapping):

        assert activity_mapping.column_map['Media Contributions']['name'] == 'title'


class TestLinkType:

    def test_creating_link_type_from_category(self):
        assert LinkType.from_object(SourceHeading.SERVICE.category) == LinkType.ACTIVITY

class TestElementsObjectID:

    def test_collisions(self, minter, activity_inputs):
        id1 = minter.mint_id(activity_inputs[0].values())
        id2 = minter.mint_id(activity_inputs[1].values()) 
        assert id1 != id2
        new_row = activity_inputs[1]
        assert minter.mint_id(new_row.values()) == id2        

class TestElementsActivityMetadata:

    def test_activity_row_attributes(self, activity_rows):
        
        assert activity_rows[0].data['additional_details'] == 'An additional detail'
        assert activity_rows[0].start_date == '2023-01-01'
        assert activity_rows[1].elements_fields == {'title': 'name', 'start-date': 'contribution_year', 'url': 'url', 'c-additional-details': 'additional_details'}

    def test_activity_rows_iter(self, activity_rows):
        mapped_dict = dict(activity_rows[0])
        assert mapped_dict['title'] == 'U.S. Department of State Office of Science and Technology Cooperation'
        assert mapped_dict['id'] == '5e31bac6'
        assert mapped_dict['c-additional-details'] == 'An additional detail'

    
    def test_activity_persons(self, activity_rows):
        for _ in activity_rows[0]:
            continue
        persons = [p for p in activity_rows[0].persons]
        assert persons == []
        for _ in activity_rows[2]:
            continue
        #persons = [p for p in activity_rows[2].persons]
        persons = [p for p in activity_rows[2].persons]
        assert persons[0]['first-name'] == 'CS'
        assert persons[0]['surname'] == 'Gunsolly'
        assert persons[0]['full'] == 'CS Gunsolly'
        assert persons[0]['field-name'] == 'co-contributors'
        assert persons[0]['id'] == '1c3c4d93'
        assert len(persons) == 4
        # All persons for the row should have the same id as the row itself
        assert {activity_rows[2].id} == {person['id'] for person in persons}
    
    def test_link_creation(self, activity_rows):
        link_row = activity_rows[0].link
        assert link_row['link-type-id'] == 23
        assert link_row['id-2'] == 'G99999998'
        assert link_row['id-1'] == '5e31bac6'
        assert link_row['category-1'] == 'activity'

        
