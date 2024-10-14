import pytest
from lyterati_utils.elements_types import SourceHeading, ElementsObjectID, ElementsMapping, LinkType
from lyterati_utils.name_parser import AuthorParser
import pandas as pd
from tests.rows_fixtures import ACTIVITIES, TEACHING_ACTIVITIES, PUBLICATIONS


@pytest.fixture()
def activity_inputs():
    return ACTIVITIES

@pytest.fixture()
def teaching_activity_inputs():
    return TEACHING_ACTIVITIES

@pytest.fixture()
def publication_inputs():
    return PUBLICATIONS

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
    return ElementsMapping('./tests/activity-mapping.csv', minter, parser, user_id_field='gw_id', path_to_choice_lists='./tests/activities-choice-list.xlsx')

@pytest.fixture()
def activity_rows(activity_inputs, activity_mapping):
    return [ activity_mapping.make_mapped_row(_input, SourceHeading.SERVICE) for _input in activity_inputs ]

@pytest.fixture()
def teaching_activity_mapping(minter, parser):
    concat_fields = { 'additional_details': ['placement_type', 'role', 'degree_type'] }
    return ElementsMapping('./tests/teaching-activity-mapping.csv', minter, parser, user_id_field='gw_id', concat_fields=concat_fields)

@pytest.fixture()
def teaching_activity_rows(teaching_activity_inputs, teaching_activity_mapping):
    return [ teaching_activity_mapping.make_mapped_row(_input, SourceHeading.TEACHING) for _input in teaching_activity_inputs ]


@pytest.fixture()
def publication_mapping(minter, parser):
    return ElementsMapping('./tests/publication-mapping.csv', minter, parser, user_id_field='gw_id')

@pytest.fixture()
def publication_rows(publication_inputs, publication_mapping):
    return [ publication_mapping.make_mapped_row(_input, SourceHeading.RESEARCH) for _input in publication_inputs ]

class TestElementsMapping:

    def test_object_mapping(self, activity_mapping):

        assert activity_mapping.object_type_map['Committees'] == 'committee-membership'
    
    def test_field_type_mapping(self, activity_mapping):

        assert activity_mapping.field_type_map['c-additional-details'] == 'text'

        
    def test_column_mapping(self, activity_mapping):

        assert activity_mapping.column_map['Media Contributions']['name'] == ['title']


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
    
    def test_choice_constraint(self, activity_rows):
        mapped_dict = dict(activity_rows[3])
        assert mapped_dict['c-membership-type'] == 'Professional'
        activity_rows[3].data['heading_type'] = 'professional'
        with pytest.warns():
            mapped_dict = dict(activity_rows[3])
        assert not mapped_dict.get('c-membership-type')

    
    def test_activity_persons(self, activity_rows):
        for _ in activity_rows[0]:
            continue
        persons = [p for p in activity_rows[0].persons]
        assert persons == []
        for _ in activity_rows[2]:
            continue
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

        
class TestElementsTeachingActivityMetadata:

    def test_row_attributes(self, teaching_activity_rows):
        assert teaching_activity_rows[0].data['role'] == 'Faculty mentor'
        assert teaching_activity_rows[0].start_date == '2021-09-01'
        assert teaching_activity_rows[0].end_date == '2022-05-31'
    
    def test_row_iter(self, teaching_activity_rows):
        #  Test for properly concatenated fields
        row_dict = dict(teaching_activity_rows[0])
        assert row_dict['c-additional-details'].startswith('During 2022 I mentored an accounting student')
        assert row_dict['c-additional-details'].endswith('(Legacy) role: Faculty mentor\n\n(Legacy) degree_type: Undergraduate')

    def test_link_creation(self, teaching_activity_rows):
        link_row = teaching_activity_rows[0].link
        assert link_row['link-type-id'] == 83
        assert link_row['id-2'] == 'G999999996'
        assert link_row['id-1'] == '579ef083'
        assert link_row['category-1'] == 'teaching-activity'

class TestElementsPublicationMetadata:

    def test_row_attributes(self, publication_rows):
        assert publication_rows[0].publication_date == '2014-01-01'
        assert publication_rows[0].doi is None
        assert publication_rows[1].doi == '10.1007/978-4-031-37776-6_19'
    
    def test_row_iter(self, publication_rows):
        # Test for source fields mapped to multiple Elements fields
        row_dict = dict(publication_rows[2])
        assert row_dict == {'id': '4b33df5e', 'category': 'publication', 'type': 'journal-article', 'publication-date': '2019-01-01', 'title': 'Impact of Impact and Impact Assistance on Journal Impact Factor for Academic Tenure', 'journal': 'Journal of Impacts in Pataphysics', 'doi': '10.1080/21551.2019.16221', 'external-identifiers': "'pmid:311244'"}
    
    def test_persons_iter(self, publication_rows):
        user_author_mapping = ['first_name', 'middle_name', 'last_name']
        # Case 1: User is in list of authors => should not be duplicated
        publication_rows[2].user_author_mapping = user_author_mapping
        _ = list(publication_rows[2])
        assert len(list(publication_rows[2].persons)) == 7
        # Case 2: User is not in list of authors => should be appended
        publication_rows[1].user_author_mapping = user_author_mapping
        _ = list(publication_rows[1])
        assert list(publication_rows[1].persons)[-1]['full'] == 'Penny Pompidour'
        
    def test_link_creation(self, publication_rows):
        link_row = publication_rows[2].link
        assert link_row == {'category-1': 'publication', 'id-1': '4b33df5e', 'category-2': 'user', 'id-2': 'G9999994', 'link-type-id': 8}
