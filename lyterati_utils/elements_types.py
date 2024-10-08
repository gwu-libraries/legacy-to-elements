from datetime import date
import pandas as pd
from enum import Enum
from hashlib import sha256
from .name_parser import AuthorParser, Author
from typing import Optional, Callable
import logging
import re

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class TermDates(Enum):
    '''Month and day for date of term'''
    FALL_START = (9, 1)
    FALL_END = (12, 31)
    SPRING_START = (1, 1)
    SPRING_END = (5, 31)
    SUMMER_START = (6, 1)
    SUMMER_END = (8, 31)

ID_LENGTH = 8

class SourceHeading(Enum):
    '''Defines the column in the source system that contains the name for Elements object type mapping'''
    SERVICE = 'service_heading'
    PUBS = 'research_heading'
    TEACHING = 'report_code' # Only one type of object in this file

    @property
    def include_user(self):
        '''Determines whether the user should be included in relevant person lists'''
        match self:
            case SourceHeading.SERVICE:
                return False
            case SourceHeading.PUBS:
                return True
    
    @property
    def category(self):
        match self:
            case SourceHeading.SERVICE:
                return 'activity'
            case SourceHeading.PUBS:
                return 'publication'
            case SourceHeading.TEACHING:
                return 'teaching-activity'
    

class LinkType(Enum):
    '''Defines the Elements user link types for each category'''
    ACTIVITY = 23
    TEACHING = 83
    AUTHOR = 8
    EDITOR = 9
    TRANSLATOR = 82
    CONTRIBUTOR = 92

    @classmethod
    def from_object(cls, heading: SourceHeading, type: str=None):
        '''Selects a link type based on the Elements object category and an optional object type'''
        match (heading.category, type):
            case ('activity', None):
                return cls.ACTIVITY
            case ('teaching-activity', None):
                return cls.TEACHING


LINK_HEADERS = ['category-1', 'id-1', 'category-2', 'id-2', 'link-type-id']

def create_links(user_id: str, work_id: str, heading: SourceHeading, object_type: Optional[str]=None) -> dict[str, str]:
    '''Returns dictionary linking object IDs to the supplied user ID, using the category and link type associated with the supplied instance of SourceHeading'''
    category = heading.category
    link_type_id = LinkType.from_object(heading, object_type).value
    return dict(zip(LINK_HEADERS, [category, work_id, 'user', user_id, link_type_id]))


def normalize(column_str: str) -> str:
    '''Normalizes column names to lower-case, underscore-separated'''
    return column_str.lower().replace(' ', '_')

class ElementsObjectID:
    '''Class to create unique IDs for objects.'''

    def __init__(self, path_to_id_store: str=None):
        '''Optionally, supply the path to CSV storing hashes and unique ID's. The ID's are assumed to be truncated prefixes of the hashes.'''
        if path_to_id_store:
            self.used = dict(pd.read_csv(path_to_id_store, header=None).values)
            self.path_to_id_store = path_to_id_store
        else:
            self.used = {}

    def mint_id(self, values: list[str]) -> str:
        '''Returns the first six characters of a hex digest for a SHA256 hash of the supplied list of values. Only non-null values will be used in creating the hash. If a list of ids was provided in creating the instance, ensures that the minted id is unique.'''
        input = ''.join([str(v) for v in values if (not pd.isna(v)) and v]).encode()
        hash = sha256(input).hexdigest()
        # If this hash is already in the store, return the ID
        if hash in self.used:
            return self.used[hash]
        # Otherwise, mint a new ID
        _id = hash[:ID_LENGTH]
        # Check for collisions on the prefix and increment until it no longer matches
        while (_id in self.used.values()):
            _id = hex(int(_id, 16) + 1)[:ID_LENGTH]
        self.used[hash] = _id
        return _id

    def persist_ids(self):
        '''Assumes a path was provided when creating the instance.'''
        pd.Series(self.used).to_csv(self.path_to_id_store, header=False)


class ElementsMapping:

    def __init__(self, path_to_mapping: str, path_to_choice_lists: str=None, concat_fields: dict[str: list[str]]=None):
        '''
        Loads a column mapping from a CSV file at path_to_mapping, and optionally, a mapping of Lyerati values to Elements values for Elements choice fields. Supply a dictionary for the concat_fields argument if necessary; fields in each list will have their values appended to the fields named as the dictionary keys. The fields names as keys should appear in the Elements mapping, or else they will be ultimately ignored.
        '''
        self.choice_map = self.build_choice_map(path_to_choice_lists) if path_to_choice_lists else None
        self.mapping = pd.read_csv(path_to_mapping)
        # Use the second row as the column heading
        self.mapping.columns = self.mapping.iloc[0].values
        # Expects that the mapping starts in the 4th column, with each pair of columns representing a mapping from Elements fields to fields in the source system
        # Maps each Elements object type to the type in the source system
        self.object_type_map = {b: a for a,b in zip(self.mapping.columns[3::2], self.mapping.columns[4::2]) if not pd.isna(b)}
        # For each record type in the source system, maps the associated fields to the underling fields in Elements
        self.column_map = {}
        for key, value in self.object_type_map.items():
            self.column_map[key] = { normalize(l_key): e_key.strip('"') for e_key, l_key in self.mapping[[self.mapping.columns[0], key]].values[2:] if not pd.isna(l_key) }
        # Mapping to derive the data type for each underlying field 
        self.field_type_map = dict([(k.strip('"'), v) for k,v in self.mapping.iloc[2:, 0:2].values 
                           if not pd.isna(k)])
        self.concat_fields = {from_field: to_field for to_field, v in concat_fields.items() 
                                    for from_field in v} if concat_fields else None
            
    def build_choice_map(self, path_to_choice_lists: str) -> dict[str, dict[str, str]]:
        '''Expects an Excel file, where each sheet corresponds to an Elements choice field.
        If the sheet has only one column, the column header is assumed to correspond to an Elements choice field, and the values to be tbe same in the source system and in Elements. If two columns, the second column is assumed to contain values in the source field to be mapped to the choice values in the Elements field. '''
        sheets = pd.read_excel(path_to_choice_lists, engine='openpyxl', sheet_name=None)
        choice_map = {}
        for name, sheet in sheets.items():
            sheet_dict = sheet.to_dict()
            # Case 1: one column, list to constrain Elements field name
            # Assume header == the sheet name == the Elements field name
            # Maps each possible value to itself
            if len(sheet_dict) == 1:
                choice_map[name] = { v: v for v in sheet_dict[name].values() }
            # Case 2: two columns, assume the first is the Elements field, the second the field in the source system.
            # Assume each value in the source column is present only once (though values in the Elements column map repeat)
            # Reverse the column order, created a mapping for each column value in 2 to 1
            else:
                choice_map[name] = dict(zip(*reversed([col.values() for col in sheet_dict.values()])))
        return choice_map
            
    
    def map_row(self, row: dict[str, str], map_type: SourceHeading) -> tuple[dict[str, str], dict[str, str]]:
        '''Input is a dict with the keys corresponding to column names in the source system. Outputs an udpated objects with keys reflecting the column names for import into Elements, with person fields in a separate dict.'''
        mapped_row = {}
        mapped_persons = {}
        source_type = row[map_type.value] # This is the input that determines the Element object type
        mapped_row['type'] = self.object_type_map[source_type]
        this_mapping = self.column_map[source_type] # This is the column mapping that determines the applicable columns for this particular type of object
        for k, v in row.items():
            # Skip rows without any data
            if pd.isna(v) or not v:
                continue
            # First, concatenate any fields as needed
            if self.concat_fields and k in self.concat_fields:
                to_field = this_mapping[self.concat_fields[k]]
                # Convert column name back to title case with spaces
                key = k.replace("_", " ").title()
                # Add to this field, which may or may not have content already
                if not mapped_row.get(to_field):
                    mapped_row[to_field] = f'(Legacy) {key}: {v}'
                else:
                    mapped_row[to_field] += f'\n\n(Legacy) {key}: {v}'
            # Check for the presence of this field in the Elements mapping
            elements_column = this_mapping.get(k)
            if not elements_column:
                continue
            if self.field_type_map[elements_column] in ['person', 'person-list']: # We don't add Person fields to the Metadata CSV
                mapped_persons[elements_column] = v
            # Choice field: make sure the values conform
            elif self.field_type_map[elements_column] == 'choice':
                choice = self.choice_map[elements_column].get(v)
                if not v:
                    logger.warn(f'Found value {v} for choice field {elements_column}, but {v} is not a permitted value for that field.')
                mapped_row[elements_column] = choice
            else:
                # Where concatenating fields, make sure we preserve any existing values when adding new values                         
                existing_value = mapped_row.get(elements_column)
                if existing_value:
                    v = existing_value + '\n\n' + v
                mapped_row[elements_column] = v  # Keep only those columns values that we want mapped
        return mapped_row, mapped_persons

class ElementsMetadataRow:
    '''Represents a single row for import data for Elements'''
    # Fields for which we want @property access, because we want to apply some formatting or type constraints
    properties = ['doi', 'start_date', 'end_date', 'department', 'institution', 'isbn_13']

    is_year = re.compile(r'((?:19|20)\d{2})(\.0)?')
    is_term = re.compile(r'(Spring|Fall|Summer) ((?:19|20)\d{2})')

    def __init__(self, row: dict[str, str], map_type: SourceHeading, mapper: ElementsMapping, minter: ElementsObjectID, parser: AuthorParser):
        '''Used to create a row for the metadata import out of a row of Lyterati data. If the namedtuple comes from a pandas DataFrame, the Index column will be discarded. The instance of ElementsMapping provides the column mapping from Lyterati. The row parameter accepts a dict or namedtuple. The instance of ElementsObjectID is used to create unique ID's for each object.'''
        if not isinstance(row, dict):
            row = row._asdict()
        if 'Index' in row:
            del row['Index']
        data, persons = mapper.map_row(row, map_type)
        # Use the original row values to mint the ID's, otherwise, we'll have duplicates, since the reduced Elements fieldset is not fully descriptive
        data['id'] = minter.mint_id(row.values())
        # Convert mapped dict (data) to object properties
        # This allows us to add special property handlers as needed for any Elements mapped fields
        for k, v in data.items():
            # Need to convert the format of Elements column names to valid Python names\
            key = k.replace('-', '_')
            # Replace nulls with null string
            value = '' if pd.isna(v) or not v else v
            # If it's in this list, we're using a custom getter
            if key in self.properties: 
                setattr(self, f'_{key}', value)
            else:
                setattr(self, key, value)
        if map_type.include_user:
            user = {'first_name': row['first_name'],
                    'last_name': row['last_name'],
                    'middle_name': row['middle_name']}
            self._persons = ElementsPersonList(persons, parser, user)
        else:
            self._persons = ElementsPersonList(persons, parser)
        self.category = map_type.category
    
    def __iter__(self):
        for key in vars(self):
            # Remove initial underscore for internal values
            if key == '_persons':
                continue
            if key.startswith('_') and key[1:] in self.properties:
                key = key[1:]
            yield key.replace('_', '-'), getattr(self, key)

    def extract_person_list(self):
        for person in self._persons:
            person.update({'category': self.category,
                           'id': self.id})
            yield person
    
    @staticmethod
    def convert_date(date_str: str, start_date: bool=True) -> str:
        if m := ElementsMetadataRow.is_year.match(str(date_str)):
            return date(int(m.group(1)), 1, 1).strftime('%Y-%m-%d')
        elif m := ElementsMetadataRow.is_term.match(date_str):
            year = int(m.group(2))
            term_suffix = '_START' if start_date else '_END'
            return date(year, *TermDates[m.group(1).upper() + term_suffix].value).strftime('%Y-%m-%d')
        else:
            logger.error(f'Unable to covert date string {date_str}. Skipping it.') 
            return

    @property
    def doi(self):
        pass

    @property
    def start_date(self):
        return ElementsMetadataRow.convert_date(getattr(self, '_start_date'))
    
    @property
    def end_date(self):
        return ElementsMetadataRow.convert_date(getattr(self, '_end_date'), False)
    
    @property
    def institution(self):
        return self._institution[:200]

    @property
    def department(self):
        return self._department[:100]
    
    @property
    def isbn_13(self):
        pass

class ElementsPersonList:
    '''Represents one or more rows of persons (expanded) data for import into Elements'''
    
    def __init__(self, persons: dict[str, str], parser: AuthorParser, user: Optional[dict[str, str]]=None):
        '''persons should be a mapping from an Elements field of type "person" or "person-list" to a string representing one or more persons. The _id should be the unique identifier of the Elements object associated with these persons. If user is supplied, the name will be added to the parsed list of personal names.'''
        self.parser = parser
        self.persons = persons
        self.user = user
    
    def __iter__(self):
        for _type, name_str in self.persons.items():
            if not isinstance(name_str, str):
                continue
            for person in self.parse_names(name_str):
                person.update({'field-name': _type})
                yield person

    def check_name_matches(self, parsed_name: Author) -> bool:
        '''Checks whether the parts of the provided user name match the parts of the provided parsed name. Surname must match, plus either first name or initials'''
        if self.user['last_name'] == ' '.join(parsed_name.last_name):
            # Case 1: first name is present, matches on all parts or first part (space-separated)
            if parsed_name.first_name:
                if (self.user['first_name'] == ' '.join(parsed_name.first_name)) or (self.user['first_name'] == parsed_name.first_name[0]):
                    return True   
            # If no first name in the parsed name, check initials  
            else: 
                if self.user['middle_name']:
                    initials = (self.user['first_name'][0] + self.user['middle_name'][0]).upper()
                else:
                    initials = self.user['first_name'][0].upper()
                # Case 2: First- and middle-initial match, or just first initial
                if parsed_name.initials:
                    if (initials == ''.join(parsed_name.initials)) or (initials[0] == parsed_name.initials[0]):
                        return True
            return False

    def name_to_dict(self, person: Author) -> str:
        surname = ' '.join(person.last_name)
        first_name = ' '.join(person.first_name)
        if first_name and person.initials:
           first_name += ' ' + ''.join(person.initials)
        elif person.initials:
            first_name = ''.join(person.initials)
        full_name = f'{first_name} {surname}' if first_name else surname
        return {'first-name': first_name, 'surname': surname, 'full': full_name}

    def parse_names(self, name_str: str) -> Optional[list[dict[str, str]]]:
        '''Parses a string containing multiple person names, returning either a list of dictionaries, where each dictionary contains the parts of the name, or else None, if the string could not be parsed. Match a user's name, if provided, against the parsed names. (Frequently, the user's name will be among those listed in the string.  If the user's name doesn't match the parsed names, append the user's name to the list.) If the string of names cannot be parsed, return only the user's name or None (if no user is provided).'''
        names_to_export = []
        match self.parser.parse_one(name_str):
            # Can't parse name: return user's name
            case None, error if self.user:
                return {'first-name': self.user['first_name'], 
                        'surname': self.user['last_name'], 
                        'full': f'{self.user["first_name"]} {self.user["last_name"]}'}
            case result, None if self.user:
                result = self.parser._post_clean(result)
                author_matched = False
                for person in result:
                    if not author_matched and self.check_name_matches(person):
                        author_matched = True
                    names_to_export.append(self.name_to_dict(person))
                # Append user name if not a match to any of the parsed person names
                if not author_matched:
                    names_to_export.append({'first-name': self.user['first_name'], 
                                            'surname': self.user['last_name'], 
                                            'full': f'{self.user["first_name"]} {self.user["last_name"]}'})
            case result, None:
                names_to_export = [self.name_to_dict(person) for person in self.parser._post_clean(result)]
        return names_to_export