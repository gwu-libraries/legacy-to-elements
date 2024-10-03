from datetime import date
import pandas as pd
from enum import Enum
from hashlib import sha256
from .name_parser import AuthorParser, Author
from typing import Optional, Callable

class SourceHeading(Enum):
    '''Defines the column in the source system that contains the name for Elements object type mapping'''
    SERVICE = 'service_heading'
    PUBS = 'research_heading'

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
            self.used = dict(zip(pd.read_csv(path_to_id_store, header=None).values))
            self.path_to_id_store = path_to_id_store
        else:
            self.used = {}

    def mint_id(self, values: list[str]) -> str:
        '''Returns the first six characters of a hex digest for a SHA256 hash of the supplied list of values. Only non-null values will be used in creating the hash. If a list of ids was provided in creating the instance, ensures that the minted id is unique.'''
        input = ''.join([v for v in values if (not pd.isna(v)) and v]).encode()
        hash = sha256(input).hexdigest()
        # If this hash is already in the store, return the ID
        if hash in self.used:
            return self.used[hash]
        # Otherwise, mint a new ID
        _id = hash[:6]
        # Check for collisions on the prefix and increment until it no longer matches
        while (_id in self.used.values()):
            _id = hex(int(_id, 16) + 1)[:6]
        self.used[hash] = _id
        return _id

    def persist_ids(self):
        '''Assumes a path was provided when creating the instance.'''
        pd.Series(self.used).to_csv(self.path_to_id_store, headers=None)


class ElementsMapping:

    def __init__(self, path_to_mapping: str, path_to_choice_file: str=None, concat_fields: dict[str: list[str]]=None):
        '''
        Loads a column mapping from a CSV file at path_to_mapping, and optionally, a mapping of Lyerati values to Elements values for Elements choice fields. Supply a dictionary for the concat_fields argument if necessary; fields in each list will have their values appended to the fields named as the dictionary keys. The fields names as keys should appear in the Elements mapping, or else they will be ultimately ignored.
        '''
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
        if concat_fields:
            self.concat_fields = {from_field: to_field for to_field, v in concat_fields.items() 
                                    for from_field in v}
    
    def map_row(self, row: dict[str, str], map_type: SourceHeading) -> tuple[dict[str, str], dict[str, str]]:
        '''Input is a dict with the keys corresponding to column names in the source system. Outputs an udpated objects with keys reflecting the column names for import into Elements, with person fields in a separate dict.'''
        mapped_row = {}
        mapped_persons = {}
        source_type = row[map_type.value] # This is the input that determines the Element object type
        mapped_row['type'] = self.object_type_map[source_type]
        this_mapping = self.column_map[source_type] # This is the column mapping that determines the applicable columns for this particular type of object
        for k, v in row.items():
            # First, concatenate any fields as needed
            if self.concat_fields and k in self.concat_fields:
                to_field = self.concat_fields[k]
                # Add to this field, which may or may not have content already
                if not row[to_field] or pd.isna(row[to_field]):
                    row[to_field] = f'{k}: {v}'
                else:
                    row[to_field] += f'\n\n{k}: {v}'
            # Check for the presence of this field in the Elements mapping
            elements_column = this_mapping.get(k)
            if not elements_column and k not in self.concat_fields:
                continue
            if self.field_type_map[elements_column] in ['person', 'person-list']: # We don't add Person fields to the Metadata CSV
                mapped_persons[elements_column] = v
            else:
                mapped_row[this_mapping[k]] = v  # Keep only those columns values that we want mapped
        return mapped_row, mapped_persons

class ElementsMetadataRow:
    '''Represents a single row for import data for Elements'''
    # Fields for which we want @property access, because we want to apply some formatting or type constraints
    properties = ['doi', 'start_date', 'department', 'institution', 'isbn_13']

    def __init__(self, row: dict[str, str], map_type: SourceHeading, mapper: ElementsMapping, minter: ElementsObjectID, parser: AuthorParser):
        '''Used to create a row for the metadata import out of a row of Lyterati data. If the namedtuple comes from a pandas DataFrame, the Index column will be discarded. The instance of ElementsMapping provides the column mapping from Lyterati. The row parameter accepts a dict or namedtuple. The instance of ElementsObjectID is used to create unique ID's for each object.'''
        if not isinstance(row, dict):
            row = row._asdict()
        if 'Index' in row:
            del row['Index']
        data, persons = mapper.map_row(row, map_type)
        data['id'] = minter.mint_id(data)
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

    @property
    def doi(self):
        pass

    @property
    def start_date(self):
        year = getattr(self, '_start_date')
        if year:
            year = int(float(year))
            return date(year, 1, 1).strftime('%Y-%m-%d')
    
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
        else:
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