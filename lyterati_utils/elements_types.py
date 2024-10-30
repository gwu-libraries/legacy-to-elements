from __future__ import annotations
from datetime import date, datetime
import pandas as pd
from hashlib import sha256
from .name_parser import AuthorParser, Author
from typing import Optional, NamedTuple, Iterator
from collections import defaultdict
import re
from functools import partial
from enum import Enum
from .doi_parser import Parser
import warnings
import unicodedata


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
    RESEARCH = 'research_heading'
    TEACHING = 'report_code' # Only one type of object in this file

    @property
    def include_user(self):
        '''Determines whether the user should be included in relevant person lists'''
        match self:
            case SourceHeading.SERVICE:
                return False
            case SourceHeading.RESEARCH:
                return True
            case SourceHeading.TEACHING:
                return False
    
    @property
    def category(self):
        match self:
            case SourceHeading.SERVICE:
                return 'activity'
            case SourceHeading.RESEARCH:
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
    def from_object(cls, category: str, type: str=None):
        '''Selects a link type based on the Elements object category and an optional object type'''
        match (category, type):
            case ('activity', None):
                return cls.ACTIVITY
            case ('teaching-activity', None):
                return cls.TEACHING
            case ('publication', None):
                return cls.AUTHOR


LINK_HEADERS = ['category-1', 'id-1', 'category-2', 'id-2', 'link-type-id', 'visible']

PRIVACY_HEADERS = ['privacy', 'lock-privacy']

def normalize(column_str: str) -> str:
    '''Normalizes column names to lower-case, underscore-separated'''
    return column_str.strip().lower().replace(' ', '_')

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

    def __init__(self, path_to_mapping: str, 
                minter: ElementsObjectID, 
                parser: AuthorParser, 
                user_id_field: str,
                path_to_choice_lists: str=None, 
                concat_fields: dict[str: list[str]]=None,
                user_author_mapping: list[str]=None,
                doi_fields: list[str]=None,
                object_privacy: str=None):
        '''
        Loads a column mapping from a CSV file at path_to_mapping, and optionally, a mapping of Lyerati values to Elements values for Elements choice fields. Supply a dictionary for the concat_fields argument if necessary; fields in each list will have their values appended to the fields named as the dictionary keys. The fields names as keys should appear in the Elements mapping, or else they will be ultimately ignored.
        '''
        self.minter = minter
        self.parser = parser
        self.user_id_field = user_id_field
        self.choice_map = self.build_choice_map(path_to_choice_lists) if path_to_choice_lists else {}
        self.mapping = pd.read_csv(path_to_mapping)
        # Use the second row as the column heading
        self.mapping.columns = self.mapping.iloc[0].values
        # Expects that the mapping starts in the 4th column, with each pair of columns representing a mapping from Elements fields to fields in the source system
        # Maps each Elements object type to the type in the source system
        self.object_type_map = {b: a for a,b in zip(self.mapping.columns[3::2], self.mapping.columns[4::2]) if not pd.isna(b)}
        # Mapping to derive the data type for each underlying field 
        self.field_type_map = dict([ (k.strip('"'), v) for k,v in self.mapping.iloc[2:, 0:2].values 
                           if not pd.isna(k) and k ])
        # For each record type in the source system, maps the associated fields to the underlying fields in Elements
        self.column_map = {}
        for key, _ in self.object_type_map.items():
            self.column_map[key] = defaultdict(list)
            for el_key, source_key in self.mapping[[self.mapping.columns[0], key]].values[2:]:
                # The same source system field may map to more than one Elements field. To account for this, we add Elements fields as a list associated with each source system field. (For a many:1 relation between system fields and an Elements field, we use the concat_fields parameter.)
                if not pd.isna(source_key) and source_key:
                    source_key = normalize(source_key)
                    self.column_map[key][source_key].append(el_key.strip('"'))
        # Fields to concatenate in the source system for matching to a single Elements field
        self.concat_fields = { from_field: to_field for to_field, v in concat_fields.items() 
                                    for from_field in v } if concat_fields else None
        self.doi_fields = doi_fields
        self.user_author_mapping = user_author_mapping
        self.object_privacy = object_privacy
            
    def build_choice_map(self, path_to_choice_lists: str) -> dict[str, dict[str, str]]:
        '''Expects an Excel file, where each sheet corresponds to an Elements choice field. The sheet name is expected to correspond to the name of the Elements (underlying) choice field.
        If the sheet has only one column, the column header is ignored, and the values to be tbe same in the source system and in Elements. If two columns, one is expected to have the header "Source System" and to contain values in the source field to be mapped to the choice values in the Elements field. '''
        sheets = pd.read_excel(path_to_choice_lists, engine='openpyxl', sheet_name=None)
        choice_map = {}
        for name, sheet in sheets.items():
            # Case 1: one column, list to constrain Elements field name
            # Assume header == "Elements"
            # Maps each possible value to itself
            if len(sheet.columns) == 1:
                sheet_dict = sheet.to_dict()
                choice_map[name] = { v: v for v in sheet_dict['Elements'].values() }
            # Case 2: two columns, assume one named "Elements", the other, "Source System"
            # Assume each value in the source column is present only once (though values in the Elements column map repeat)
            # Create a mapping from each Source System column value to the Elements value
            else:
                # Drop nulls -- where a value isn't mapped
                sheet_dict = sheet.dropna().to_dict()
                # Assume one columne is named "Elements" and there is only one other column
                other_key = [k for k in sheet_dict.keys() if k != 'Elements'][0]
                choice_map[name] = dict(zip(*[sheet_dict[other_key].values(), sheet_dict['Elements'].values()]))
        return choice_map
    
    @staticmethod
    def choice_validator(value: str, choices: dict[str, str]):
        '''Used to validate choice fields based on the supplied dictionary.'''
        try:
            return choices[value]
        except KeyError:
            warnings.warn(f'Value {value} not in choice list {list(choices.keys())}. Skipping this value because it won\'t map to the underlying choice field.')

    
    def make_mapped_row(self, row: dict[str, str] | NamedTuple, map_type: SourceHeading) -> ElementsMetadataRow:
        '''Input is a dict with the keys (or namedtuple with attributes) corresponding to column names in the source system, and values corresponding to a row of data. Outputs an instance of ElementsMetadataRow for mapping that data to Elements fields.'''
         # source_type is the input that determines the Element object type
        if hasattr(row, '_asdict'):
            source_type = getattr(row, map_type.value)
            # Need to remove the pandas Index attribute, as this will cause problems for minting the unique IDs
            row = { k: v for k,v in row._asdict().items() if k != 'Index' }
        else:
            source_type = row[map_type.value]
        # Check for unmapped types in the source data
        if source_type not in self.column_map:
            warnings.warn(f'Unmapped type {source_type} found: skipping.')
            return None
        mapped_row = ElementsMetadataRow(row)
        mapped_row.user_id_field = self.user_id_field
        mapped_row.parser = self.parser
        # Elements object category
        mapped_row.category = map_type.category
        # Elements object type
        mapped_row.type = self.object_type_map[source_type]
        mapped_row.id = self.minter.mint_id(row.values())
        mapped_row.concat_fields = self.concat_fields
        # This is the column mapping that determines the applicable Elements columns for this particular type of object
        mapped_row.fields_from_source = self.column_map[source_type] 
        # This provides the name of the Elements fields for each column in the source, inverting the previous dict
        mapped_row.elements_fields = { el_field: source_key for source_key, v in mapped_row.fields_from_source.items() 
                                        for el_field in v}
        # Person fields are handled separately
        mapped_row.person_fields = [ k for k in mapped_row.elements_fields if self.field_type_map[k] in ['person', 'person-list'] ]
        # Whether to include the user in the person data
        if self.user_author_mapping:
            mapped_row.user_author_mapping = self.user_author_mapping
        # Whether to map DOI's
        if self.doi_fields:
            mapped_row.doi_fields = self.doi_fields
        # Whether to make objects (in)visibile
        if self.object_privacy is not None:
            # Comma-delimited tuple (from the config file), second value should be a Boolean
            privacy_settings = self.object_privacy.split(',')
            privacy_settings[1] = privacy_settings[1].upper()
            mapped_row.privacy_settings = privacy_settings
        # Add validator for choice fields
        for k in mapped_row.elements_fields:
            if k in self.choice_map:
                setattr(mapped_row, f'{k}_validator', partial(ElementsMapping.choice_validator, choices=self.choice_map[k]))
        return mapped_row

class ElementsMetadataRow:
    '''Represents a single row for import data for Elements'''
    # Fields for which we want @property access, because we want to apply some formatting or type constraints
    # Note that these field names use hyphens, not underscores, to match the Elements fields
    properties = ['doi', 'start-date', 'end-date', 'department', 'institution', 'isbn-13', 'publication-date', 'external-identifiers']

    is_year = re.compile(r'((?:19|20)\d{2})(\.0)?')
    is_term = re.compile(r'(Spring|Fall|Summer) ((?:19|20)\d{2})')

    def __init__(self, row: dict[str, str]):
        '''Used to create a row for the metadata import out of a row of Lyterati data. If the namedtuple comes from a pandas DataFrame, the Index column will be discarded. The instance of ElementsMapping provides the column mapping from Lyterati. The row parameter accepts a dict or namedtuple. The instance of ElementsObjectID is used to create unique ID's for each object.'''
        self.data = row

    def _concatenate_fields(self):
        '''Updates data to concatenate fields before returning mapped fields.'''
        if self.concat_fields:
            for k, v in self.concat_fields.items():
                concat_value = self.data.get(k)
                # Only concat if something to add
                if not pd.isna(concat_value) and concat_value:
                    key_string = k.replace('_', ' ').title()
                    # Check for empty fields
                    if pd.isna(self.data[v]) or not self.data[v]:
                        self.data[v] = f'(Legacy) {key_string}: {concat_value}'
                    else:
                        self.data[v] += f'\n\n(Legacy) {key_string}: {concat_value}'

    def __iter__(self) -> Iterator[str, str]:
        # Re-initialize _persons before every iteration, or else we'll create duplicates
        self._persons = {}
        # Fields every row will have
        yield 'id', self.id
        yield 'category', self.category
        yield 'type', self.type
        # Make sure fields are concatenated appropriately before returning
        self._concatenate_fields()
        # Other fields from the source system
        for key, value in self.data.items():
            # Skip NaN's
            if pd.isna(value) or (not value):
                continue
            if isinstance(value, str):
                # Fix bad strings, including form-feed characters
                value = Parser.clean_xl_text(value, False).encode('utf8').decode()
                value = unicodedata.normalize('NFKD', value).replace('\x0b', ' ')
            # Map field name to Elements
            # There may be more than one Elements field to be derived
            for e_key in self.fields_from_source.get(key, []):
                # Person field: extract separately
                if e_key in self.person_fields:
                    self._persons[e_key] = value
                # If property descriptor exists, use it
                elif e_key in self.properties:
                    yield e_key, getattr(self, e_key.replace('-', '_'))
                # If validator exists, use it
                elif hasattr(self, f'{e_key}_validator'):
                    yield e_key, getattr(self, f'{e_key}_validator')(value)
                else:
                    yield e_key, value
        if hasattr(self, 'privacy_settings'):
            for key, value in zip(PRIVACY_HEADERS, self.privacy_settings):
                yield key, value

    @property
    def persons(self) -> Iterator[dict[str, str]]:
        # Can't call persons unless __iter__ has been called already
        # In this case, we return None
        if not hasattr(self, '_persons'):
            raise Exception(f'Cannot access the persons attribute of {self} before invoking its __iter__ method.')
        if hasattr(self, 'user_author_mapping'):
            # Add the current user's names if needed to the list of persons
            user = { k: self.data[k] for k in self.user_author_mapping['fields'] if not pd.isna(self.data[k]) }
            persons = ElementsPersonList(self._persons, self.parser, user)
        else:
            persons = ElementsPersonList(self._persons, self.parser)
        for person in persons:
            person.update({'category': self.category,
                           'id': self.id})
            yield person
    
    @property
    def link(self):
        link_type_id = LinkType.from_object(self.category, None).value
        # 
        if hasattr(self, 'visibility_setting'):
            # Expect Boolean
            visibility_setting = str(self.visibility_setting).upper()
            return dict(zip(LINK_HEADERS, [self.category, self.id, 'user', self.data[self.user_id_field], link_type_id, visibility_setting]))
        return dict(zip(LINK_HEADERS, [self.category, self.id, 'user', self.data[self.user_id_field], link_type_id]))
    
    @staticmethod
    def convert_date(date_str: str, start_date: bool=True) -> str:
        if m := ElementsMetadataRow.is_year.match(str(date_str)):
            year = int(m.group(1))
            if start_date:
                return date(year, 1, 1).strftime('%Y-%m-%d')
            elif year < datetime.now().year:
                return date(int(m.group(1)), 12, 31).strftime('%Y-%m-%d')
            else:
                # None for end_date when it would be the current year
                return None 
        elif m := ElementsMetadataRow.is_term.match(date_str):
            year = int(m.group(2))
            term_suffix = '_START' if start_date else '_END'
            return date(year, *TermDates[m.group(1).upper() + term_suffix].value).strftime('%Y-%m-%d')
        else:
            warnings.warn(f'Unable to covert date string {date_str}. Skipping it.') 
            return

    @property
    def doi(self):
        '''Not a great solution, hard-coding the URL field from the source system, but we need to check multipled fields for DOI's without concatenating the fields in the output.'''
        source_key = self.elements_fields['doi']
        doi = Parser.extract_doi(self.data[source_key])
        if not doi:
            for field in self.doi_fields:
                doi = Parser.extract_doi(self.data.get(field, ''), is_url=True)
                if doi:
                    break
        return doi


    @property
    def start_date(self):
        source_key = self.elements_fields['start-date']
        return ElementsMetadataRow.convert_date(self.data[source_key])

    @property
    def publication_date(self):
        source_key = self.elements_fields['publication-date']
        return ElementsMetadataRow.convert_date(self.data[source_key])
    
    @property
    def end_date(self):
        source_key = self.elements_fields['end-date']
        return ElementsMetadataRow.convert_date(self.data[source_key], False)
    
    @property
    def institution(self):
        source_key = self.elements_fields['institution']
        return self.data[source_key][:200]

    @property
    def department(self):
        source_key = self.elements_fields['department']
        return self.data[source_key][:100]
    
    @property
    def isbn_13(self):
        source_key = self.elements_fields['isbn-13']
        return Parser.extract_isbn(self.data[source_key])
    
    @property
    def external_identifiers(self):
        '''
        Note from Elements team:
        An identifier consists of both a scheme(external system/type) and a value, these are separated by a colon. 
        Each identifier must be enclosed in single quotes, any quotes within an identifier must be escaped with a backslash. If a value contains colon it does not need to be escaped, since the identifier will only be split on the first identifier. 
        An identifier scheme cannot contain a colon. An identifier scheme must be one of the following values (meaning in brackets):

        pmc (PubMed Central ID)
        arxiv (arXiv ID)
        pubmed (PubMed ID)
        doi
        nihms (NIH Manuscript Submission ID)
        isidoc (Thomson Reuters Document Solution ID) 

        Below is an example of a possible value of an identifiers field:
        'arxiv:quant-ph/0612120';'pmc:PMC3348095';'pubmed:22547652'
        '''
        source_key = self.elements_fields['external-identifiers']
        ids = Parser.extract_pmids(self.data[source_key])
        if not all(ids): 
            url_ids = Parser.extract_pmids(self.data.get('url', ''), is_url=True)
            # Consolidate non-null values
            ids = [ _id if _id else url_ids[i] for i, _id in enumerate(ids) ]
        ids = dict(zip(('pubmed', 'pmc'), ids))
        return ';'.join([ f"'{key}:{value}'" for key, value in ids.items() if value ])
        

class ElementsPersonList:
    '''Represents one or more rows of persons (expanded) data for import into Elements'''
    
    def __init__(self, persons: dict[str, str], parser: AuthorParser, user: Optional[dict[str, str]]=None):
        '''persons should be a mapping from an Elements field of type "person" or "person-list" to a string representing one or more persons. If user is supplied, the name will be added to the parsed list of personal names.'''
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
                if self.user.get('middle_name'):
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
                return [{'first-name': self.user['first_name'], 
                        'surname': self.user['last_name'], 
                        'full': f'{self.user["first_name"]} {self.user["last_name"]}'}]
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