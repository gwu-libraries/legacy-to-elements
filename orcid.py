from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Optional, Type
from jinja2 import Environment, PackageLoader
from datetime import datetime
from io import StringIO
from itertools import groupby
import re, uuid
import pandas as pd


def finalize(value):
    '''
    Ensures that Jinja passes an empty string and "None" where None is set in the Python object
    '''
    return value if value is not None else ''

ENV = Environment(
    loader=PackageLoader(package_name='orcid', package_path='schemas'),
    finalize=finalize
)

@dataclass
class ORCiDWork:
    '''
    Class for all ORCiD work types
    '''
    title: str                                      # work title
    journal_title: str                              # publisher, journal title, etc.
    contributors: list[ORCiDContributor]            # author names, possibly with ORCiD and sequence indicator
    _type: str                                      # ORCiD work type
    publication_date: ORCiDFuzzyDate                # publication date
    orcid: str                                      # The ORCiD associated with the user whose work this is
    doi: Optional[str] = None                       # DOI for the work
    url: Optional[str] = None                       # possible URL for work
    _work_id: uuid.UUID = field(default_factory=uuid.uuid4) # internal ID for works; used in creating ORCiD records without DOI's
    _index: Optional[int] = None                    # Used for identifying possible duplicates when creating a sorted list of results
    _metadata_source: Optional[str] = None          # To indicate the external source for the data (lyterati, open_alex)


    template = ENV.get_template('work-full-3.0.json') # template for works
    
    @property 
    def type(self):
        return self._type
    
    @property
    def external_id(self):
        return self.doi if self.doi else self._work_id
    
    @property
    def external_id_type(self):
        return 'doi' if self.doi else 'source-work-id'
    
    @property
    def external_id_url(self):
        if self.doi and self.doi.startswith('https://'):
            return self.doi

    def create_json(self):
       return ORCiDWork.template.render(work=self) 
    
    def to_dict(self) -> dict[str, str]:
        '''
        Returns a dictionary representation of an instance, where the instance attributes correspond to columns. Flattens the contributors element, creating semicolon-delimited strings.
        '''
        obj_dict = asdict(self)
        obj_dict['contributors'] = ';'.join([ c['credit_name'] for c in obj_dict['contributors'] ])
        for key in ['year', 'month', 'day']:
            obj_dict[f'publication_{key}'] = getattr(obj_dict['publication_date'], key)
        return obj_dict        

class ORCiDBatch:
    '''
    Class for creating a batch of ORCiD works
    '''
    CSV_FIELDS = [ 'work_number', 'title', 'contributors', 'publication_source', 'publication_year', 'publication_month', 'publication_day', 'work_type', 'doi', 'use_this_version', 'metadata_source']
    # for use in labeling duplicates
    PREFERRED_METADATA_SOURCE = 'open_alex'

    def __init__(self, user_id: str, orcid: str):
        '''
        Creates a new batch of ORCiD works associated with the provided user ID and ORCiD. Creates a unique identifier for this batch.
        '''
        self.user_id = user_id
        self.orcid = orcid
        self.batch_id = f'{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}_{self.orcid}'
        self.mappings = {}
        self.works = []

    def register_mapping(self, mapping_cls, label):
        '''Registers a metadata mapper from an external source to ORCiD. The mapping class instance can be invoked by the label.
        The mapper class should have a to_orcid_work method that accepts the relevant metadata for a single work or result from the external source.'''
        self.mappings[label] = mapping_cls()
    
    def add_work(self, work: list | dict, mapping: str = None, index: int = -1, **kwargs):
        '''
        Creates an instance of an ORCiDWork using the supplied metadata.
        :param mapping: should be a label for a mapping previously registered with the register_mapping method.
        :param index: use if creating a batch from works where duplicate versions exist (duplicates should share the same index)
        '''
        if mapping:
           orcid_work = self.mappings[mapping].to_orcid_work(work, **kwargs)
           orcid_work.update({'_metadata_source': mapping, '_index': index})
           self.works.append(ORCiDWork(**orcid_work, orcid=self.orcid))
        else:
            self.works.append(ORCiDWork(**work))
        return self
    
    @classmethod
    def groupby_size_and_label(cls, df: Type[pd.DataFrame]) -> Type[pd.DataFrame]:
        '''
        Helper function to group duplicate works and flag the preferred datasource for duplicates
        '''
        df['size'] = len(df)
        if len(df) == 1:
            df['use_this_version'] = True
        else:
            df.loc[df._metadata_source == cls.PREFERRED_METADATA_SOURCE, 'use_this_version'] = True
        return df

    def flatten(self) -> Type[pd.DataFrame]:
        '''
        Formats the batch of ORCiD works as a DataFrame. Uses the _index attribute of each work for sorting and flagging duplicates.
        '''
        # Create a DataFrame of ORCiDWOrk instances, using the empty string for nulls
        works_df = pd.DataFrame.from_records([ work.to_dict() for work in self.works ]).fillna('')

        # sort so that duplicates works appear before unduplicated works and label preferred versions
        # explicitly passing all columns to avoid deprecation warning from pandas about group keys being excluded
        works_df = works_df.groupby('_index')[works_df.columns].apply(ORCiDBatch.groupby_size_and_label)

        works_df = works_df.rename(columns={ 'journal_title': 'publication_source',
                                            '_type': 'work_type',
                                            '_metadata_source': 'metadata_source',
                                             '_index': 'work_number' }).sort_values(['size', 'work_number'], ascending=False)
        return works_df[ORCiDBatch.CSV_FIELDS]

    def to_csv(self):
        '''Returns a flattened version of the batch of works as a CSV (string buffer) '''
        # Buffer for file output
        output = StringIO()
        self.flatten().to_csv(output, index=False)
        return output
            
           
@dataclass
class ORCiDAffiliation:
    '''
    Class for all ORCiD data types using the common:affiliation element (see https://github.com/ORCID/orcid-model/blob/master/src/main/resources/common_3.0/common-3.0.xsd)
    '''
    department_name: str                         
    role_title: str
    start_date: str


class ORCiDFuzzyDate:
    '''
    Utility class for creating elements using the common:fuzzy-date element (see https://github.com/ORCID/orcid-model/blob/master/src/main/resources/common_3.0/common-3.0.xsd)
    '''

    valid_year = re.compile(r'\d{4}')
    valid_date_part = re.compile(r'\d{2}')


    def __init__(self, year: str = None, month: str = None, day: str = None):
        '''
        Stores a date using separate attributes for year, month, and day
        '''
        self._year = year
        self._month = month
        self._day = day
    
    def validate(self, obj, obj_type):
        if not obj:
            return
        obj = str(obj).zfill(2)
        try:
            match obj_type:
                case 'year' if ORCiDFuzzyDate.valid_year.match(obj):
                    year = int(obj)
                    if year >= 1900 and year <= 2100:
                        return obj
                case 'month' if ORCiDFuzzyDate.valid_date_part.match(obj):
                    month = int(obj)
                    if month >= 1 and month <= 12 and self.year:
                        return obj
                case 'day' if ORCiDFuzzyDate.valid_date_part.match(obj):
                    day = int(obj)
                    if day >= 1 and day <= 31 and self.month:
                        return obj
                case _:
                    return 
        except TypeError:
            return None
    
    @property
    def year(self):
        return self.validate(self._year, 'year')
    
    @property
    def month(self):
        return self.validate(self._month, 'month')
    
    @property
    def day(self):
        return self.validate(self._day, 'day')
    
    @classmethod
    def create_from_date(cls, date_str: str) -> ORCiDFuzzyDate:
        '''
        Parses a date string, expecting %Y-%m-%d format, and creates an instance accordingly
        '''
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        return cls(date_obj.year, date_obj.month, date_obj.day)

@dataclass
class ORCiDContributor:
    
    credit_name: str                        # The contributor's name
    contributor_sequence: str = None        # One of first, additional
    contributor_orcid: str = None           # Contributor's ORCiD, if available

    @property
    def contributor_orcid_uri(self):
        if self.contributor_orcid:
            if not self.contributor_orcid.startswith('https'):
                return f'https://orcid.org/{self.contributor_orcid}'
            else:
                return self.contributor_orcid


    @classmethod
    def add_contributors(cls, contributors: list[dict[str, str]]) -> list[ORCiDContributor]:
        '''Given a list of contributors, returns a list of instances of this class, setting the sequence attribute according to their position in the list.'''
        orcid_contributors = []
        for i, contributor in enumerate(contributors):
            seq_value = 'first' if i == 0 else 'additional'
            orcid_contributors.append(cls(contributor_sequence=seq_value, **contributor))
        return orcid_contributors