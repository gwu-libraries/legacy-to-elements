from lxml import etree
from pathlib import Path
import re
from orcid import ORCiDFuzzyDate, ORCiDContributor

class Lyterati:

    parser = etree.XMLParser(recover=True)

    # attribute of the field containing the user ID 
    ID_FIELD = 'gw_id'

    FILE_MAP = {'work_files': [{'file_name': 'fis_acad_articles.xml', 
                                'use_doi': True},
                                {'file_name': 'fis_article_abstracts.xml',
                                 'use_doi': True},
                                {'file_name': 'fis_articles.xml', 
                                 'use_doi': True},
                                {'file_name': 'fis_books.xml',
                                 'use_doi': True},
                                {'file_name': 'fis_chapters.xml',
                                 'use_doi': True},
                                {'file_name': 'fis_conf_abstracts.xml',
                                 'use_doi': False}, 
                                {'file_name': 'fis_conf_papers.xml',
                                 'use_doi': False},
                                {'file_name': 'fis_conf_posters.xml',
                                 'use_doi': False},
                                {'file_name': 'fis_presentations.xml',
                                 'use_doi': False},
                                {'file_name': 'fis_reports.xml',
                                 'use_doi': True},
                                {'file_name': 'fis_reviews.xml',
                                'use_doi': True}
                               ],
                'employment_files': [{'file_name': 'fis_academic_appointment.xml'},
                                     {'file_name': 'fis_admin_appointment.xml'}],
                'identity_file': [{'file_name': 'fis_faculty.xml'}]
    }

    inverted_file_map = {item['file_name']: {'file_set': k, **item} for k, l in FILE_MAP.items() 
                         for item in l }

    def __init__(self, 
                 user_id: str, 
                 file_path: str = None, 
                 subset: str = None):
        
        '''
        Handles importing of data from Lyterati source. 
        :param user_id: ID of user whose Lyterati records should be returned
        :param file_path: if given, a path to the data (presumed XML) on disk
        :param subset: key to FILE_MAP; used to identify which types of Lyterati data to parse
        '''
        self.user_id = user_id
        
        if file_path:
            self.user_data = Lyterati.load_data_from_files(user_id, file_path, subset)
            # The fis_faculty.xml contains the name of the user associated with the provided user ID
            self.user_metadata = Lyterati.load_data_from_files(user_id, file_path, subset='identity_file')
        # Extract the name elements associated with this user
        # Assumes one record per user 
        self.user_name = {'first_name': self.user_metadata[0]['first_name'],
                     'last_name': self.user_metadata[0]['last_name']}
    
        self.author_ids = {}
    
    def update_with_author_ids(self, author_ids: list[dict[str, str]]):
        '''
        :param author_ids: each dict should include id_type and id_value as keys.
        '''
        for author_id in author_ids:
            self.author_ids.update({ author_id['id_type']: author_id['id_value'] })
    
    @classmethod
    def load_data_from_files(cls,
                  user_id: str, 
                  file_path: str, 
                  subset: str = None) -> list[dict[str, str]]:
        '''
        Return a set of Lyterati records from XML files (subset or all). The @name attribute of each <field> element is mapped to a key of the included dict. 
        '''
        source_files = list(Path(file_path).glob('*.xml'))
        if subset:
            source_files = [ file for file in source_files if file.name in [f['file_name'] for f in cls.FILE_MAP[subset]] ]
        records = []
        index = 0 # unique index for each record
        for file in source_files:
            doc = etree.parse(file, parser=Lyterati.parser)
            rows = doc.xpath(f'//row[field/@name="{cls.ID_FIELD}" and field/text() = "{user_id}"]')
            for row in rows:
                fields = row.xpath("field")
                row_dict = { f.get("name") if (f.get("name") != cls.ID_FIELD) else 'user_id': f.text for f in fields }
                row_dict.update({"file_name": file.name, 
                                 '_index': index})
                index += 1
                records.append(row_dict)
        return records


class LyteratiMapping:
    
    # mappinng fields from Lyterati to ORCiD
    LYTERATI_FIELD_MAPPING = { field: 'journal_title' for field in [ 'publisher',
                                                                'publication_venue', 
                                                                'conference', 
                                                                'event', 
                                                                'distributor' ]}
   
    LYTERATI_TYPE_MAPPING = { 'acad_articles': 'journal-article',
                             'articles': 'journal-article',
                             'article_abstracts': 'journal-article',
                             'books': 'book',
                             'chapters': 'book-chapter',
                             'conf_papers': 'conference-paper',
                             'presentations': 'lecture-speech',
                             'reports': 'report',
                             'reviews': 'book-review' }
        
    @staticmethod
    def split_authors(authors_str: str) -> list[str]:
        # Split the input string by comma, &, with, and
        # Return empty string for None
        if not authors_str:
            return ''
        authors = re.split(r',|&|\s+with\s+|\(\s*with\s+|\s+and\s+|\(and\s+', authors_str, flags=re.IGNORECASE)
        # Strip leading/trailing whitespace from each name and convert to title case if necessary
        return [author.strip().replace(')', '') for author in authors if author.strip()]
    
    def to_orcid_work(self, lyterati_work: dict[str, str], user_name: dict[str, str], orcid: str) -> dict[str, str]:
        '''Maps data from Lyterati fields  to fields used by the ORCiDWork class
        :param user_name: the Lyterati user's name
        :param orcid: the Lyterati user's ORCiD '''
        orcid_work = {}
        orcid_date = ORCiDFuzzyDate()
        for field, value in lyterati_work.items():
            match field:
                case 'file_name':
                    orcid_work['_type'] = self.LYTERATI_TYPE_MAPPING[value.replace('fis_', '').replace('.xml', '')]
                case 'authors':
                    contributors =  [ORCiDContributor(name) for name in self.split_authors(value) ]
                    orcid_work['contributors'] = contributors
                case lyterati_field if lyterati_field in self.LYTERATI_FIELD_MAPPING.keys():
                    orcid_field = self.LYTERATI_FIELD_MAPPING[lyterati_field]
                    orcid_work[orcid_field] = value
                case 'start_month' | 'start_year':
                    key = field.split('_')[1]
                    setattr(orcid_date, f'_{key}', value)
                case 'user_id':
                    continue
                case _:
                    orcid_work[field] = value
        # if the Lyterati field is empty, populate with just the Lyterati user's name
        if not orcid_work.get('contributors'):
            orcid_work['contributors'] = [ORCiDContributor(credit_name=f'{user_name["first_name"]} {user_name["last_name"]}',
                                           contributor_orcid=orcid)]
        orcid_work['publication_date'] = orcid_date
        return orcid_work