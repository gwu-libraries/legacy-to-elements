import re
import openpyxl 
import string
import urllib
from typing import Optional

# CrossRef's recommended regular expression
CROSSREF_RE = re.compile(r'(10.\d{4,9}/[-._;()/:A-Z0-9]+)', re.IGNORECASE)
# PubMed ID regular expression; looks for label in text
PMID_RE = re.compile(r'(PMID: (\d{7,8}))\b')
# ISBN: from  O'Reilly Regular Expressions Cookbook, 2nd edition
ISBN_RE = re.compile(r'(?:ISBN(?:-13)?:?\ )?(?=[0-9]{13}|(?=(?:[0-9]+[-\ ]){4})[-\ 0-9]{17})(97[89][-\ ]?[0-9]{1,5}[-\ ]?[0-9]+[-\ ]?[0-9]+[-\ ]?[0-9])') 

class Parser:

    def __init__(self, txt: str, is_url: bool=False):
        # Clean up Excel characters
        self.txt = openpyxl.utils.escape.unescape(txt)
        if is_url:
            self.txt = urllib.parse.unquote(self.txt) # Unescape URL characters
        self.is_url = is_url

    def extract_dois(self) -> Optional[str]:
        '''Extracts text matching the CrossRef DOI pattern from a larger string. Attempts to catch certain edge cases.'''
        if match := CROSSREF_RE.search(self.txt):
            doi = match.group(1)
            doi = doi.rstrip(string.punctuation)
            # Sometimes the PMID label appears immediately following the DOI
            if doi.endswith('PMID'):
                doi = doi[:-4].rstrip(string.punctuation)
            # Edge cases where we can guess that the DOI does not extend to the end of the path
            elif self.is_url:
                doi = re.sub(r'/abstract.*$|/full.*$|/pdf.*$', '', doi)
            # Artefact of some Lyterati data capture? 
            elif doi.endswith('Date'): 
                return
            return doi
    
    @property
    def doi(self):
        return self.extract_dois()

    def extract_isbns(self) -> Optional[str]:
        '''Extracts ISBN's from a larger string. Takes the first ISBN when multiple ISBN's are present.'''
        if match := ISBN_RE.search(self.txt):
            isbn = match.group(1)
            return isbn
    
    @property
    def isbn(self):
        return self.extract_isbns()

    def extract_pmids(self) -> Optional[str]:
        '''Extracts text matching the PMID pattern from a larger string,'''
