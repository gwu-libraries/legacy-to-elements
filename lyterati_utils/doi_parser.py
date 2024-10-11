import re
import openpyxl 
import string
import urllib
from typing import Optional
import pandas as pd

# CrossRef's recommended regular expression
CROSSREF_RE = re.compile(r'(10.\d{4,9}/[-._;()/:A-Z0-9]+)', re.IGNORECASE)
# PubMed ID regular expression; looks for label in text
PMID_RE = re.compile(r'(PMID: (\d{1,8}))\b')
# PubMedCentral ID 
PMCID_RE = re.compile(r'(PMC\d+)\b')
# ISBN: from  O'Reilly Regular Expressions Cookbook, 2nd edition
ISBN_RE = re.compile(r'(?:ISBN(?:-13)?:?\ )?(?=[0-9]{13}|(?=(?:[0-9]+[-\ ]){4})[-\ 0-9]{17})(97[89][-\ ]?[0-9]{1,5}[-\ ]?[0-9]+[-\ ]?[0-9]+[-\ ]?[0-9])') 

class Parser:

    @staticmethod
    def clean_xl_text(txt: str, is_url: bool) -> str:
        if pd.isna(txt) or not txt:
            return ''
        txt = openpyxl.utils.escape.unescape(txt)
        if is_url:
            txt = urllib.parse.unquote(txt) # Unescape URL characters
        return txt

    @staticmethod
    def extract_doi(txt: str, is_url: bool=False) -> Optional[str]:
        '''Extracts text matching the CrossRef DOI pattern from a larger string. Attempts to catch certain edge cases.'''
        txt = Parser.clean_xl_text(txt, is_url)
        if match := CROSSREF_RE.search(txt):
            doi = match.group(1)
            doi = doi.rstrip(string.punctuation)
            # Sometimes the PMID label appears immediately following the DOI
            if doi.endswith('PMID'):
                doi = doi[:-4].rstrip(string.punctuation)
            # Edge cases where we can guess that the DOI does not extend to the end of the path
            elif is_url:
                doi = re.sub(r'/abstract.*$|/full.*$|/pdf.*$', '', doi)
            # Artefact of some Lyterati data capture? 
            elif doi.endswith('Date'): 
                return
            return doi
        
    @staticmethod
    def extract_isbn(txt: str, is_url: bool=False) -> Optional[str]:
        '''Extracts ISBN's from a larger string. Takes the first ISBN when multiple ISBN's are present.'''
        txt = Parser.clean_xl_text(txt, is_url)
        if match := ISBN_RE.search(txt):
            isbn = match.group(1)
            return isbn

    @staticmethod   
    def extract_pmids(txt: str, is_url: bool=False) -> Optional[str]:
        '''Extracts text matching the PMID or PMC pattern from a larger string.'''
        txt = Parser.clean_xl_text(txt, is_url)
        pmid, pmc = PMID_RE.search(txt), PMCID_RE.search(txt)
        if pmid:
            pmid = pmid.group(1)
        if pmc:
            pmc = pmc.group(1)
        return pmid, pmc 