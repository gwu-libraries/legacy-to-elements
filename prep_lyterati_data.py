from typing import Optional
import pandas as pd
from pandas import DataFrame
import click 
import json
from pathlib import Path
from lxml import etree
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

LYTERATI_TYPE_MAPPING = 'category-mapping.json'

LYTERATI_PROFILE_XML = 'fis_faculty.xml'

PROFILE_FIELDS = ['first_name', 'middle_name', 'last_name', 'home_college', 'home_department', 'gw_id']

PROFILE_FIELD_MAP = { 'home_college': 'college_name', 
                     'home_department': 'department_name' }

PROFILE_ID_FIELD = 'gw_id'

MERGE_FIELDS = ['first_name', 'last_name', 'college_name', 'department_name']

def load_ids_from_profiles(path_to_profile_xml: str) -> DataFrame:
    '''
    Loads data from the fields in PROFILE_FIELDS for each row in the file LYTERATI_PROFILE_XML. Returns as a DataFrame, one row per user record.
    '''
    parser = etree.XMLParser(recover=True)
    if not path_to_profile_xml.endswith('xml'):
        path_to_profile_xml = Path(path_to_profile_xml) / LYTERATI_PROFILE_XML
    doc = etree.parse(path_to_profile_xml, parser=parser)
    rows = doc.xpath('//row')
    records = [row.xpath('field') for row in rows]
    records = [ { field.get('name'): field.text for field in record
                if field.get('name') in PROFILE_FIELDS }  
              for record in records ]
    logger.info(f'Found {len(records)} records in {path_to_profile_xml}')
    return DataFrame.from_records(records)

def load_lyterati_report(path_to_lyterati_file: str) -> DataFrame:
    '''Loads a CSV or Excel file containing one set of Lyterati records. File names are expected to conform to the format: "{SCHOOL CODE} {Report Type}. Adds the school code and report type as columns to the returned DataFrame. It is assumed that an Excel file will contain all the data on a single sheet/tab.'''
    logger.info(f'Loading data from {path_to_lyterati_file}')
    if path_to_lyterati_file.endswith('xlsx'):
        df = pd.read_excel(path_to_lyterati_file)
    else:
        df = pd.read_csv(path_to_lyterati_file)
    school_code, report_code = Path(path_to_lyterati_file).stem.split(maxsplit=1) # Split on the first space only
    df['school_code'] = school_code
    df['report_code'] = report_code
    df.columns = [c.lower().replace(' ', '_') for c in df.columns] # normalize column name formatting
    return df

def load_reports(path_to_lyterati_files: str, map_report_types: bool=True, subset: Optional[list]=None) -> DataFrame:
    '''Given a path to a directory containing Lyterati reports, which may be in CSV or Excel format, it will load either all files of those formats, or only those whose names are provided in the optional subset list. If the max_report_types argument is supplied,the LYTERATI_TYPE_MAPPING will be used to add the report category as an additional column. All files are concatenated into a single DataFrame.'''
    path_to_lyterati_files = Path(path_to_lyterati_files)
    reports = pd.DataFrame()
    if map_report_types:
        report_type_mapping = load_mapping(path_to_lyterati_files)
    else:
        report_type_mapping = {}
    for file in list(path_to_lyterati_files.glob('*.xlsx')) + list(path_to_lyterati_files.glob('*.csv')):
        if file.stem.startswith('_'):
            continue # skip files that start with an underscore
        if not subset or file.stem in subset:
            df = load_lyterati_report(str(file))
            if report_type_mapping:
                try:
                    df['category'] = df.report_code.apply(lambda x: report_type_mapping[x])
                except KeyError as e:
                    logger.error(f'Unable to map report type for {file}. Not such category {str(e)} in {LYTERATI_TYPE_MAPPING}')
                    return pd.DataFrame()
            reports = pd.concat([reports, df])
    return reports

def merge_ids_with_reports(reports: DataFrame, ids: DataFrame, column_map: Optional[dict[str, str]]=None) -> DataFrame:
    '''It is assumed that both arguments will have the columns in common listed in MERGED_FIELDS. Reports are joined to ids with a LEFT JOIN, leaving nulls for the identifier field where no match is found. If column_map is provided, it will be applied to ids before merging.'''
    if column_map:
        ids = ids.rename(columns=column_map)
    merged = reports.merge(ids, on=MERGE_FIELDS, how='left')
    logger.info(f'Merged {len(merged)} records with profiles. {len(merged.loc[~merged[PROFILE_ID_FIELD].isnull()])} matches found.')
    if len(merged) > len(reports):
        logger.warning(f'Merging has created duplicates.{len(merged) - len(reports)} are potential duplicates.')
    return merged

def save_reports(reports: DataFrame, path_to_save_reports: str, by_category: bool=True): 
    '''Saves the merged reports to the provided path. If by_category is True, reports will be divided by category, presumed to be the values of the given dictionary (and corresponding to the top-level categories in LYTERATI_TYPE_MAPPING).'''
    path_to_save_reports = Path(path_to_save_reports)
    ts = datetime.now().strftime('%Y-%m-%d')
    extract_non_matches(reports, path_to_save_reports)
    if by_category:
        for category in reports.category.unique():
            df = reports.loc[reports.category == category]
            file = path_to_save_reports / f'lyterati_data_for_{category}_{ts}.csv'
            logger.info(f'Saving report to {file}')
            df.to_csv(file, index=False)
            generate_stats(df, path_to_save_reports, category)
    else:
        file = path_to_save_reports / f'lyterati_data_{ts}.csv'
        logger.info(f'Saving reports to {file}')
        reports.to_csv(file, index=False)
        generate_stats(reports, path_to_save_reports)

def generate_stats(reports: DataFrame, path_to_save_stats: str, category: str=None):
    '''Generates and saves to CSV basic info per school on non-matched users, unique users, and number of records per type.'''
    grouped = reports.groupby('school_code')
    def apply_stats(df):
        df['unique_id'] = df.first_name + df.last_name + df.department_name
        df['missing_users'] = len(df.unique_id.unique()) - len(df.gw_id.unique())
        df['unique_users'] = len(df.unique_id.unique())
        return df.groupby(['missing_users', 'unique_users', 'report_code']).college_name.count()
    stats_df = grouped.apply(apply_stats).unstack()
    path_to_save_stats = Path(path_to_save_stats)
    if category:
        filename = f'lyterati_data_{category}_stats.csv'
    else:
        filename = f'lyterati_data_stats.csv'
    stats_df.to_csv(path_to_save_stats / filename)

def extract_non_matches(reports: DataFrame, path_to_save_file: str):
    '''Generates a CSV of those names and affiliations for which a match on profile data could not be found.'''
    missing_ids = reports.loc[reports[PROFILE_ID_FIELD].isnull()][MERGE_FIELDS].drop_duplicates()
    missing_ids.to_csv(Path(path_to_save_file) / 'missing_ids.csv', index=False)

def load_mapping(path_to_lyterati_reports: str) -> dict[str, list[str]]:
    '''Loads the JSON mapping from LYTERATI_TYPE_MAPPING, which specifies the category to which each Lyterati report type belongs. The mapping is presumed to reside in the same directory as the reports to be processed.'''
    with open(Path(path_to_lyterati_reports) / LYTERATI_TYPE_MAPPING) as f:
        mapping = json.load(f)
        return { _type: category for category, list_of_types in mapping.items() 
                for _type in list_of_types }

if __name__ == '__main__':
    
    ids = load_ids_from_profiles('./data/lyterati-xml/expert-finder-feed')
    reports = load_reports('./data/lyterati-exports')
    if reports.empty:
        logger.error('Unable to finish processing reports. Please fix the errors flagged in the log.')
        exit()    
    reports = merge_ids_with_reports(reports, ids, PROFILE_FIELD_MAP)
    save_reports(reports, './data/to-migrate')

