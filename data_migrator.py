from typing import Optional, Union
import pandas as pd
from pandas import DataFrame
import click 
import json
from pathlib import Path
from lxml import etree
import logging
from logging import getLogger
from datetime import datetime
from lyterati_utils.doi_parser import Parser
from lyterati_utils.name_parser import AuthorParser
import re
from lyterati_utils.elements_types import SourceHeading, ElementsObjectID, ElementsMapping
import yaml


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
warnings_logger = getLogger("py.warnings")
logging.captureWarnings(True)


with open('./migration-config.yml') as f:
    CONFIG = yaml.load(f, Loader=yaml.FullLoader)

def load_ids_from_profiles() -> DataFrame:
    '''
    Loads data from the fields in PROFILE_FIELDS for each row in the file LYTERATI_PROFILE_XML. Returns as a DataFrame, one row per user record.
    '''
    parser = etree.XMLParser(recover=True)
    doc = etree.parse(CONFIG['id_source'], parser=parser)
    rows = doc.xpath('//row')
    records = [row.xpath('field') for row in rows]
    records = [ { field.get('name'): field.text for field in record
                if field.get('name') in CONFIG['profile_fields'] }  
              for record in records ]
    logger.info(f'Found {len(records)} records in {CONFIG["id_source"]}')
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
    return df.drop_duplicates() # Remove exact duplicate entries within each report

def load_reports(path_to_lyterati_files: str, map_report_types: bool=True, exclude: Optional[list]=None) -> DataFrame:
    '''Given a path to a directory containing Lyterati reports, which may be in CSV or Excel format, it will load either all files of those formats, or only those whose names not in the optional exclude list. If the map_report_types argument is supplied,the LYTERATI_TYPE_MAPPING will be used to add the report category as an additional column. All files are concatenated into a single DataFrame.'''
    path_to_lyterati_files = Path(path_to_lyterati_files)
    reports = pd.DataFrame()
    if map_report_types:
        report_type_mapping = load_mapping()
    else:
        report_type_mapping = {}
    for file in list(path_to_lyterati_files.glob('*.xlsx')) + list(path_to_lyterati_files.glob('*.csv')):
        if file.stem.startswith('_'):
            continue # skip files that start with an underscore
        # Skip files that match values passed in with the --exclude cli option
        if not exclude or not [f for f in exclude if f.lower() in file.stem.lower()]:
            df = load_lyterati_report(str(file))
            if report_type_mapping:
                try:
                    df['category'] = df.report_code.apply(lambda x: report_type_mapping[x])
                except KeyError as e:
                    logger.error(f'Unable to map report type for {file}. Not such category {str(e)} in {CONFIG["lyterati_type_mapping"]}')
                    return pd.DataFrame()
            reports = pd.concat([reports, df])
    return reports

def merge_ids_with_reports(reports: DataFrame, ids: DataFrame) -> DataFrame:
    '''It is assumed that both arguments will have the columns in common listed in MERGED_FIELDS. Reports are joined to ids with a LEFT JOIN, leaving nulls for the identifier field where no match is found. If column_map is provided, it will be applied to ids before merging.'''
    if CONFIG.get('profile_field_map'):
        ids = ids.rename(columns=CONFIG['profile_field_map'])
    merged = reports.merge(ids, on=CONFIG['merge_fields'], how='left')
    logger.info(f'Merged {len(merged)} records with profiles. {len(merged.loc[~merged[CONFIG["profile_id_field"]].isnull()])} matches found.')
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
            # Drop null columns
            df = df.dropna(axis=1, how='all')
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
    profile_id_field, merge_fields = CONFIG['profile_id_field'], CONFIG['merge_fields']
    missing_ids = reports.loc[reports[profile_id_field].isnull()][merge_fields].drop_duplicates()
    missing_ids.to_csv(Path(path_to_save_file) / 'missing_ids.csv', index=False)

def load_mapping() -> dict[str, list[str]]:
    '''Loads the JSON mapping from LYTERATI_TYPE_MAPPING, which specifies the category to which each Lyterati report type belongs. The mapping is presumed to reside in the same directory as the reports to be processed.'''
    with open(CONFIG['lyterati_type_mapping']) as f:
        mapping = json.load(f)
        return { _type: category for category, list_of_types in mapping.items() 
                for _type in list_of_types }

def update_ids(reports: DataFrame, path_to_id_map: str) -> DataFrame:
    '''Given a DataFrame representing Lyterati reports, and a path to an additional file (CSV or Excel) that contains missing ID's mapped to the MERGE_FIELDS columns in the reports DataFrame, add those ID's to the DataFrame.'''
    pid = CONFIG['profile_id_field']
    if path_to_id_map.endswith('csv'):
        missing_ids = pd.read_csv(path_to_id_map)
    else:
        missing_ids = pd.read_excel(path_to_id_map)
    # Identify the column that contains GWIDs
    gwid_re = re.compile(r'G[0-9]{8}')
    for c in missing_ids.columns:
        if missing_ids[c].str.match(gwid_re).all():
            missing_ids = missing_ids.rename(columns={c: pid})
            break
    if pid not in missing_ids.columns:
        logger.error(f'File {path_to_id_map} should contain a column of GWIDs, but no such column was found. Please correct the file and run the script again.')
        return
    matched = reports.merge(missing_ids, on=[c for c in missing_ids.columns if c != pid], how='left')
    matched[f'{pid}_x'] = matched[f'{pid}_x'].fillna(matched[f'{pid}_y'])
    matched = matched.rename(columns={f'{pid}_x': f'{pid}'}).drop(columns=f'{pid}_y')
    num_missing = len(matched.loc[matched[pid].isnull()])
    if num_missing > 0:
        logger.warn(f'After merge, {num_missing} missing IDs remain.')
    return matched

def process_for_elements(df: DataFrame, category: str) -> list[Union[list[dict[str, str]], DataFrame]]:
    '''df should be the single DataFrame containing the merged Lyterati reports for import. Returns three lists of dicts: metadata, persons, and linking data, for constructing the import files, as well as the original DataFrame, with the list of object IDs appended as column.'''
    elements_category = category.category
    concat_fields = CONFIG['concat_fields'][elements_category]
    path = Path(CONFIG['object_id_store'])
    if not path.exists():
        path.touch()
        minter = ElementsObjectID()
        minter.path_to_id_store = path
    else:
        minter = ElementsObjectID(path)
    parser = AuthorParser()
    user_author_mapping = CONFIG['user_author_mapping'] if elements_category in CONFIG['user_author_mapping']['included_in'] else None
    object_privacy = CONFIG.get('object_privacy', {}).get(elements_category)
    doi_fields = CONFIG['doi_fields'] if elements_category == 'publication' else None
    blank_end_dates = CONFIG['blank_end_dates']
    mapper = ElementsMapping(path_to_mapping=CONFIG['mapping'][elements_category], 
                             minter=minter,
                             parser=parser,
                             user_id_field=CONFIG['profile_id_field'],
                             path_to_choice_lists=CONFIG['choice_lists'].get(elements_category),
                             concat_fields=concat_fields, 
                             user_author_mapping=user_author_mapping,
                             doi_fields=doi_fields,
                             object_privacy=object_privacy,
                             blank_end_dates=blank_end_dates)

    metadata_rows = []
    linking_rows = []
    persons_rows = []
    object_ids = []
    for row in df.itertuples(index=False):
        elements_row = mapper.make_mapped_row(row, map_type=category)
        if not elements_row:
            object_ids.append(None)
            continue
        metadata_rows.append(dict(elements_row))
        linking_rows.append(elements_row.link)
        # Temporary hack: skipping author parsing for publications
        if elements_category != 'publication':
            persons_rows.extend(list(elements_row.persons))
        object_ids.append(elements_row.id)
    minter.persist_ids()
    df['elements_id'] = object_ids
    return metadata_rows, linking_rows, persons_rows, df
    
@click.group()
def cli():
    pass

@cli.command()
@click.option('--data-source', required=True) # Should be a single CSV containing the aggregated records for this category
@click.option('--category', type=click.Choice(['service', 'research', 'teaching'], case_sensitive=False), default='service') # As present in Lyterati -- the config YAML file determines how these are mapped to Elements object categories
def make_import_files(data_source, category):
    data = pd.read_csv(data_source)
    output_dir = Path(CONFIG['output_dir'])
    category = SourceHeading[category.upper()]
    processed = process_for_elements(data, category)
    for name, output in zip(['metadata', 'linking', 'persons'], processed[:3]):
        if output:
            df = pd.DataFrame.from_records(output)
            label = {'publication': 'publications', 'activity': 'activities', 'teaching-activity': 'teaching-activities'}.get(category.category)
            df.to_csv(output_dir / f'{label}-{name}.csv', index=False)
    # Write original with object ID's for cross-reference
    data_source_path = Path(data_source).parents[0]
    file_name = Path(data_source).stem
    processed[3].to_csv(data_source_path / f'{file_name}_migrated.csv', index=False)


@cli.command()
@click.option('--id-source', default='./data/to-migrate/missing_ids.csv') # File with ID's missing from the output of prep_lyterati_reports
@click.option('--data-source', required=True) # Output of prep_lyterati_reports: should be a single CSV
def add_missing_ids(id_source, data_source):
    '''Adds IDs from the id-source to the data-source, matching on columns defined in the constant MERGE_FIELDS. Result is saved to the original file specified by data-source.'''
    reports = pd.read_csv(data_source)
    reports = update_ids(reports, id_source)
    reports.to_csv(data_source, index=False)

@cli.command()
@click.option('--data-source', default='./data/lyterati-exports') # Should be a folder containing one or more CSV files, which will be aggregated and split according to the top-level categories in Lyterati (research, service, teaching)
@click.option('--target', default='./data/to-migrate') 
@click.option('--exclude', '-e', multiple=True) # One or more Lyterati record types to exclude, such as Grants or Work in Progress
def prep_lyterati_reports(data_source, target, exclude):
    '''Values passed to --exclude/-e should correspond to the part of the filename designating either a school or a type of report. For instance, -e grants will exclude all files with "grants" or "Grants" in the title. Matching is case-insensitive.'''
    ids = load_ids_from_profiles()
    reports = load_reports(data_source, exclude=exclude)
    if reports.empty:
        logger.error('Unable to finish processing reports. Please fix the errors flagged in the log.')
        exit()    
    reports = merge_ids_with_reports(reports, ids)
    save_reports(reports, target)

if __name__ == '__main__':
    cli()
    

