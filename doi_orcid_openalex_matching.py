import requests
import csv
import pandas as pd
import time

OPENALEX_API_URL = "https://api.openalex.org"
INPUT_CSV = "input.csv"
OUTPUT_CSV = "output.csv"
SLEEP_TIME = 1

df = pd.read_csv(INPUT_CSV)

# Script for taking a CSV of author names and DOIs of works they are associated with
# and then querying OpenAlex to retrieve the author records associated with the DOI
# and comparing name records from the input CSV to retrieved author record names
# and assigning a score based on name similarity

# SLEEP_TIME is the time between API requests to OpenAlex


def write_header_row():
    with open(OUTPUT_CSV, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        header_row = [
            "college_name",
            "department_name",
            "last_name",
            "first_name",
            "middle_name",
            "research_heading",
            "heading_type",
            "contribution_year",
            "title",
            "authors",
            "publication_name",
            "additional_details",
            "url",
            "school_code",
            "report_code",
            "category",
            "gw_id",
            "doi",
            "best_match_name_score",
            "best_match_orcid",
        ]
        writer.writerow(header_row)


def get_authors_from_open_alex_by_doi(doi):
    time.sleep(SLEEP_TIME)
    response = requests.get(f"{OPENALEX_API_URL}/works/doi:{doi}")
    if response.status_code == 200:
        data = response.json()
        author_list = []
        for authorship in data["authorships"]:
            author_list.append(
                {
                    "orcid": authorship["author"]["orcid"],
                    "display_name": authorship["author"]["display_name"],
                }
            )
        return author_list
    else:
        return []


def name_similarity_score(display_name, first_name, last_name, middle_name):
    score = 0
    split_display_name = display_name.split(" ")
    if last_name is not None and last_name in split_display_name:
        score += 50
    if first_name is not None and first_name in split_display_name:
        score += 30
    if middle_name is not None and middle_name in split_display_name:
        score += 10
    return score


write_header_row()

with open(OUTPUT_CSV, "a", newline="") as file:
    writer = csv.writer(file)
    for index, row in df.iterrows():
        authors_list = get_authors_from_open_alex_by_doi(row.doi)
        if authors_list != []:
            for author in authors_list:
                author["name_score"] = name_similarity_score(
                    author["display_name"],
                    row.first_name,
                    row.last_name,
                    row.middle_name,
                )
            best_match = max(authors_list, key=lambda x: x["name_score"])
        else:
            best_match = {"name_score": 0, "orcid": "Not found"}

        new_row = [
            row.college_name,
            row.department_name,
            row.last_name,
            row.first_name,
            row.middle_name,
            row.research_heading,
            row.heading_type,
            row.contribution_year,
            row.title,
            row.authors,
            row.publication_name,
            row.additional_details,
            row.url,
            row.school_code,
            row.report_code,
            row.category,
            row.gw_id,
            row.doi,
            best_match["name_score"],
            best_match["orcid"],
        ]
        writer.writerow(new_row)
