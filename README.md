# Code for migrating Lyterati data to Elements

The purpose of the scripts in this repo is to prepare Lyterati data for import into Elements, using the Elements mapping tool. 

## Using the scripts

The `data_migrator.py` script is the entry point for this process. It is designed to be run at the command line (within a suitably configured Python environment -- see `reuirements.txt`) and has a few sub-commands, which can be used in the following sequence:

- `prep-lyterati-reports` uses faculty profiles from the Lyterati-Expert-Finder XML feed as a source for faculty identifiers, which it merges with CSV files of faculty activities (research, service, teaching) exported from the Lyterati reporting interface. Multiple CSV files of the same category (e.g., research) will be concatenated into a single CSV for mapping and import into Elements. This command also outputs a file of faculty names and affiliations for which an identifier in the profile data could not be located.

- `add-missing-ids` supports merging a CSV file with faculty names, affiliations, and identifiers with the output of the previous command, in order to fill gaps where identifiers could not be retrieved from the Lyterati profiles.

- `make-import-files` takes an aggregate CSV file of Lyterati data with user identifiers and a mapping file from the Elements mapping tool and generates the three  files required for import into Elements:
  - `metadata.csv`: One row per object, containing object metadata from Lyterati as mapped to Elements fields.
  - `persons.csv`: One row per person extracted from a contributor field (e.g., `authors` or `co-contributors`), with a mapping to the object associated with that entry.
  - `links.csv`: One row per object, establishing the link between the object and the user.

## Configuration & data organization

- There is one mapping CSV per category of Elements object: publications, activities (professional activities), and teaching activities. These reside in `data/to-migrate/mapping` and have been included in the repo. These files reflect the decisions made by the GW implementation team for mapping each type of record in Lyterati to a corresponding category/type in Elements.
- Additionally, one or more "Choice Lists" (in Excel format) may be included in `data/to-migrate/mapping`. These constrain the available choices for given Elements fields when the Elements field type is `choice`. (See the Elements mapping documentation for more information about field types.)
- Various details of configuration are set in `migration-config.yml`, including the paths to mapping files, choice lists, and other inputs/outputs of the script. 
- Each subcommand of `data_migrator.py` accepts some command-line arguments, as documented in the script. 



### Note on this repo

I've moved code here from our [orcid-integration](https://github.com/gwu-libraries/orcid-integration) repo, because this project is at least technically, if not conceptually, distinct.

Some of the code here reflects our efforts to map Lyterati data to the ORCiD API schema, which is no longer in scope. (We'll instead aim to use the Elements API to populate ORCiD profiles with data that Elements itself doesn't push to ORCiD; this work can reside in the `orcid-integration` repo, since it would likely be implemented as part of that integration.)

