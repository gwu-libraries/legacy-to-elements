# Code for migrating Lyterati data to Elements

I've moved code here from our [orcid-integration](https://github.com/gwu-libraries/orcid-integration) repo, because this project is at least technically, if not conceptually, distinct.

Some of the code here reflects our efforts to map Lyterati data to the ORCiD API schema, which is no longer in scope. (We'll instead aim to use the Elements API to populate ORCiD profiles with data that Elements itself doesn't push to ORCiD; this work can reside in the `orcid-integration` repo, since it would likely be implemented as part of that integration.)

### Relevant scripts and status

| Task | File(s) | Test Coverage | Status | To Do |
| ---- | ------- | ------------- | ------ | ----- | 
| Parse co-author strings | name_parser.py, author-grammer.txt | tests/test_author_parsing.py | Mostly complete | Handle more edge cases |
| Extract Lyterati XML data | lyterati_utils.py | tests/test_data_prep.py (needs work) | In progress, pending XML example from Deloitte | Refactor to use Elements field mapping & fields from expanded Lyerati XML |
| Extract DOI's from Lyterati data | | | In progress | Implement as script, write tests | 
| Find author matches on OpenAlex | external_sources.py | tests/test_data_prep.py (needs work) | Not started, but existing code could be repurposed | Implement, write tests | 