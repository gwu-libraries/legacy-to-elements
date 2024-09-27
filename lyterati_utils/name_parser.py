from __future__ import annotations
from lark import Lark, Tree, Transformer, v_args, Token
from lark.visitors import Transformer_InPlace
from lark.exceptions import UnexpectedCharacters, UnexpectedEOF
from typing import Iterator, Optional
from .author_grammar import AUTHOR_GRAMMAR
import regex

# Retain capitalization of these strings
ACRONYMS = r'MPH|DO|MD|FACP|MS|III|DNP|LCSW|MPP|EPFL'

# If the name contains one of these, don't treat it as a person name
STOP_WORDS = ['Association', 'Institute', 'Director', 'Department', 'Faculty', 'Student', 'Research', 'Laboratory', 'Panel', 'Group', 'Inc', 'University', 'Organization', 'Foundation', 'Office', 'Medical', 'Health', 'System', 'Council', 'Fund', 'Club', 'USDA', 'Network', 'Philanthropy', 'Center', 'Librarian', 'Clinic', 'Government', 'Community', 'Practitioners', 'Services', 'Academic', 'Repository', 'Students', 'Members', 'School']

TITLES = r'Professor|Dr\.?|Dean'

SUFFIXES = r'Jr\.?|III'

def score(tree: Tree) -> int:
    '''
    Scores an option by how many children (and grand-children, and
    grand-grand-children, ...) it has.
    This means that the option with fewer large terminals gets selected
    '''
    return sum(len(t.children) for t in tree.iter_subtrees())

class RemoveAmbiguities(Transformer_InPlace):
    '''
    Selects an option to resolve an ambiguity using the score function above.
    Scores each option and selects the one with the higher score, e.g. the one
    with more nodes.
    '''
    def _ambig(self, options):
        return max(options, key=score)

class Author:
    '''A class corresponding to a single parsed author name.
    The name_type, derived from the aliases in the grammar, indicates whether the name is 1) in full or initials form, and 2) in regular or last-first order'''
    def __init__(self, name_type: str):
        name_type = name_type.split('_')
        self.type = name_type[1]
        self.last_first = len(name_type) > 2
        self.first_name = []
        self.initials = []
        self.last_name = []

    @property
    def name(self) -> str:
        '''Returns the name as a string, in first-name-first order'''
        return f'{" ".join(self.first_name)}|{"".join(self.initials)}|{" ".join(self.last_name)}'
    
    @name.setter
    def name(self, value):
        # lfo, second name seen
        if self.last_name:
            self.first_name = value
        # lfo, first name seen
        elif self.last_first:
            self.last_name = value
        # second name seen (or first name after initials), first-name first
        elif self.first_name or self.initials:
            self.last_name = value
        # first name seen, first-name-first
        else:
            self.first_name = value

    def add_name(self, tree: Tree):
        match tree.data:
            # each subtree corresponding to a name element has a token as its data attribute and other tokens as its children
            case Token(type='RULE', value='author_name'):
                self.name = [t.value for t in tree.children if t]
            case Token(type='RULE', value='initials'):
                self.initials = [t.value for t in tree.children if t and t.value]

    @classmethod
    def unpack_tree(cls, tree: Tree) -> list[Author]:
        '''Given a Tree object from the PyLark parser, unpack the tree into one or more instances of the Author class defined above.'''
        authors = []
        # Navigate the tree from the top, so that we can group the terminals by higher units
        for subtree in tree.iter_subtrees_topdown():
            # nodes with an alias will not have a Token instance as their data
            if not isinstance(subtree.data, Token):
                author = Author(subtree.data)
                authors.append(author)
            else:
                # Skip the initial nodes (groupings of authors)
                if not authors:
                    continue
            author.add_name(subtree)
        return authors

class AuthorParser:
    '''
    Implements the Lark parser with the associated grammar. 
    '''

    def __init__(self, pre_clean: bool=True):
        '''
        :param pre_clean: whether to perform pre-parsing steps on the string (removes extraneous punctuation, title cases words in all caps, etc.)
        '''

        self.parser = Lark(AUTHOR_GRAMMAR, start='authors', ambiguity='explicit', regex=True)
        self.errors = []
        self.parsed = []
        self.stop_words = set(STOP_WORDS)
        self.titles = regex.compile(TITLES)
        self.suffixes = regex.compile(SUFFIXES)

        self.pre_clean = pre_clean
        if pre_clean:
            self.punct = regex.compile(r'[.,;:]$')
            self.acronyms = regex.compile(ACRONYMS)
            self.capital_names = regex.compile(r'[\p{Lu}]{4,}')

    
    def _pre_clean(self, names: str) -> str:
        '''Performs basic string cleaning before parsing.'''
        # Remove trailing punctuation
        if self.punct.search(names):
            names = names[:-1]
        for name in self.capital_names.findall(names):
            if not self.acronyms.match(name):
                names = names.replace(name, name.title())
        return names

    def _post_clean(self, authors: list[Author]) -> list[Author]:
        '''Does post-parsing cleanup, including merging names for corporate entities into the last_name field'''
        for i, author in enumerate(authors):
            # Check for corporate author
            if (self.stop_words & set(author.last_name)) or  (self.stop_words & set(author.first_name)):
                authors[i].last_name = authors[i].first_name + [''.join(authors[i].initials)] + authors[i].last_name
                # Remove empty initials slot
                authors[i].last_name = [n for n in authors[i].last_name if n]
                authors[i].first_name = []
                authors[i].initials = []
                continue
            # Check for initial titles
            # If we have only title and last name, the name will be blank
            if author.first_name and self.titles.match(author.first_name[0]):
                authors[i].first_name.pop(0)
                if not authors[i].first_name and not author.initials:
                    authors[i].last_name = []
                    continue
            # Remove periods from initials
            authors[i].initials = [initial.replace('.', '') for initial in author.initials]
            # Check for cases where the name suffix has been parsed as the last name
            if (len(authors[i].last_name) == 1) and (self.suffixes.match(authors[i].last_name[0])):
                # Assume the last name has been treated as the first name by the parser
                authors[i].last_name = authors[i].first_name + authors[i].last_name
                authors[i].first_name = []
        return [a for a in authors if a.last_name]


    def parse_one(self, names: str) -> tuple[Optional[list[Author]], Optional[dict[str, str]]]:
        '''
        Given a representing multiple author names, parse the names using the associated grammar. Names are returned as instances of the Author class. Unparsed strings, with errors, are returned as well.
        '''
        if self.pre_clean:
            names = self._pre_clean(names)
        try:
            tree = self.parser.parse(names.strip())
            tree = RemoveAmbiguities().transform(tree)
            return Author.unpack_tree(tree), None
        except (UnexpectedCharacters, UnexpectedEOF) as e:
            return None, { 'error': str(e) }

    def parse_many(self, list_of_names: list[str]) -> Iterator[dict[int, list[Author]]]:
        '''
        Given a list of strings representing multiple author names, parse the names using the associated grammar. Names are emitted as instances of the Author class, one list per string. Unparsed strings, with errors, are stored on the class instance. Keys of the return dictionary correspond to the index of the string in the original list.
        '''
        for i, names in enumerate(list_of_names):
            result, error = self.parse_one(names)
            if result:
                yield {i: self._post_clean(result)}
            else:
                error.update({'index': i})
                self.errors.append(error)
           
