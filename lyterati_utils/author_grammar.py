AUTHOR_GRAMMAR = r'''
authors: single_author
  | multiple_authors

multiple_authors: single_author "," (_WHITESPACE? single_author  ",")* _WHITESPACE? single_author 
  | single_author  ";" (_WHITESPACE? single_author ";")* _WHITESPACE? single_author 
  | single_author ("," _WHITESPACE? single_author )* ","? _WHITESPACE ("and"|"with"|"And"|"With"|"&") _WHITESPACE single_author 


single_author: author_name [_WHITESPACE initials] _WHITESPACE author_name degrees? -> an_full
  | author_name "," _WHITESPACE? author_name [_WHITESPACE initials] degrees? -> an_full_lfo
  | initials _WHITESPACE author_name degrees? -> an_init
  | author_name ("," | _WHITESPACE) initials degrees? -> an_init_lfo

author_name: NAME_PART (_WHITESPACE NAME_PART)* [","? _WHITESPACE SUFFIX]

degrees: ("," | _WHITESPACE) DEGREE ("," _WHITESPACE? DEGREE)*

initials: INITIAL [INITIAL INITIAL?]

INITIAL: /[A-Z]\.?/

SUFFIX: /Jr\.?|III/

NAME_PART: /((El|Von|De|Del|de|von|del|el|of) [\p{Lu}][\p{Ll}']+)|([\p{Lu}]([\p{Ll}]+[\p{Lu}])?[\p{Ll}']+(-[\p{Lu}]([\p{Ll}]+[\p{Lu}])?[\p{Ll}']+)?)/

DEGREE: /MPH|DO|MD|MEd|FACP|MScPT|EdD|MS|PhD|MPP|DN|LCSW|EPFL/

_WHITESPACE: /[ ]+/

%ignore _WHITESPACE
'''