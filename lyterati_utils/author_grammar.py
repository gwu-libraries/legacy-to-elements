AUTHOR_GRAMMAR = r'''
authors. 1: single_author
  | multiple_authors

multiple_authors. 1: single_author "," (_WHITESPACE? single_author  ",")* _WHITESPACE? single_author 
  | single_author  ";" (_WHITESPACE? single_author ";")* _WHITESPACE? single_author 
  | single_author ("," _WHITESPACE? single_author )* ","? _WHITESPACE ("and"|"with"|"And"|"With"|"&") _WHITESPACE single_author 


single_author. 1: author_name [_WHITESPACE initials] _WHITESPACE author_name degrees? -> an_full
  | author_name "," _WHITESPACE? author_name [_WHITESPACE initials] degrees? -> an_full_lfo
  | initials _WHITESPACE author_name degrees? -> an_init
  | author_name ("," | _WHITESPACE) initials degrees? -> an_init_lfo

author_name. 1: NAME_PART (_WHITESPACE NAME_PART)* [","? _WHITESPACE SUFFIX]

degrees. 2: ("," | _WHITESPACE) DEGREE ("," _WHITESPACE? DEGREE)*

initials. 1: INITIAL [INITIAL INITIAL?]

INITIAL. 1: /[A-Z]\.?/

SUFFIX. 1: /Jr\.?|III/

NAME_PART. 1: /((El|Von|De|Del|de|von|del|el|of) [\p{Lu}][\p{Ll}']+)|([\p{Lu}]([\p{Ll}]+[\p{Lu}])?[\p{Ll}']+(-[\p{Lu}]([\p{Ll}]+[\p{Lu}])?[\p{Ll}']+)?)/

DEGREE. 1: /MPH|DO|MD|MEd|FACP|MScPT|EdD|MS|PhD|MPP|DN|LCSW|EPFL/

_WHITESPACE. 1: /[ ]+/

%ignore _WHITESPACE
'''