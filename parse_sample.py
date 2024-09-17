from name_parser import AuthorParser
import json
from mimesis import Person
from mimesis.locales import Locale
import regex
import click
from string import ascii_lowercase, ascii_uppercase
from random import choice

@click.group()
def cli():
    pass

@click.command()
@click.option('--input', default='./author_strings.txt')
@click.option('--size', default=500)
@click.option('--output', default=None)
@click.option('--errors', default='./unparsed_names.json')
def parse_sample(input, size, output, errors):
    with open(input) as f:
        names = [line for line in f]
    parser = AuthorParser()
    names = names[:size] if size > 0 else names
    parsed_output = []
    for parsed in parser.parse_many(names):
        i, result = parsed.popitem()
        parsed_output.append({'index': i, 
                            'original': names[i].strip(),
                            'result': ';'.join([f'{i+1}_{author.name}' for i, author in enumerate(result)])
                            })
    with open(errors, 'w') as f:
        json.dump(parser.errors, f)
    if output:
        with open(output, 'w') as f:
            json.dump(parsed_output, f)

@click.command()
@click.argument('name_str')
def parse_string(name_str):
    parser = AuthorParser()
    result, error = parser.parse_one(name_str)
    if error:
        print(error)
    else:
        print(';'.join([f'{i+1}_{author.name}' for i, author in enumerate(result)]))

def replace_non_ascii(match):
    return ''.join([ choice(ascii_lowercase) if letter.islower() else choice(ascii_uppercase) for letter in match.group() ])

@click.command()
@click.argument('name_str')
def anonymize(name_str):
    # Identify Unicode alphabetic strings representing a name
    unicode_pattern = r"([\p{Lu}](?:[\p{Ll}]+[\p{Lu}])?[\p{Ll}']+)"
    person = Person(Locale.EN)
    for name in regex.findall(unicode_pattern, name_str):
        non_ascii = regex.search('[^A-Za-z]', name)
        if non_ascii:
            new_name = regex.sub('[A-Za-z]+',replace_non_ascii, name)
            name_str = name_str.replace(name, new_name)
        else:
            name_str = name_str.replace(name, person.name())
    print(name_str)

cli.add_command(parse_sample)
cli.add_command(anonymize)
cli.add_command(parse_string)

if __name__ == '__main__':
   cli() 