from random import choice
from dataclasses import dataclass
from collections import namedtuple as nt


magic_str = '__URL'


def strip_url_flag(s):
    return s.replace(magic_str, '')


def get_proxy(filename):
    with open(filename, 'r') as f:
        proxy_list = f.readlines()
    return {'http': choice(proxy_list)}


def strip_filename(url):
    return url[url.rfind('/') + 1:]


def contains_url_flag(s):
    return magic_str in s


def strip_url_flag(s):
    return s.replace(magic_str, '')
