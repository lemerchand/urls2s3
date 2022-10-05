import pandas as pd
import argparse as argp
import boto3
import sys
import requests as rq
import shutil
from os import getpid
from io import BytesIO
from multiprocessing import Pool
from rich.console import Console
from datatypes import Status as status
import misc_functions as mf

# Setup rich console text
c = Console()

# Handle the argument parser
# Store args under 'args'
parser = argp.ArgumentParser()

parser.add_argument(
    'fn',
    help='The path to the .csv to be processed.'
    )

parser.add_argument(
    'bucket',
    help='The AWS S3 bucket you are transferring to.'
    )

parser.add_argument(
    '-f',
    '--folder-column',
    default='folder',
    help='Specify a column name to organize files by.'
    )

parser.add_argument(
    '-n',
    '--name-column',
    action='store_true',
    help='Specify a column that dictates filenames.'
    )

parser.add_argument(
    '-c',
    '--add-column-name',
    action='store_true',
    help='Adds name of column to filename.'
    )

parser.add_argument(
    '-p',
    '--proxies',
    help='Name of file with a list of proxies'
    )

parser.add_argument(
    '-v',
    '--verbose',
    action='store_true'
    )

parser.add_argument(
    '-d',
    '--debug',
    action='store_true'
    )

parser.add_argument(
    '-r',
    '--reset-status',
    action='store_true'
    )

args = parser.parse_args()
VERBOSE = args.verbose

# Get our .csv loaded, separate names from urls
data = pd.read_csv(args.fn)

# Name column.....XXX Will be differed with miyamoto csv
filenames = data['name'] if args.name_column else None

# Get folder names from folder col
folders = data[args.folder_column]


# Look for a column called 'status' and if it don't exist...make it exist
try:
    statuses = data['status']
    if VERBOSE:
        c.log('Status column found...')
    if args.reset_status:
        c.log('Resetting it thuogh...')
        data['this_should_throw_an_exeption']
except KeyError:
    if VERBOSE:
        c.log('No status column found. Creating one...')
    all_statuses_pending = [0 for _ in range(len(data.index))]
    data['status'] = all_statuses_pending
    data.to_csv(args.fn)
    statuses = data['status']

# Necessary to avoid the pandas copy warning
tmp_stat = statuses.copy()

# S3 connection object
s3 = boto3.resource('s3')
bucket = s3.Bucket(args.bucket)


def transfer_file(row_data):
    name, url, folder = row_data
    with rq.Session() as session:
        proxy = session.proxies = mf.get_proxy(args.proxies) if args.proxies else None

        if VERBOSE:
            msg = f'Attempting transfer of file {name} on PID: {getpid()}'
            msg += f' using proxy [blue]{proxy["http"]}[/blue]' if args.proxies else '.'
            msg += f'{url}'
            c.log(msg)
        try:
            file = BytesIO(session.get(
                url,
                stream=True).content
                )

            bucket.upload_fileobj(file, f'{folder}/{name}')

            c.log(
                f'[green]SUCCESS: File {name} transferred into {folder} successfully![/green]')

        except KeyError:
            c.log(
                f'[red]FAILURE: File {name} failed to transfer...marking it so in .csv...[/red]]')


def process_row(row):
    names = []
    urls = []
    folder = []

    for k, v in zip(row._fields, row):
        if mf.contains_url_flag(k) and isinstance(v, str):
            url = rq.utils.unquote(v)
            urls.append(url.replace('original', 'large'))
            name = filenames[row.Index] if args.name_column else mf.strip_filename(url)
            name = mf.strip_url_flag(k) + "_" + name if args.add_column_name else name
            names.append(name)
            folder.append(row.folder)

    row_data = zip(names, urls, folder)

    with Pool() as p:
        p.map(transfer_file, row_data)

    c.print()
    return True


def main():

    total_rows = len(data)

    for r, row in enumerate(data.itertuples()):
        if VERBOSE:
            c.log(f'{((r+1)/total_rows)*100:.2f}% done. Starting on row {r+1} of {total_rows}...')

        if row.status == int(status.SUCCESSFUL.value):
            if VERBOSE:
                c.log(
                    f'[yellow]Skipping row {row.Index} as it has already been transfered.[/yellow]\n')
            continue

        try:
            res = process_row(row)
        except KeyboardInterrupt:
            sys.exit()
        except KeyError:
            res = False

        tmp_stat[r] = int(status.SUCCESSFUL.value) if res else int(status.FAILED.value)
        c.log('Commiting...')
        data['status'] = tmp_stat
        data.to_csv(args.fn, index=False)


if __name__ == "__main__":

    main()
