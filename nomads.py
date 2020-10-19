import json
import glob
import re
import argparse
import pathlib
import datetime
import requests
import shutil
import string
import asyncio
from html.parser import HTMLParser
from urllib.parse import urljoin

VARS = {
    "dateYMD": datetime.date.today().strftime("%Y%m%d")
}

class HRefParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.href_list = []
    
    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for attr, value in attrs:
                if attr == 'href':
                    self.href_list.append(value)

def read_config(fn):
    with open(fn, 'r') as fh:
        return json.load(fh)
        

def cli():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("config")
    
    return parser.parse_args()

def get_file_list(prefix):
    """ Get file list from http prefix """
    print("Fetching file list from", prefix)
    k = requests.get(prefix)
    if not k.ok:
        raise Exception("Unable to get http directory listing")
    
    parser = HRefParser()
    parser.feed(k.content.decode())
    k.close()
    return parser.href_list

def filter_files_regex(pattern, file_list):
    re_pattern = re.compile(f"^{pattern}$")
    
    matched = []
    for f in file_list:
        if re_pattern.match(f):
            matched.append(f)
    return matched

def process_prefix(prefix):
    """ Process prefix template """
    temp = string.Template(prefix)
    return temp.substitute(VARS)

def download_file(remote_dir, local_dir, filename):
    with requests.get(urljoin(remote_dir, filename), stream=True) as R:
        with open(local_dir.joinpath(filename), 'wb') as fh:
            shutil.copyfileobj(R.raw, fh)

async def main(args):
    config = read_config(args.config)
    
    loop = asyncio.get_event_loop()
    for job, details in config.items():
        # ensure destination exists
        dest = process_prefix(details["destination"])
        dest = pathlib.Path(dest).resolve()
        dest.mkdir(parents=True, exist_ok=True)
        
        prefix = process_prefix(details["http_prefix"])
        
        # get the file list for prefix
        flist = get_file_list(prefix)
        flist = filter_files_regex(details["regex"], flist)
        
        print(prefix)
        for i, f in enumerate(flist, 1):
            #print("Downloading file", i, "of", len(flist), ":", f)
            loop.run_in_executor(None, download_file, prefix, dest, f)
    
if __name__ == "__main__":
    args = cli()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
