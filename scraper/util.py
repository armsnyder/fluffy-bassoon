"""
Copyright 2018 Adam Snyder

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import os
import time

import requests


def download_to_file(url, file_path):
    print('Downloading %s to %s' % (url, file_path))
    content = _get_web_content(url)
    write_file(file_path, content)


def _get_web_content(url):
    for i in range(4):  # try 4 times with exponential backoff
        response = requests.get(url)
        if response.status_code / 100 == 5:  # only retry if error is a 5xx
            time_to_sleep = 2 ** i
            print('Error %d - Retrying in %d seconds...' % (response.status_code, time_to_sleep))
            time.sleep(time_to_sleep)
            continue
        break
    if response.status_code != 200:
        raise HttpResponseError(response.status_code, response.content)
    return response.content


def write_file(file_path, content):
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    with open(file_path, 'wb') as f:
        f.write(content)


class HttpResponseError(RuntimeError):
    def __init__(self, status_code, content):
        self.status_code = status_code
        self.content = content

    def __str__(self):
        return 'HttpResponseError %d: %s' % (self.status_code, self.content)
