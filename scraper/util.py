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
