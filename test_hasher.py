#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from dropbox_content_hasher import DropboxContentHasher, StreamHasher
import time, os

def human_size(size_bytes):
    """
    format a size in bytes into a 'human' file size, e.g. bytes, KB, MB, GB, TB, PB
    Note that bytes/KB will be reported in whole numbers but MB and above will have greater precision
    e.g. 1 byte, 43 bytes, 443 KB, 4.3 MB, 4.43 GB, etc
    """
    if size_bytes == 1:
        # because I really hate unnecessary plurals
        return "1 byte"

    suffixes_table = [('bytes',0),('KB',0),('MB',1),('GB',2),('TB',2), ('PB',2)]

    num = float(size_bytes)
    for suffix, precision in suffixes_table:
        if num < 1024.0:
            break
        num /= 1024.0

    if precision == 0:
        formatted_size = f'{int(num)}'
    else:
        formatted_size = str(round(num, ndigits=precision))

    return f'{formatted_size} {suffix}'

def main(file):
    hasher = DropboxContentHasher()
    size = os.path.getsize(file)
    chunk = 512 * 1024 * 1024
    start = time.time()
    total = 0
    print('Reading file: {}, size: {}, chunk: {}'.format(file, human_size(size), human_size(chunk)))
    with open(file, 'rb') as f:
        wrapped_f = StreamHasher(f, hasher)
        while wrapped_f.tell() < size:
            response = wrapped_f.read(chunk)
            total = wrapped_f.tell()
            duration = time.time() - start
            print('{}s read: {} out of {}, {} per s, {}%'.format(int(duration), human_size(total), human_size(size), human_size(int(total/duration)), round(total*100//size, 2)))
    locally_computed = hasher.hexdigest()
    print('Hash: {}, time: {}s, size: {}, rate:{}per s'.format(locally_computed, int(duration), human_size(size), human_size(int(size/duration))))
    
if __name__ == "__main__":
    main('/shares/Backups/dump/vzdump-qemu-106-2023_02_13-01_00_10.vma.zst')