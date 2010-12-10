import os
import sys
import hashlib
import datetime
import csv
from optparse import OptionParser

def recursive_file_list(dir):
    """Breadth-first search of directory yeilding full paths"""
    subfolders = []
    for basename in os.listdir(dir):
        p = os.path.join(dir, basename)
        if os.path.isdir(p):
            subfolders.append(p)
        else:
            yield p
        for f in subfolders:
            for p in recursive_file_list(f):
                yield p
                
def files_with_info(dir):
    for file in recursive_file_list(dir):
        hashobjs = (hashlib.md5(), hashlib.sha1())
        try:
            contents = open(file, "rb").read()
        except IOError:
            hashes = (None, None)
        else:
            hashes = []
            for h in hashobjs:
                h.update(contents)
                hashes.append(h.hexdigest())
        hashobjs = None
        contents = ""
        yield (
            os.path.dirname(file),
            file,
            os.path.getsize(file),
            datetime.datetime.fromtimestamp(os.path.getctime(file)),
            datetime.datetime.fromtimestamp(os.path.getmtime(file)),
            datetime.datetime.fromtimestamp(os.path.getatime(file)),
            hashes[0],
            hashes[1],
        )
        
if __name__ == '__main__':
    parser = OptionParser()
    (options, args) = parser.parse_args()

    if sys.platform == "win32":
        import os, msvcrt
        msvcrt.setmode(sys.stdout.fileno(), os.O_BINARY)

    c = csv.writer(sys.stdout)
    for i in files_with_info(args[0]):
        c.writerow(i)
    