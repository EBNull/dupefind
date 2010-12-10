import os
import sys
import hashlib
import datetime
import csv
import logging
from optparse import OptionParser

log = logging.getLogger()

def recursive_file_list(dir, on_exception=None):
    """Breadth-first search of directory yeilding full paths"""
    subfolders = []
    try:
        for basename in os.listdir(dir):
            p = os.path.join(dir, basename)
            if os.path.isdir(p):
                subfolders.append(p)
            else:
                yield p
        for f in subfolders:
            for p in recursive_file_list(f, on_exception):
                yield p
    except Exception:
        log.exception("Exception in recursive_file_list")
        if callable(on_exception):
            on_exception(dir, sys.exc_info())
                
def files_with_info(dir, on_exception=None):
    for file in recursive_file_list(dir, on_exception):
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
        
def create_hashfile(dir, outstream):
    c = csv.writer(outstream)
    for i in files_with_info(dir):
        c.writerow(i)
        
def create_dupefile(instream, outstream):
    i = csv.reader(instream)
    o = csv.writer(outstream)
    hashgroups = {}
    for row in i:
        blah, file, fsize, ctime, mtime, atime, md5, sha1 = row
        files = hashgroups.setdefault((md5, sha1), [])
        files.append(row)
    for hashgroup in sorted(hashgroups.itervalues(), key=lambda r: r[0][6]): #Sort by hash
        if len(hashgroup) > 1:
            for n in sorted(hashgroup, key=lambda r: r[1]): #Sort by name
                o.writerow(n)
        
def main(argv):
    parser = OptionParser()
    parser.add_option("-c", "--hash", action="store_true", default=False, dest="action_hash", help='Create hashfile csv')
    parser.add_option("-d", "--duplicates", action="store_true", default=False, dest="action_duplicates", help='Filter hashfile csv for duplicates')
    
    parser.add_option("-o", "--out", action="store", type="string", dest="output_filename")
    (options, args) = parser.parse_args(argv)
    
    saw_action = (getattr(options, attr) for attr in ("action_hash", "action_duplicates"))
    if not any(saw_action):
        parser.print_help()
        sys.stdout.write("\nNeed at least one action.\n\n")
        return

    if len([i for i in saw_action if i > 1]):
        parser.print_help()
        sys.stdout.write("\nNeed only one action.\n\n")
        return
    
    if options.output_filename in ('-', '', None):
        if sys.platform == "win32":
            import os, msvcrt
            msvcrt.setmode(sys.stdout.fileno(), os.O_BINARY)
        outfile = sys.stdout
    else:
        outfile = open(options.output_filename, "wb")
        
    if options.action_hash:
        create_hashfile(args[1], outfile)
    if options.action_duplicates:
        infile = open(args[1], "rb")
        create_dupefile(infile, outfile)
    
    
if __name__ == '__main__':
    logging.basicConfig()
    main(sys.argv)