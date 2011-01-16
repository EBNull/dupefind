import os
import sys
import hashlib
import datetime
import csv
import logging
import shutil

from optparse import OptionParser

from collections import namedtuple
FileEntry = namedtuple('FileEntry', [
    'dirbase', #Relative subpath to file from starting path of generated hashfile
    'dirname', 
    'path',
    'size',
    'ctime',
    'mtime',
    'atime',
    'md5',
    'sha1',
])

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
    """Generator yielding information about each file in a directory"""
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
        prefix = os.path.relpath(os.path.dirname(file), dir)
        if prefix == '.':
            prefix = ''
        yield FileEntry(
            prefix,
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
    """For a given dir, iterate through all the files and create a hashfile containing all the data from files_with_info"""
    c = csv.writer(outstream)
    for i in files_with_info(dir):
        c.writerow(i)
        
def create_dupefile(instream, outstream):
    """Given a hashfile, produce a dupefile (another csv) containing only duplicate entries"""
    i = csv.reader(instream)
    o = csv.writer(outstream)
    hashgroups = {}
    for row in i:
        row = FileEntry(row)
        files = hashgroups.setdefault((row.md5, row.sha1), [])
        files.append(row)
    for hashgroup in sorted(hashgroups.itervalues(), key=lambda r: (r[0].md5, r[0].sha1)): #Sort by hash
        if len(hashgroup) > 1:
            for n in sorted(hashgroup, key=lambda r: r.path): #Sort by name
                o.writerow(n)

def choice_latest_mtime_keep_dupes(hashgroup):
    """Saves duplicate files by giving them a .dupe_# pre-extension"""
    """A choice function takes a hashgroup as input. A hashgroup is simply a list of FileEntry tuples. Return value should be a tuple of tuples whose elements are FileEntry, dest_rel_filename"""
    ret = []
    for i, n in enumerate(sorted(hashgroup, key=lambda r: r.mtime)): #Sort by mtime
        basename, ext = os.path.splitext(os.path.basename(n.path))
        if i == 0:
            basename = "%s%s"%(basename, ext)
        else:
            basename = "%s%s%s"%(basename, '.dupe_%s'%(i), ext)
        ret.append((n, os.path.join(n.dirbase, basename)))
    return ret
    
def choice_latest_mtime_drop_dupes(hashgroup):
    """Does not copy file that already have been copied with the same hash"""
    ret = []
    for i, n in enumerate(sorted(hashgroup, key=lambda r: r.mtime)): #Sort by mtime
        basename, ext = os.path.splitext(os.path.basename(n.path))
        if i == 0:
            basename = "%s%s"%(basename, ext)
            ret.append((n, os.path.join(n.dirbase, basename)))
        else:
            ret.append((n, None))
    return ret

def fn_collision_rename(destfile):
    """Finds an acceptable filename that does not exist. Susceptible to race conditions."""
    full_p, full_ext = os.path.splitext(destfile)
    count = 1
    while True:
        middle = ".collision_%s"%(count)
        count += 1
        dest = "%s%s%s"%(full_p, middle, full_ext)
        if not os.path.exists(dest):
            return dest

def copy_file_creation_time_win32(src, dest):
    #shutil.copy2 doesn't copy the created date properly
    import ctypes
    GENERIC_READ = 0x80000000
    GENERIC_WRITE = 0x40000000
    FILE_SHARE_DELETE = 0x00000004
    FILE_SHARE_READ = 0x00000001
    FILE_SHARE_WRITE = 0x00000002
    FILE_FLAG_BACKUP_SEMANTICS = 0x2000000
    OPEN_EXISTING = 3
    FILE_ATTRIBUTE_NORMAL = 128
    INVALID_HANDLE_VALUE = -1
    class FILETIME(ctypes.Structure):
        _fields_ = [
            ("low", ctypes.c_ulong),
            ("high", ctypes.c_ulong),
        ]
    ctime, mtime, atime = FILETIME(), FILETIME(), FILETIME()
    hFile = ctypes.windll.kernel32.CreateFileW(unicode(src), GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, None, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, None)
    if hFile == INVALID_HANDLE_VALUE:
        raise ctypes.WinError()
    if not ctypes.windll.kernel32.GetFileTime(hFile, ctypes.byref(ctime), ctypes.byref(atime), ctypes.byref(mtime)):
        err = ctypes.WinError()
        ctypes.windll.kernel32.CloseHandle(hFile)
        raise ctypes.WinError()
    ctypes.windll.kernel32.CloseHandle(hFile)
    hFile = ctypes.windll.kernel32.CreateFileW(unicode(dest), GENERIC_WRITE, FILE_SHARE_READ, None, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, None)
    if hFile == INVALID_HANDLE_VALUE:
        raise ctypes.WinError()
    if not ctypes.windll.kernel32.SetFileTime(hFile, ctypes.byref(ctime), ctypes.byref(atime), ctypes.byref(mtime)):
        err = ctypes.WinError()
        ctypes.windll.kernel32.CloseHandle(hFile)
        raise ctypes.WinError()
    ctypes.windll.kernel32.CloseHandle(hFile)

def nodupe_copy(hashstream, dest_dir, choice_func=None, fn_collision_func=None, dry_run=True):
    log = logging.getLogger('nodupe_copy')
    if not choice_func:
        choice_func = choice_latest_mtime_drop_dupes
    if not fn_collision_func:
        fn_collision_func = fn_collision_rename
    i = csv.reader(hashstream)
    hashgroups = {}
    for row in i:
        row = FileEntry(*row)
        files = hashgroups.setdefault((row.md5, row.sha1), [])
        files.append(row)
    for hashgroup in sorted(hashgroups.itervalues(), key=lambda r: (r[0].md5, r[0].sha1)): #Sort by hash
        copydata = choice_func(hashgroup)
        for fileentry, dest_filename in copydata:
            if dest_filename is None:
                log.debug("Skipping file %s", fileentry.path)
            else:
                dest = os.path.join(dest_dir, dest_filename)
                log.debug("Copying file %s to %s", fileentry.path, dest)
                if os.path.exists(dest):
                    newdest = fn_collision_func(dest)
                    log.warning("%s already exists, collision resolved to %s", dest, newdest)
                    dest = newdest
                if not dry_run:
                    if not os.path.isdir(os.path.dirname(dest)):
                        os.makedirs(os.path.dirname(dest))
                        if sys.platform == 'win32':
                            copy_file_creation_time_win32(os.path.dirname(fileentry.path), os.path.dirname(dest))
                    shutil.copy2(fileentry.path, dest)
                    if sys.platform == 'win32':
                        copy_file_creation_time_win32(fileentry.path, dest)
                else:
                    logging.info("DRY: copy %s to %s", fileentry.path, dest)
                    
        
def main(argv):
    parser = OptionParser()
    parser.add_option("-c", "--hash", action="store_true", default=False, dest="action_hash", help='Create hashfile csv')
    parser.add_option("-d", "--duplicates", action="store_true", default=False, dest="action_duplicates", help='Filter hashfile csv for duplicates')
    parser.add_option("",   "--nodupe_copy", action="store_true", default=False, dest="action_nodupe_copy", help='Copy files from hashfile, eliminating duplicates, to dest')
    parser.add_option("",   "--dry", action="store_true", default=False, dest="dry_run", help="Don't copy anything")
    
    parser.add_option("-o", "--out", action="store", type="string", dest="output_filename")
    
    (options, args) = parser.parse_args(argv)
    
    saw_action = (getattr(options, attr) for attr in ("action_hash", "action_duplicates", "action_nodupe_copy"))
    if not any(saw_action):
        parser.print_help()
        sys.stderr.write("\nNeed at least one action.\n\n")
        return

    if len([i for i in saw_action if i > 1]):
        parser.print_help()
        sys.stderr.write("\nNeed only one action.\n\n")
        return
    
    if options.output_filename in ('-', '', None):
        #Set stdout output mode to binary
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
    if options.action_nodupe_copy:
        infile = open(args[1], "rb")
        nodupe_copy(infile, args[2], dry_run=options.dry_run)
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main(sys.argv)