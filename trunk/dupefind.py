import os
import sys
import hashlib
import datetime
import csv
import logging
import shutil
import ctypes

from optparse import OptionParser

from collections import namedtuple
FileEntry = namedtuple('FileEntry', [
    'dirbase', #Relative subpath to file from starting path of generated hashfile
    'dirname', #Full dirname to file. Not really needed.
    'path', #Full path to file
    'size',
    'ctime',
    'mtime',
    'atime',
    'ctime_dt',
    'mtime_dt',
    'atime_dt',
    'md5',
    'sha1',
])
ISO_FMT = "%Y-%m-%d %H:%M:%S.%f"
def _from_timestr(t):
    if not t:
        return None
    try:
        return datetime.datetime.strptime(t, ISO_FMT)
    except Exception:
        pass
    try:
        return datetime.datetime.strptime(t, ISO_FMT[:-3])
    except Exception:
        pass
        
def fe_to_unicode(fe):
    fe = FileEntry(*fe)
    return FileEntry(
        fe.dirbase.decode('utf-8'),
        fe.dirname.decode('utf-8'),
        fe.path.decode('utf-8'),
        fe.size,
        fe.ctime,
        fe.mtime,
        fe.atime,
        _from_timestr(fe.ctime_dt),
        _from_timestr(fe.mtime_dt),
        _from_timestr(fe.atime_dt),
        fe.md5,
        fe.sha1,
    )
def fe_to_utf8(fe):
    return FileEntry(
        fe.dirbase.encode('utf-8'),
        fe.dirname.encode('utf-8'),
        fe.path.encode('utf-8'),
        fe.size,
        fe.ctime,
        fe.mtime,
        fe.atime,
        str(fe.ctime_dt) if fe.ctime_dt else '',
        str(fe.mtime_dt) if fe.mtime_dt else '',
        str(fe.atime_dt) if fe.atime_dt else '',
        fe.md5,
        fe.sha1,
    )

log = logging.getLogger()

def recursive_file_list(dir, on_exception=None, keep_dirs=False):
    """Breadth-first search of directory yeilding full paths"""
    dir = unicode(dir)
    dir = os.path.realpath(dir)
    subfolders = []
    try:
        for basename in os.listdir(dir):
            p = os.path.join(dir, basename)
            if os.path.isdir(p):
                if not os.path.islink(p) and not is_win32_reparsepoint(p):
                    subfolders.append(p)
                if keep_dirs:
                    yield p
            else:
                yield p
        for f in subfolders:
            for p in recursive_file_list(f, on_exception):
                yield p
    except Exception:
        log.exception("Exception in recursive_file_list")
        if callable(on_exception):
            on_exception(dir, sys.exc_info())

def open_file_for_backup_win32(fn):
    import ctypes
    GENERIC_READ = 0x80000000
    GENERIC_WRITE = 0x40000000
    FILE_SHARE_DELETE = 0x00000004
    FILE_SHARE_READ = 0x00000001
    FILE_SHARE_WRITE = 0x00000002
    FILE_FLAG_BACKUP_SEMANTICS = 0x2000000
    OPEN_EXISTING = 3
    INVALID_HANDLE_VALUE = -1
    hFile = ctypes.windll.kernel32.CreateFileW(unicode(fn), GENERIC_READ, FILE_SHARE_READ, None, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, None)
    if hFile == INVALID_HANDLE_VALUE:
        raise ctypes.WinError()
    import msvcrt
    c_fh = msvcrt.open_osfhandle(hFile, os.O_RDONLY)
    return os.fdopen(c_fh, 'rb')
    
def files_with_info(dir, on_exception=None):
    """Generator yielding information about each file in a directory"""
    dir = os.path.realpath(dir)
    for file in recursive_file_list(dir, on_exception):
        hashobjs = (hashlib.md5(), hashlib.sha1())
        try:
            f = open(file, "rb")
            while True:
                data = f.read(16*1096) #16MB chunks
                if not data:
                    break
                for h in hashobjs:
                    h.update(data)
            f.close()
        except IOError:
            hashes = (None, None)
        else:
            hashes = []
            for h in hashobjs:
                hashes.append(h.hexdigest())
        hashobjs = None
        contents = ""
        prefix = os.path.relpath(os.path.dirname(file), dir)
        if prefix == '.':
            prefix = ''
        def from_timestamp(t):
            try:
                return datetime.datetime.fromtimestamp(t)
            except ValueError:
                return None
        yield FileEntry(
            prefix,
            os.path.dirname(file),
            file,
            os.path.getsize(file),
            os.path.getctime(file),
            os.path.getmtime(file),
            os.path.getatime(file),
            from_timestamp(os.path.getctime(file)),
            from_timestamp(os.path.getmtime(file)),
            from_timestamp(os.path.getatime(file)),
            hashes[0],
            hashes[1],
        )
        
def create_hashfile(dir, outstream):
    """For a given dir, iterate through all the files and create a hashfile containing all the data from files_with_info"""
    c = csv.writer(outstream)
    for i in files_with_info(dir):
        c.writerow(fe_to_utf8(i))
        
def create_dupefile(instream, outstream):
    """Given a hashfile, produce a dupefile (another csv) containing only duplicate entries"""
    i = csv.reader(instream)
    o = csv.writer(outstream)
    hashgroups = {}
    for row in i:
        row = FileEntry(*fe_to_unicode(row))
        files = hashgroups.setdefault((row.md5, row.sha1), [])
        files.append(row)
    for hashgroup in sorted(hashgroups.itervalues(), key=lambda r: (r[0].md5, r[0].sha1)): #Sort by hash
        if len(hashgroup) > 1:
            for n in sorted(hashgroup, key=lambda r: r.path): #Sort by name
                o.writerow(fe_to_utf8(n))

def choice_latest_mtime_keep_dupes(hashgroup):
    """Saves duplicate files by giving them a .dupe_# pre-extension"""
    """A choice function takes a hashgroup as input. A hashgroup is simply a list of FileEntry tuples. Return value should be a tuple of tuples whose elements are FileEntry, dest_rel_filename"""
    ret = []
    for i, n in enumerate(sorted(hashgroup, key=lambda r: r.mtime)): #Sort by mtime
        basename, ext = os.path.splitext(os.path.basename(n.path))
        if i == 0:
            basename = u"%s%s"%(basename, ext)
        else:
            basename = u"%s%s%s"%(basename, u'.dupe_%s'%(i), ext)
        ret.append((n, os.path.join(n.dirbase, basename)))
    return ret
    
def choice_latest_mtime_drop_dupes(hashgroup):
    """Does not copy file that already have been copied with the same hash"""
    ret = []
    for i, n in enumerate(sorted(hashgroup, key=lambda r: r.mtime)): #Sort by mtime
        basename, ext = os.path.splitext(os.path.basename(n.path))
        if i == 0:
            basename = u"%s%s"%(basename, ext)
            ret.append((n, os.path.join(n.dirbase, basename)))
        else:
            ret.append((n, None))
    return ret

def fn_collision_rename(destfile):
    """Finds an acceptable filename that does not exist. Susceptible to race conditions."""
    full_p, full_ext = os.path.splitext(destfile)
    count = 1
    while True:
        middle = u".collision_%s"%(count)
        count += 1
        dest = u"%s%s%s"%(full_p, middle, full_ext)
        if not os.path.exists(dest):
            return dest
            
def get_privileges_win32(priv):
    import ctypes
    from ctypes.wintypes import DWORD, LONG, HANDLE
    kernel32 = ctypes.windll.kernel32
    advapi32 = ctypes.windll.advapi32
    TOKEN_ADJUST_PRIVILEGES = 0x20
    TOKEN_QUERY = 0x8
    SE_PRIVILEGE_ENABLED = 0x2
    class LUID(ctypes.Structure):
        _fields_ = [
            ('LowPart', DWORD),
            ('HighPart', LONG),
        ]
    class LUID_AND_ATTRIBUTES(ctypes.Structure):
        _fields_ = [
            ('Luid', LUID),
            ('Attributes', DWORD),
        ]
    class TOKEN_PRIVILEGES(ctypes.Structure):
        _fields_ = [
            ('PrivilegeCount', DWORD),
            ('Privileges', LUID_AND_ATTRIBUTES * 1),
        ]
    hToken = HANDLE()
    luid = LUID()
    token_state = TOKEN_PRIVILEGES()
    if not advapi32.OpenProcessToken(kernel32.GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES, ctypes.byref(hToken)):
        raise ctypes.WinError()
    try:
        if not advapi32.LookupPrivilegeValueA(None, priv, ctypes.byref(luid)):
            raise ctypes.WinError()
        token_state.PrivilegeCount = 1
        token_state.Privileges[0].Luid = luid
        token_state.Privileges[0].Attributes = SE_PRIVILEGE_ENABLED
        if not advapi32.AdjustTokenPrivileges(hToken, 0, ctypes.byref(token_state), 0, 0, 0):
            raise ctypes.WinError()
    finally:
        kernel32.CloseHandle(hToken)


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
    def __lt__(self, other):
        if self.high > other.high:
            return True
        if self.high < other.high:
            return False
        if self.low > other.low:
            return True
        return False
class FileTimePreserver(object):
    def __new__(cls, filename):
        klass = cls
        if sys.platform == 'win32':
            klass = FileTimesWin32
        i = super(cls, cls).__new__(klass, filename)
        if klass != cls:
            i.__init__(filename)
        return i
    def __init__(self, filename):
        pass
    def __enter__(self):
        pass
    def __exit__(self, exctype, exc, tb):
        pass
        
class FileTimesWin32(FileTimePreserver):
    """When used as a context manager, rewrites a file's times on exit"""
    def __init__(self, filename):
        self.filename=filename
    
    def __enter__(self):
        self._times = self.read_file_times(self.filename)
        return self
    def __exit__(self, exctype, exc, tb):
        if not exc:
            self.write_file_times(self.filename, *self._times)
            
    @staticmethod
    def read_file_times(filename):
        ctime, mtime, atime = FILETIME(), FILETIME(), FILETIME()
        hFile = ctypes.windll.kernel32.CreateFileW(unicode(filename), GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, None, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, None)
        if hFile == INVALID_HANDLE_VALUE:
            raise ctypes.WinError()
        if not ctypes.windll.kernel32.GetFileTime(hFile, ctypes.byref(ctime), ctypes.byref(atime), ctypes.byref(mtime)):
            err = ctypes.WinError()
            ctypes.windll.kernel32.CloseHandle(hFile)
            raise err
        ctypes.windll.kernel32.CloseHandle(hFile)
        return ctime, mtime, atime
        
    @staticmethod
    def write_file_times(filename, ctime, mtime, atime):
        hFile = ctypes.windll.kernel32.CreateFileW(unicode(filename), GENERIC_WRITE, FILE_SHARE_READ, None, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, None)
        if hFile == INVALID_HANDLE_VALUE:
            raise ctypes.WinError()
        if not ctypes.windll.kernel32.SetFileTime(hFile, ctypes.byref(ctime), ctypes.byref(atime), ctypes.byref(mtime)):
            err = ctypes.WinError()
            ctypes.windll.kernel32.CloseHandle(hFile)
            raise err
        ctypes.windll.kernel32.CloseHandle(hFile)
        
def copy_file_creation_time_win32(src, dest):
    #shutil.copy2 doesn't copy the created date properly
    ctime, mtime, atime = FileTimesWin32.read_file_times(src)
    FileTimesWin32.write_file_times(dest, ctime, mtime, atime)

def filecopy(src, dest):
    if sys.platform == 'win32':
        open_src = open_file_for_backup_win32(src)
    else:
        open_src = open(src, 'rb')
    with open_src as fsrc:
        with FileTimePreserver(os.path.dirname(dest)):
            with open(dest, 'wb') as fdst:
                shutil.copyfileobj(fsrc, fdst)
    shutil.copystat(src, dest)
    if sys.platform == 'win32':
        copy_file_creation_time_win32(src, dest)
    
def nodupe_copy(hashstream, dest_dir, choice_func=None, fn_collision_func=None, dry_run=True, continue_on_error=False):
    log = logging.getLogger('nodupe_copy')
    if not choice_func:
        choice_func = choice_latest_mtime_drop_dupes
    if not fn_collision_func:
        fn_collision_func = fn_collision_rename
    i = csv.reader(hashstream)
    hashgroups = {}
    for row in i:
        row = FileEntry(*fe_to_unicode(row))
        files = hashgroups.setdefault((row.md5, row.sha1), [])
        files.append(row)
    for hashgroup in sorted(hashgroups.itervalues(), key=lambda r: (r[0].md5, r[0].sha1)): #Sort by hash
        copydata = choice_func(hashgroup)
        for fileentry, dest_filename in copydata:
            if dest_filename is None:
                log.debug(u"Skipping file %s", fileentry.path)
            else:
                dest = os.path.join(dest_dir, dest_filename)
                log.debug(u"Copying file %s to %s", fileentry.path, dest)
                if os.path.exists(dest):
                    newdest = fn_collision_func(dest)
                    log.warning(u"%s already exists, collision resolved to %s", dest, newdest)
                    dest = newdest
                if not dry_run:
                    if not os.path.isdir(os.path.dirname(dest)):
                        os.makedirs(os.path.dirname(dest))
                        if sys.platform == 'win32':
                            copy_file_creation_time_win32(os.path.dirname(fileentry.path), os.path.dirname(dest))
                    try:
                        filecopy(fileentry.path, dest)
                    except Exception:
                        log.exception("Couldn't copy %s to %s"%(fileentry.path, dest))
                        if not continue_on_error:
                            raise
                else:
                    logging.info(u"DRY: copy %s to %s", fileentry.path, dest)
                    
def is_win32_reparsepoint(fn):
    """Reparsepoint can be a junction, etc. Basically symlinks."""
    if sys.platform != 'win32':
        return False
    INVALID_FILE_ATTRIBUTES = -1
    FILE_ATTRIBUTE_REPARSE_POINT = 0x400
    import ctypes
    attr = ctypes.windll.kernel32.GetFileAttributesW(unicode(fn))
    if attr == INVALID_FILE_ATTRIBUTES:
        raise ctypes.WinError()
    if attr & FILE_ATTRIBUTE_REPARSE_POINT:
        return True
    return False

def fix_dir_mtimes(root_dir):
    root_dir = os.path.realpath(root_dir)
    subpaths = {}
    for file in recursive_file_list(root_dir):
        dir = os.path.dirname(file)
        mtime = FileTimePreserver('').read_file_times(file)[1]
        while dir != root_dir:
            dinfo = subpaths.setdefault(dir, [len(os.path.split(dir)), dir, mtime])
            if dinfo[2] > mtime:
                dinfo[2] = mtime
            dir = os.path.dirname(dir)
    for dirdepth, dir, mtime in sorted(subpaths.values()):
        with FileTimePreserver(dir) as fp:
            fp._times = list(fp._times)
            fp._times[1] = mtime
    
def main(argv):
    log = logging.getLogger('main')
    parser = OptionParser()
    parser.add_option("-c", "--hash", action="store_true", default=False, dest="action_hash", help='Create hashfile csv')
    parser.add_option("-d", "--duplicates", action="store_true", default=False, dest="action_duplicates", help='Filter hashfile csv for duplicates')
    parser.add_option("",   "--nodupe_copy", action="store_true", default=False, dest="action_nodupe_copy", help='Copy files from hashfile, eliminating duplicates, to dest')
    parser.add_option("",   "--continue_on_error", action="store_true", default=False, dest="continue_on_error", help='Big try/except over each file copy')
    parser.add_option("",   "--fix_dir_times", action="store_true", default=False, dest="action_fix_dir_times", help="Set each directory's mtime to the latest mtime of it's contents")
    parser.add_option("",   "--dry", action="store_true", default=False, dest="dry_run", help="Don't copy anything")
    
    parser.add_option("-o", "--out", action="store", type="string", dest="output_filename")
    
    (options, args) = parser.parse_args(argv)
    
    saw_action = (getattr(options, attr) for attr in ("action_hash", "action_duplicates", "action_nodupe_copy", "action_fix_dir_times"))
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
        
    if sys.platform == "win32":
        try:
            get_privileges_win32("SeBackupPrivilege")
        except Exception:
            log.exception("Couldn't aquire SeBackupPrivilege")
        else:
            log.debug("Aquired SeBackupPrivilege")
        
        
    if options.action_fix_dir_times:
        log.info(u"Fixing directory mtime starting at %s", unicode(args[1]))
        fix_dir_mtimes(unicode(args[1]))
    if options.action_hash:
        log.info(u"Creating hashfile from %s", unicode(args[1]))
        create_hashfile(unicode(args[1]), outfile)
    if options.action_duplicates:
        log.info(u"Creating dupefile from %s", unicode(args[1]))
        infile = open(unicode(args[1]), "rb")
        create_dupefile(infile, outfile)
    if options.action_nodupe_copy:
        log.info(u"Copying without dupes from %s to %s", unicode(args[1]), unicode(args[2]))
        infile = open(unicode(args[1]), "rb")
        nodupe_copy(infile, unicode(args[2]), dry_run=options.dry_run, continue_on_error=options.continue_on_error)
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main(sys.argv)
