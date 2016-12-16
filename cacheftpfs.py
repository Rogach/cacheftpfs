#!/usr/bin/python2

from ftputil import FTPHost
import fuse
import signal
import sys
import os
from stat import S_IFDIR, S_IFREG
from errno import ENOENT
import threading
import time
from optparse import OptionParser

parser = OptionParser()
parser.add_option("--mountpoint", dest="mountpoint", help="mount fs here", metavar="DIR")
parser.add_option("--host", dest="host", help="ftp host", metavar="HOST")
parser.add_option("--user", dest="username", help="ftp username", metavar="NAME")
parser.add_option("--pass", dest="password", help="ftp password", metavar="PASS")
parser.add_option("--debug-ftp", action="store_true", dest="debug_ftp", help="list ftp calls")
parser.add_option("--debug-fs", action="store_true", dest="debug_fs", help="list fs calls")
(options, args) = parser.parse_args()

ftp = FTPHost(options.host, options.username, options.password)
ftp.stat_cache.max_age = 5

# helper class to store relevant results of stat() call
class Stat:
    def __init__(self):
        self.st_mode = 0
        self.st_size = 0
        self.enoent = False

class FtpFs:
    def __init__(self):
        self.ftp_lock = threading.Lock()

    def ftp_stat(self, path):
        st = Stat()
        if path == "/":
            st.st_mode = S_IFDIR | 0o755
            st.st_size = 0
        else:
            self.ftp_lock.acquire()
            if options.debug_ftp:
                print("ftp:stat " + path)
            try:
                ftp_st = ftp.stat(path)
            except:
                # path not found
                st.enoent = True
                return st
            finally:
                self.ftp_lock.release()
            st.st_mode = ftp_st.st_mode
            st.st_size = ftp_st.st_size
        return st

    def ftp_write(self, path, data):
        with self.ftp_lock:
            if options.debug_ftp:
                print("ftp:write " + path)
            with ftp.open(path, mode = "wb") as f:
                f.write(data)

    def ftp_read(self, path):
        with self.ftp_lock:
            if options.debug_ftp:
                print("ftp:read " + path)
            with ftp.open(path, mode = "rb") as f:
                return f.read()

    def ftp_listdir(self, path):
        with self.ftp_lock:
            if options.debug_ftp:
                print("ftp:listdir " + path)
            return map(lambda f: fuse.Direntry(f), ftp.listdir(path))

    def ftp_mkdir(self, path):
        with self.ftp_lock:
            if options.debug_ftp:
                print("ftp:mkdir " + path)
            ftp.mkdir(path)

    def ftp_rename(self, from_path, to_path):
        with self.ftp_lock:
            if options.debug_ftp:
                print("ftp:rename " + from_path + " -> " + to_path)
            ftp.rename(from_path, to_path)

    def ftp_remove(self, path):
        with self.ftp_lock:
            if options.debug_ftp:
                print("ftp:remove " + path)
            ftp.remove(path)

    def ftp_rmtree(self, path):
        with self.ftp_lock:
            if options.debug_ftp:
                print("ftp:rmtree " + path)
            ftp.rmtree(path)

class IoQueue:
    def __init__(self):
        self.queue_lock = threading.Lock()
        self.queue_set = set()
        self.queue_list = []

    def put(self, *msg):
        with self.queue_lock:
            for m in msg:
                if m not in self.queue_set:
                    self.queue_set.add(m)
                    self.queue_list.append(m)

    def get(self):
        with self.queue_lock:
            if len(self.queue_list) > 0:
                p = min(m[0] for m in self.queue_list)
                m = next(m for m in self.queue_list if m[0] == p)
                self.queue_set.discard(m)
                self.queue_list.remove(m)
                return m
            else:
                return None

PRIORITY_WRITE = 1
PRIORITY_READ = 2

class CacheFtpFS(fuse.Fuse, FtpFs):
    def __init__(self, *args, **kw):
        fuse.Fuse.__init__(self, *args, **kw)
        FtpFs.__init__(self)

        self.cache_attr = {}
        self.cache_list = {}
        self.cache_data = {}

        self.io_queue = IoQueue()

        ftp_thread = threading.Thread(target = self.ftp_worker)
        ftp_thread.setDaemon(True)
        ftp_thread.start()

    def flush_dir(self, path):
        d = os.path.dirname(path)
        self.cache_list.pop(d, None)
        self.io_queue.put((PRIORITY_READ, "listdir", d))

    def ftp_worker(self):
        while True:
            job = self.io_queue.get()
            if job != None:
                try:
                    if job[1] == "stat":
                        self.cache_attr[job[2]] = self.ftp_stat(job[2])
                    elif job[1] == "write":
                        self.ftp_write(job[2], self.cache_data[job[2]])
                    elif job[1] == "read":
                        self.cache_data[job[2]] = self.ftp_read(job[2])
                    elif job[1] == "listdir":
                        self.cache_list[job[2]] = self.ftp_listdir(job[2])
                    elif job[1] == "mkdir":
                        self.ftp_mkdir(job[2])
                    elif job[1] == "rename":
                        self.ftp_rename(job[2], job[3])
                    elif job[1] == "remove":
                        self.ftp_remove(job[2])
                    elif job[1] == "rmtree":
                        self.ftp_rmtree(job[2])
                except Exception as e:
                    print("exception in ftp job")
                    print(e)
            else:
                time.sleep(0.2)

    def hostile_dir(self, path):
        badends = [
            ".dir-locals.el", "RCS", "CVS", ".svn", "SCCS",
            ".bzr", ".git", ".hg", "_MTN", "{arch}", ".Trash", ".Trash-1000"
        ]
        return any(path.endswith(e) for e in badends)

    def getattr(self, path):
        if options.debug_fs:
            print("getattr " + path)

        if self.hostile_dir(path):
            return -ENOENT
        if self.cache_attr.get(path) != None:
            ftp_st = self.cache_attr[path]
            if path != "/":
                self.io_queue.put((PRIORITY_READ, "stat", path))
        else:
            ftp_st = self.ftp_stat(path)
            self.cache_attr[path] = ftp_st

        if ftp_st.enoent:
            return -ENOENT

        st = fuse.Stat()

        # won't handle number of hard links
        st.st_nlink = 1
        # won't handle file ownership
        st.st_uid = os.geteuid()
        st.st_gid = os.getegid()
        # won't handle modification times
        st.st_mtime = 0

        st.st_mode = ftp_st.st_mode
        st.st_size = ftp_st.st_size

        return st

    # set file modification times ('touch' does that, for example)
    # we are ignoring times, so just pass in these methods
    # we also ignore various attribute functions
    def utimens(self, path, ts_acc, ts_mod):
        pass
    def getxattr(self, path, name, position=0):
        return ""
    def listxattr(self, path):
        return []
    def setxattr(self, path, name, value, options, position=0):
        pass
    def removexattr(self, path, name):
        pass
    def chmod(self, path, mode):
        pass

    def readdir(self, path, offset, dh=None):
        if options.debug_fs:
            print("readdir " + path)

        if self.cache_list.get(path) != None:
            self.io_queue.put((PRIORITY_READ, "listdir", path))
            return self.cache_list[path]

        f = [fuse.Direntry("."), fuse.Direntry("..")] + self.ftp_listdir(path)
        self.cache_list[path] = f
        return f

    def open(self, path, flags):
        return 0

    def create(self, path, mode, rdev):
        if options.debug_fs:
            print("create " + path + " mode=" + str(mode))

        self.cache_data[path] = ""

        if self.cache_attr.get(path) == None:
            self.cache_attr[path] = Stat()
        self.cache_attr[path].enoent = False
        self.cache_attr[path].st_size = 0
        self.cache_attr[path].st_mode = mode

        self.io_queue.put((PRIORITY_WRITE, "write", path), (PRIORITY_READ, "stat", path))
        self.flush_dir(path)

    def write(self, path, buf, offset, fh=None):
        if options.debug_fs:
            print("write " + path + " offset=" + str(offset))

        if self.cache_data.get(path) == None:
            self.cache_data[path] = self.ftp_read(path)

        self.cache_data[path] = self.cache_data[path][:offset] + buf
        if self.cache_attr.get(path) != None:
            self.cache_attr[path] = Stat()

        self.cache_attr[path].enoent = 0
        self.cache_attr[path].st_size = len(self.cache_data[path])
        self.cache_attr[path].st_mode = S_IFREG | 0o644

        self.io_queue.put((PRIORITY_WRITE, "write", path), (PRIORITY_READ, "stat", path))

        return len(buf)

    def truncate(self, path, length, fh=None):
        if options.debug_fs:
            print("truncate " + path)

        self.cache_data[path] = ""

        if self.cache_attr.get(path) == None:
            self.cache_attr[path] = Stat()
        self.cache_attr[path].enoent = False
        self.cache_attr[path].st_size = 0
        self.cache_attr[path].st_mode = S_IFREG | 0o644

        self.io_queue.put((PRIORITY_WRITE, "write", path), (PRIORITY_READ, "stat", path))
        self.flush_dir(path)

    def read(self, path, length, offset, fh=None):
        if options.debug_fs:
            print("read " + path + " length=" + str(length) + " offset=" + str(offset))

        if self.cache_data.get(path) != None:
            self.io_queue.put((PRIORITY_READ, "read", path))
        else:
            self.cache_data[path] = self.ftp_read(path)

        return self.cache_data[path][offset : offset + length]

    def mkdir(self, path, mode):
        if options.debug_fs:
            print("mkdir " + path)

        self.io_queue.put((PRIORITY_WRITE, "mkdir", path), (PRIORITY_READ, "stat", path))
        if self.cache_attr.get(path) == None:
            self.cache_attr[path] = Stat()
        self.cache_attr[path].enoent = False
        self.cache_attr[path].st_size = 0
        self.cache_attr[path].st_mode = S_IFDIR | 0o755

        self.flush_dir(path)

    def rename(self, from_path, to_path):
        if options.debug_fs:
            print("rename " + from_path + " -> " + to_path)

        self.io_queue.put((PRIORITY_WRITE, "rename", from_path, to_path), (PRIORITY_READ, "stat", to_path))

        self.flush_dir(from_path)
        self.flush_dir(to_path)

        self.cache_attr.pop(from_path, None)
        self.cache_attr.pop(to_path, None)
        self.cache_list.pop(from_path, None)
        self.cache_list.pop(to_path, None)
        self.cache_data.pop(from_path, None)
        self.cache_data.pop(to_path, None)

    def unlink(self, path):
        if options.debug_fs:
            print("unlink " + path)

        self.io_queue.put((PRIORITY_WRITE, "remove", path))

        self.flush_dir(path)
        if self.cache_attr.get(path) != None:
            self.cache_attr[path].enoent = True
        else:
            self.cache_attr[path] = Stat()
            self.cache_attr[path].enoent = True
        self.cache_data.pop(path, None)

    def rmdir(self, path):
        if options.debug_fs:
            print("rmdir " + path)

        self.io_queue.put((PRIORITY_WRITE, "rmtree", path))

        self.flush_dir(path)
        if self.cache_attr.get(path) != None:
            self.cache_attr[path].enoent = True
        else:
            self.cache_attr[path] = Stat()
            self.cache_attr[path].enoent = True
        self.cache_list.pop(path, None)

fuse.fuse_python_api = (0, 2)

fs = CacheFtpFS()

# allow us to exit via C-c
signal.signal(signal.SIGINT, signal.SIG_DFL)

fs.main(args=["./cacheftpfs.py", options.mountpoint, "-f"])
