CacheFtpFS
==========

Simple ftp-backed filesystem. Uses FUSE to mount remote directory over ftp.

Assumes that remote files are rarely changed by other clients, and that
user doesn't care about file ownership, permissions or modification times.