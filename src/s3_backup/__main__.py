import argparse
import logging
import sqlite3

import s3_backup
from s3_backup import __version__, File

logging.getLogger(None).setLevel(logging.INFO + 1)  # Set just above INFO
log_file_handler = logging.StreamHandler()
log_file_handler.setFormatter(logging.Formatter(
    fmt="[%(name)s %(levelname)s] %(message)s"
))
logging.getLogger(None).addHandler(log_file_handler)


def main(args=None):
    """
    Main entry point for your project.

    Args:
        args : list
            A of arguments as if they were input in the command line. Leave it
            None to use sys.argv.
    """

    parser = argparse.ArgumentParser('s3-backup',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--version', '-V', action='version', version=f"s3_backup v{__version__}")

    parser.add_argument('path',
                        help="Path to backup (recursively). Symlinks are treated as the thing they "
                             "point to (directories or files)")
    parser.add_argument('bucket',
                        help="S3 bucket to upload to")

    parser.add_argument('--storage-class', default="STANDARD",
                        help="Storage class to use. See " 
                             "https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html "
                             "for options.")

    parser.add_argument('--cache-file', default="s3_content.sqlite",
                        help="Path to the location of the cache file. The content "
                             "of this file can be reconstructed from the S3 bucket, "
                             "but that is an extensive operation.")

    parser.add_argument('--data-xform',
                        help="Use the given command to transform the data before uploading. "
                             "E.g. `gpg --encrypt-to backup-key --sign --set-filename \"$ORIG_FILENAME\" -` "
                             "will encrypt the files with GnuPG before uploading. "
                             "The command will receive the file on its stdin, and should output "
                             "the transformed data to its stdout. The command is passed through "
                             "/bin/bash, so you can use basic shell magic. The following environment "
                             "variables are available to the command(line): $ORIG_FILENAME, $XFORM_FILENAME")
    parser.add_argument('--filename-xform',
                        help="Similar to --data-xform, but transform the filename instead of the "
                             "file data. Make sure this transform is a consistent one-to-one mapping! "
                             "Note that the command receives the filename on stdin, "
                             "without a trailing newline, and should output without a trailing newline. "
                             "Available environment variables: $FILENAME. E.g. `echo -n \"$FILENAME.gpg\"` "
                             "will append a .gpg extension.")

    parser.add_argument('--verbose', '-v', action='count', default=0,
                        help="Increase verbosity, can be used multiple times for increased verbosity "
                             "(up to 11 times)")

    args = parser.parse_args(args)

    for i in range(0, args.verbose):
        logging.getLogger(None).setLevel(logging.getLogger(None).level - 1)

    File.filename_xform_command = args.filename_xform
    File.data_xform_command = args.data_xform

    s3_backup.do_sync(
        local_path=args.path,
        s3_bucket=args.bucket,
        cache_db=sqlite3.connect(args.cache_file),
        storage_class=args.storage_class,
    )


if __name__ == '__main__':
    main()
