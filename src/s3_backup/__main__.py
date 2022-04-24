import argparse
import logging
import sqlite3

import s3_backup
from s3_backup import __version__, FileScanner, LocalFile, KeyTransform, DataTransform

logging.getLogger(None).setLevel(logging.INFO + 1)  # Set just above INFO
log_file_handler = logging.StreamHandler()
log_file_handler.setFormatter(logging.Formatter(
    fmt="[%(name)s %(levelname)s] %(message)s"
))
logging.getLogger(None).addHandler(log_file_handler)
logger = logging.getLogger(None)


class AddOptionValueTuple(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if getattr(namespace, self.dest, None) is None:
            setattr(namespace, self.dest, [])
        getattr(namespace, self.dest).append((option_string, values))


def main(args=None):
    """
    Main entry point for your project.

    Args:
        args : list
            A of arguments as if they were input in the command line. Leave it
            None to use sys.argv.
    """

    parser = argparse.ArgumentParser('s3-backup',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description="Makes a backup of `path` to S3 bucket `bucket`. "
                                                 "Filters can be applied to transform the list of local files to what "
                                                 "you want to get on S3. Note that filters are applied in the order "
                                                 "given on the command line.")
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

    parser.add_argument('--verbose', '-v', action='count', default=0,
                        help="Increase verbosity, can be used multiple times for increased verbosity "
                             "(up to 11 times)")
    parser.add_argument('--dry-run', action='store_true',
                        help="Don't actually upload/delete objects."
                        )

    parser.add_argument('--no-trust-mtime', action='store_true',
                        help="Do not trust file modification time to identify if a file needs uploading. "
                             "By default, a file will only be uploaded if the modification time is more "
                             "recent than the S3 object or if the size is different. "
                             "Enabling this option will only use filesize and hash to be used to "
                             "decide if uploading is needed.")

    parser.add_argument('--data-xform',
                        action=AddOptionValueTuple, dest='filter',
                        help="Use the given command to transform the data before uploading. "
                             "E.g. `gpg --encrypt -r backup-key --sign --set-filename \"KEY\" -` "
                             "will encrypt the files with GnuPG before uploading. "
                             "The command will receive the file data on its stdin, and should output "
                             "the transformed data to its stdout. The command is passed through "
                             "/bin/bash, so you can use basic shell magic. The following environment "
                             "variables are available to the command(line): $KEY")
    parser.add_argument('--filename-xform',
                        action=AddOptionValueTuple, dest='filter',
                        help="Use the given command to ransform the filename/key of the objects. "
                             "Make sure this transform is a consistent one-to-one mapping! "
                             "Note that the command receives the filename on stdin, "
                             "without a trailing newline, and should output without a trailing newline. "
                             "Available environment variables: KEY. E.g. `echo -n \"$KEY.gpg\"` "
                             "will append a .gpg extension. "
                             "Returning nothing at all is a special case and will ignore this file "
                             "(i.e. will pretend this file does not exist locally, not upload to S3, "
                             "and maybe delete the item from S3 if it was already there)")

    args = parser.parse_args(args)

    for i in range(0, args.verbose):
        logging.getLogger(None).setLevel(logging.getLogger(None).level - 1)

    if args.no_trust_mtime:
        LocalFile.trust_mtime = False

    orig_file_list = FileScanner(args.path)
    file_list = orig_file_list

    if args.filter is not None:
        for filter_name, value in args.filter:
            # Note: remember to close over `value`!

            if filter_name == '--filename-xform':
                file_list = KeyTransform.wrap_iter(
                    iter(file_list),
                    value,
                )

            elif filter_name == '--data-xform':
                file_list = DataTransform.wrap_iter(
                    iter(file_list),
                    value,
                )

            else:
                raise RuntimeError(f"Unrecognized filter {filter_name}")

    s3_backup.do_sync(
        file_list=iter(file_list),
        s3_bucket=args.bucket,
        cache_db=sqlite3.connect(args.cache_file),
        storage_class=args.storage_class,
        dry_run=args.dry_run,
    )
    logger.log(logging.INFO+1, orig_file_list.summary())


if __name__ == '__main__':
    main()
