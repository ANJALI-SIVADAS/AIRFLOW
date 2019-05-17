from abc import abstractmethod, ABCMeta
import argparse
from datetime import datetime, date
import logging
from pathlib import Path
import csv
import shutil
import requests
import sys

import s3fs

from pymodules.dbwrapper import PostgresqlWrapper
from pymodules.command import SSHConnection, local_cmd
import config as cfg

# create module level logger
log = logging.getLogger(__name__)
log.setLevel('INFO')

#################################################################################
# Utility Functions
#################################################################################


def configure_logging():
    """call this to return a logger for a top-level script
    e.g.
       from fetchload import configure_logging
       log = configure_logging()
    """
    log = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel('INFO')
    return log

#################################################################################
# Command Line Parsing for Fetch and Load scripts
#################################################################################


def iso_date(datestr):
    try:
        date_obj = datetime.strptime(datestr, '%Y-%m-%d').date()
        return date_obj
    except ValueError as e:
        raise argparse.ArgumentTypeError(f'{datestr} is not a valid date in YYYY-MM-DD format')


def parse_arguments(argv, fileformats):
    parser = argparse.ArgumentParser(argv)
    parser.add_argument('--rundate', dest='run_date',
                        help='the run date in YYYY-MM-DD format',
                        type=iso_date,
                        required=True)

    parser.add_argument('--fetch', action='store_true', default=False,
                        help='fetch data from API and archive to S3')

    parser.add_argument('--load', action='store_true', default=False,
                        help='retrieve data from S3 and load to database')

    parser.add_argument('--fileformat', help='select one of the listed source formats',
                        choices=fileformats)

    args = parser.parse_args()

    if not args.fetch and not args.load:
        raise argparse.ArgumentTypeError('You must specify either --fetch, --load or both')

    return args

#################################################################################

####################################


class FetchLoadProcessor:
    __metaclass__ = ABCMeta

    def __init__(self,
                 run_date,
                 source_system,
                 filename,
                 api_return_type,
                 target_table_name=None
                 ):
        self.run_date = run_date
        self.run_date_string = datetime.strftime(run_date, '%Y-%m-%d')
        self.run_date_YYYYMMDD = run_date.strftime('%Y%m%d')
        self.local_folder = Path(cfg.ETL_DATA_HOME) / source_system / self.run_date_YYYYMMDD
        if filename:
            self.file_path = self.local_folder / filename
            self.s3_path = f'{cfg.ETL_S3_BUCKET}/{source_system}/{self.run_date_YYYYMMDD}/{filename}'
        else:
            self.file_path = self.local_folder
            self.s3_path = f'{cfg.ETL_S3_BUCKET}/{source_system}/{self.run_date_YYYYMMDD}'
        self.s3 = s3fs.S3FileSystem(anon=False)
        self.api_return_type = api_return_type
        self.sftp_path = f'{cfg.ETL_DATA_HOME}/{filename}'
        self.sftp_path_server = cfg.SFTP_PATH_SERVER
        self.sftp_user_server = cfg.SFTP_USER_SERVER
        self.sftp_name_server = cfg.SFTP_NAME_SERVER

    @staticmethod
    def rmdir_if_empty(path):
        """removes path if path is an empty folder"""
        log.info(f'Attempting removal of directory {path}')
        try:
            Path(path).rmdir()
        except (OSError, FileNotFoundError) as e:
            log.info(e)

    @abstractmethod
    def fetch_from_api(self):
        """This method MUST be over-ridden and should return the data from the API / sftp
           as an iterator"""
        print('Should be overridden')
        pass

    def _cleanup(self):
        log.info(f'deleting {self.file_path}')
        try:
            self.file_path.unlink()
        except (FileNotFoundError, IsADirectoryError) as e:
            print(e)
            print('folder is not empty')
        self.rmdir_if_empty(self.local_folder)

    def sftp_fetch_and_archive_to_s3(self):
        if not cfg.FETCH_DATA_FROM_SOURCE:
            log.info(f'copying all files from {self.local_folder} to {self.s3_path}')
            print(f'FETCH_DATA_FROM_SOURCE is set to {cfg.FETCH_DATA_FROM_SOURCE} returning for fetch')
            return
        self.local_folder.mkdir(parents=True, exist_ok=True)
        with SSHConnection(user=self.sftp_user_server, server=self.sftp_name_server,
                           use_persistent_connection=False) as ssh:
            for files in self.fetch_from_api():
                sftp_dir = f'{self.sftp_path_server}{files}'
                log.info(f'copying files to {self.local_folder} \n from {sftp_dir}')
                ssh.get_file(from_path=sftp_dir, to_path=self.local_folder)

        log.info(f'copying all files from {self.local_folder} to {self.s3_path}')
        self.check_inserted_files()
        self.put_to_s3_using_cli()

    def put_to_s3(self, file_path, s3_path):
        log.info(f'copying {file_path} to {s3_path}')
        self.s3.put(file_path, s3_path)
        self._cleanup()

    def put_to_s3_using_cli(self):
        """Override this method if required. if need to copy one file"""
        aws_s3_cmd = f'aws s3 cp {self.local_folder}/ {self.s3_path} --recursive'
        local_cmd(aws_s3_cmd)
        all_files = Path(self.local_folder).glob('**/*')
        for files in all_files:
            files.unlink()

    def check_inserted_files(self):
        """Override this method if required"""
        """this is for checking the downloaded files """
        pass

    def fetch_and_archive_to_s3(self):
        if not cfg.FETCH_DATA_FROM_SOURCE:
            log.info(f'copying all files from {self.local_folder} to {self.s3_path}')
            print(f'FETCH_DATA_FROM_SOURCE is set to {cfg.FETCH_DATA_FROM_SOURCE} returning for fetch')
            return
        self.local_folder.mkdir(parents=True, exist_ok=True)
        log.info(f'writing locally to {self.file_path}')
        # TODO: improve memory usage?? test with other api calls
        if self.api_return_type == 'generator':
            data = iter(self.fetch_from_api())
            with self.file_path.open(mode='w+', encoding='utf8') as f:
                wr = csv.writer(f, delimiter=',')
                wr.writerows(data)
            self.put_to_s3(self.file_path, self.s3_path)
        elif self.api_return_type == 'list':
            with self.file_path.open(mode='w') as f:
                f.write(self.fetch_from_api())
            self.put_to_s3(self.file_path, self.s3_path)
        else:
            with self.file_path.open(mode='wb') as f:
                f.write(self.fetch_from_api())
            self.put_to_s3(self.file_path, self.s3_path)


########################################################
# Overrideable methods for loading phase
########################################################
    def get_from_s3(self, file_path, s3_path):
        log.info(f'copying {file_path} to {s3_path}')
        self.s3.get(s3_path, file_path)

    def get_from_s3_using_cli(self):
        """Override this method if required according to need to copy one file"""
        aws_s3_cmd = f'aws s3 cp {self.s3_path} {self.local_folder} --recursive'
        local_cmd(aws_s3_cmd)

    def pre_copy_transform(self):
        """Override this method if required"""
        pass

    @abstractmethod
    def pre_copy_sql(self, db):
        """This method MUST be over-ridden and should provide ddl for creating the table if it does
        not exist"""
        #db.execddl("create table if not exists and truncate table ddl")
        pass

    def copy_to_db(self, db):
        """This method MUST be over-ridden"""
        log.info("copying {self.filepath} to {tablename}")
        with self.from_path.open() as f:
            db.csv_to_table(filehandle=f, tablename=tablename)

    def load_from_s3_to_db(self):
        self.local_folder.mkdir(parents=True, exist_ok=True)
        log.info(f'copying {self.s3_path} to {self.file_path}')
        self.get_from_s3(self.file_path, self.s3_path)
        self.pre_copy_transform()
        #TODO: externalize DB params
        db = PostgresqlWrapper(**cfg.SQL_PARAMS)
        self.pre_copy_sql(db)
        self.copy_to_db(db)
        db.commit()
        self._cleanup()

    def sftp_load_from_s3_to_db(self):
        self.local_folder.mkdir(parents=True, exist_ok=True)
        log.info(f'copying {self.s3_path} to {self.local_folder}')
        self.get_from_s3_using_cli()
        self.pre_copy_transform()
        db = PostgresqlWrapper(**cfg.SQL_PARAMS)
        db.commit()
        self.pre_copy_sql(db)
        self.copy_to_db(db)
        db.commit()
        self._cleanup()

    def sftp_add_file_name_for_cleanup(self, file_name):
        self.file_path = self.local_folder / file_name
        self._cleanup()
