import argparse
from datetime import datetime, date
from pathlib import Path
import sys

import requests
from requests.auth import HTTPBasicAuth
import s3fs

import config as cfg

from pymodules.fetchload import (FetchLoadProcessor,
                       configure_logging,
                       parse_arguments)


class CompetitorRankings(FetchLoadProcessor):

    def fetch_from_api(self):
        log.info(f'fetching from {cfg.HITWISE_URL}')
        session = requests.Session()
        session.auth = HTTPBasicAuth(cfg.HITWISE_USERNAME, cfg.HITWISE_PASSWORD)
        r = session.get(cfg.HITWISE_URL)
        r.raise_for_status()
        return r.content

    def pre_copy_transform(self):
        import pandas as pd

        def xl_to_csv(sheet, csv_filename):
            log.info(f"converting xl sheet {sheet} to {csv_filename}")
            csv_data = pd.read_excel(self.file_path, sheet_name=sheet, header=0)
            cols = ["week_ending_date", "website", "domain", "total_weekly_visits", "weekly_visits_share"]
            csv_data.columns = cols
            csv_data.total_weekly_visits = csv_data.total_weekly_visits.str.replace('<', '')
            csv_data.weekly_visits_share = csv_data.weekly_visits_share.str.replace('<', '')
            csv_data.weekly_visits_share = csv_data.weekly_visits_share.str.replace('%', '')
            csv_data.to_csv(self.local_folder / csv_filename, index=False)

        xl_to_csv(sheet='DFS Competitors', csv_filename='dfs_competitors.csv')
        xl_to_csv(sheet='DFS Competitors (sections)', csv_filename='dfs_competitors_sections.csv')

    def pre_copy_sql(self, db):
        db.execddl("""
        create table if not exists ddd_staging.hitwise (
            week_ending_date date null,
            website varchar null,
            "domain" varchar null,
            total_weekly_visits float8 null,
            weekly_visits_share float8 null
        );
        truncate table ddd_staging.hitwise;

        create table if not exists ddd_staging.hitwise_wider_comparison (
            week_ending_date date null,
            website varchar null,
            "domain" varchar null,
            total_weekly_visits float8 null,
            weekly_visits_share float8 null
        );
        truncate table ddd_staging.hitwise_wider_comparison;
        """.split(';'))

    def copy_to_db(self, db):
        def load_table(from_path, tablename):
            log.info(f"copying {from_path} to {tablename}")
            with from_path.open() as f:
                db.csv_to_table(filehandle=f, tablename=tablename)
            # remove file from local storage
            from_path.unlink()

        load_table(self.local_folder / 'dfs_competitors.csv', 'ddd_staging.hitwise')
        load_table(self.local_folder / 'dfs_competitors_sections.csv', 'ddd_staging.hitwise_wider_comparison')

    def update_db_after_push(self, db):
        db.execddl(f"""
        INSERT INTO ddd_staging.check_staging_completeness(created_date,hitwise) 
        VALUES('{self.run_date_string}','hitwise')
        ON CONFLICT (created_date)
        DO
         UPDATE
           SET hitwise = EXCLUDED.hitwise;""")

def main(argv):
    global log
    log = configure_logging()

    file_formats = {
        'competitor_rankings': {'filename': 'competitor_rankings_weekly.xls', 'klass': CompetitorRankings,
                                'api_return_type': 'bytes'},
    }

    args = parse_arguments(argv, fileformats=file_formats.keys())
    processor_class = file_formats[args.fileformat]['klass']

    processor = processor_class(
        run_date=args.run_date,
        source_system='hitwise',
        filename=file_formats[args.fileformat]['filename'],
        api_return_type=file_formats[args.fileformat]['api_return_type'])

    if args.fetch:
        processor.fetch_and_archive_to_s3()
    if args.load:
        processor.load_from_s3_to_db()


if __name__ == '__main__':
    main(sys.argv)

