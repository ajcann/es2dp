import argparse
import json
import logging
import os
from datetime import datetime
from typing import Dict, List
from urllib.parse import urlparse
from uuid import uuid4

import awswrangler as wr
import pandas as pd
import ray
import requests

DATASETS_RELEASE_API = "https://api.semanticscholar.org/datasets/v1/release"
DATASET_TYPES = ("abstracts", "papers")  # TODO: Skipping "citations" for purposes of demo
BUCKET = "ought-s2ag"
S3_PATH = f"s3://{BUCKET}/s2ag"


class S2AGDownloadLinks:
    def __init__(
        self, dataset_types=DATASET_TYPES, release_api=DATASETS_RELEASE_API, api_key=os.getenv("S2AG_API_KEY")
    ) -> None:
        self.api_key = api_key
        self.auth_headers = {
            "x-api-key": self.api_key,
        }
        self.release_api = release_api
        self.link_types = dataset_types
        self.release_name = self.get_latest_release()
        self.dataset_links = self.get_links()

    def get_links(self) -> Dict:
        links = {}
        for link_type in self.link_types:
            req = requests.get(
                f"{self.release_api}/{self.release_name}/dataset/{link_type}", headers=self.auth_headers  # type: ignore
            )
            req.raise_for_status()
            links[link_type] = json.loads(req.content).get('files')
        return links

    def get_releases(self) -> List:
        req = requests.get(self.release_api)
        req.raise_for_status()
        return json.loads(req.content)

    def get_latest_release(self) -> str:
        return max((datetime.strptime(release, "%Y-%m-%d"), release) for release in self.get_releases())[1]


class S2AGDownloadRunner:
    def __init__(self, s3_dest=S3_PATH) -> None:
        self.run_id = datetime.now().strftime('%Y%m%d-') + str(uuid4())[:8]  # TODO: Maybe add hh:mm as well
        self.s3_dest = s3_dest
        self.datasets_to_download = S2AGDownloadLinks()

    def run(self):
        logging.info(f"Starting Run ID: {self.run_id}")

        # Process S2AG datasets and upload to S3 as parquet files for next steps
        parquet_futures = {}
        for dataset_type, links in self.datasets_to_download.dataset_links.items():
            parquet_futures[dataset_type] = []
            # TODO: Switch to pool and queue
            for link in links:
                parquet_futures[dataset_type].append(
                    prepare_parquet.remote(dataset_type, link, self.run_id, self.s3_dest)
                )

        # Wait until we've finished preparing all parquet files TODO: Add logging
        parquet_files = {}
        for dataset_type, futures in parquet_futures.items():
            parquet_files[dataset_type] = ray.get(futures)


def download_file(url: str) -> str:
    """Returns filepath of downloaded file"""
    p_url = urlparse(url)
    filename = p_url.path.split("/")[-1]
    with requests.get(url, stream=True) as r:
        r.raise_for_status()  # TODO: Add handling to survive failed downloads / retry logic
        with open(filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=2**14):  # Chunk at  16KB  TODO: Perf test this
                f.write(chunk)
    return filename


@ray.remote(memory=5000 * 1024 * 1024)  # type: ignore
def prepare_parquet(dataset_type: str, link: str, run_id: str, s3_dest=S3_PATH):
    """Downloads S2AG jsonl.gz files, chunks into number of parquet files for memory efficiency and AWS Athena query
    speed, uploads to S3."""

    # Download file
    p_url = urlparse(link)
    logging.info(f"Processing {p_url.scheme}://{p_url.netloc}{p_url.path}")
    filepath = download_file(link)

    # Chunk file and upload to S3
    df_chunks = pd.read_json(
        filepath, lines=True, chunksize=5 * 10**5
    )  # TODO: Look into optimal chunksize and threading (want to saturate NIC)
    for i, df in enumerate(df_chunks):
        logging.info(f"Uploading parquet chunk {i} for {dataset_type}:{filepath}")  # TODO: Estimate total chunks

        # id_bucket creation - 100 partitions that will speed up future join
        df["id_bucket"] = df["corpusid"] % 10**2

        # Clean up df to more closely match destination schema
        if dataset_type == "papers":
            df.drop(columns=[], inplace=True)
            df["doi"] = [d.get('DOI') for d in df["externalids"]]
            df["author_list"] = [[d.get("name") for d in author] for author in df["authors"]]
            df.drop(
                columns=[
                    "externalids",
                    "authors",
                    "venue",
                    "s2fieldsofstudy",
                    "isopenaccess",
                    "influentialcitationcount",
                ],
                inplace=True,
            )
        elif dataset_type == "abstracts":
            df.drop(columns=["openaccessinfo"], inplace=True)

        wr.s3.to_parquet(
            df=df,
            path=f"{s3_dest}/{run_id}/{dataset_type}/",
            dataset=True,
            mode="append",
            concurrent_partitioning=True,
            partition_cols=["id_bucket"],
        )

    # Remove jsonl.gz file
    os.remove(filepath)


def main():
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
    ray.init()
    runner = S2AGDownloadRunner()
    runner.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--key', help='S2AG API Key')
    args = parser.parse_args()
    if args.key:  # TODO: Clean S2AG_API_KEY use up
        os.environ["S2AG_API_KEY"] = args.key
    main()
