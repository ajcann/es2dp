"""Modify provided S2AG sample data so that all samples are joinable -
 These relationships are fake but demonstrate in principal what we're after"""

import gzip
import json
from pathlib import Path

SAMPLE_FILES = [
    './data/s2ag/abstracts/abstracts-sample.jsonl.gz',
    './data/s2ag/citations/citations-sample.jsonl.gz',
    './data/s2ag/papers/papers-sample.jsonl.gz',
]


def main():
    for sample in SAMPLE_FILES:
        sample_path = Path(sample)
        mod_path = sample_path.parent.joinpath("mod-" + sample_path.name)
        with gzip.open(sample) as f_in:
            with gzip.open(mod_path, "wb") as f_out:
                for i, line in enumerate(f_in):
                    record = json.loads(line)
                    record['corpusid'] = i
                    record_json = json.dumps(record) + "\n"
                    f_out.write(record_json.encode('utf-8'))


if __name__ == "__main__":
    main()
