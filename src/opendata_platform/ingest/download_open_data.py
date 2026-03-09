from __future__ import annotations

import argparse
from pathlib import Path
from urllib.request import urlretrieve


DEFAULT_URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx"


def download_open_dataset(out_dir: str | Path, url: str = DEFAULT_URL) -> Path:
    """Optional utility to download an open dataset when internet is available."""
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    dest = out_path / "online_retail.xlsx"
    urlretrieve(url, dest)
    return dest


def main() -> None:
    parser = argparse.ArgumentParser(description="Download optional open e-commerce dataset")
    parser.add_argument("--out", default="data/raw_open", help="Output folder")
    parser.add_argument("--url", default=DEFAULT_URL, help="Dataset URL")
    args = parser.parse_args()

    output = download_open_dataset(args.out, args.url)
    print(f"Downloaded dataset to: {output}")


if __name__ == "__main__":
    main()
