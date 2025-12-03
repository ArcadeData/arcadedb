#!/usr/bin/env python3
"""
Download sample datasets for ArcadeDB import examples.

We introduce NULL values in some fields to test handling of missing data.

This script downloads the MovieLens datasets, which includes:
- movies.csv: Movie information with titles and genres
    - columns: movieId,title,genres
- ratings.csv: User ratings
    - columns: userId,movieId,rating,timestamp
- tags.csv: User-generated tags
    - columns: userId,movieId,tag,timestamp
- links.csv: IMDb and TMDb IDs
    - columns: movieId,imdbId,tmdbId

Available dataset sizes:
- large: ml-large (~265 MB, ~33M ratings) - Realistic performance testing
- small: ml-small (~1 MB, ~100K ratings) - Lightweight, quick testing


License: Free to use for educational purposes
Source: https://grouplens.org/datasets/movielens/

Usage:
    python download_sample_data.py --size large  # Downloads large dataset
    python download_sample_data.py --size small  # Downloads small dataset
"""

import argparse
import os
import shutil
import urllib.request
import zipfile
from pathlib import Path


def introduce_null_values(extract_dir):
    """
    Modify CSV files to introduce NULL values for testing.

    This demonstrates how the importer handles:
    - Empty strings
    - Missing values
    - Incomplete records
    """
    import csv
    import random

    print("\n🔧 Introducing NULL values in all CSV files for testing...")

    # Modify movies.csv - make some genres empty
    movies_path = extract_dir / "movies.csv"
    if movies_path.exists():
        rows = []
        with open(movies_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Make ~3% of genres empty (to test NULL in genre field)
                if random.random() < 0.03:
                    row["genres"] = ""
                rows.append(row)

        with open(movies_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["movieId", "title", "genres"])
            writer.writeheader()
            writer.writerows(rows)

        null_genres = sum(1 for r in rows if not r["genres"])
        print(f"   ✅ movies.csv: {null_genres} NULL genres")

    # Modify ratings.csv - make some timestamps empty
    ratings_path = extract_dir / "ratings.csv"
    if ratings_path.exists():
        rows = []
        with open(ratings_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Make ~2% of timestamps empty (to test NULL in numeric timestamp)
                if random.random() < 0.02:
                    row["timestamp"] = ""
                rows.append(row)

        with open(ratings_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=["userId", "movieId", "rating", "timestamp"]
            )
            writer.writeheader()
            writer.writerows(rows)

        null_timestamps = sum(1 for r in rows if not r["timestamp"])
        print(f"   ✅ ratings.csv: {null_timestamps} NULL timestamps")

    # Modify links.csv - make some imdbId and tmdbId values NULL
    links_path = extract_dir / "links.csv"
    if links_path.exists():
        rows = []
        with open(links_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Make ~10% of imdbId values empty
                if random.random() < 0.1:
                    row["imdbId"] = ""
                # Make ~15% of tmdbId values empty
                if random.random() < 0.15:
                    row["tmdbId"] = ""
                rows.append(row)

        # Write back
        with open(links_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["movieId", "imdbId", "tmdbId"])
            writer.writeheader()
            writer.writerows(rows)

        null_imdb = sum(1 for r in rows if not r["imdbId"])
        null_tmdb = sum(1 for r in rows if not r["tmdbId"])
        print(f"   ✅ links.csv: {null_imdb} NULL imdbId, {null_tmdb} NULL tmdbId")

    # Modify tags.csv - make some tag values empty
    tags_path = extract_dir / "tags.csv"
    if tags_path.exists():
        rows = []
        with open(tags_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Make ~5% of tags empty (to test NULL in string fields)
                if random.random() < 0.05:
                    row["tag"] = ""
                rows.append(row)

        with open(tags_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=["userId", "movieId", "tag", "timestamp"]
            )
            writer.writeheader()
            writer.writerows(rows)

        null_tags = sum(1 for r in rows if not r["tag"])
        print(f"   ✅ tags.csv: {null_tags} NULL tags")


def download_movielens(size="large"):
    """Download and extract MovieLens dataset.

    Args:
        size: 'large' (~265 MB, ~33M ratings) or 'small' (~1 MB, ~100K ratings)
    """

    # Create data directory
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)

    # Dataset configurations
    datasets = {
        "small": {
            "url": "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip",
            "dirname": "ml-small",
            "description": "~100K ratings, ~9K movies",
            "size_mb": "~1 MB",
        },
        "large": {
            "url": "https://files.grouplens.org/datasets/movielens/ml-latest.zip",
            "dirname": "ml-large",
            "description": "~33M ratings, ~86K movies",
            "size_mb": "~265 MB",
        },
    }

    if size not in datasets:
        raise ValueError(f"Unknown dataset size: {size}. Choose 'small' or 'large'")

    config = datasets[size]
    url = config["url"]
    dirname = config["dirname"]
    zip_path = data_dir / f"{size}.zip"
    extract_dir = data_dir / dirname

    # Check if already downloaded
    if extract_dir.exists():
        print(f"✅ Dataset already exists at: {extract_dir}")
        print(f"   Size: {config['description']} ({config['size_mb']})")
        print()
        for csv_file in ["movies.csv", "ratings.csv", "tags.csv", "links.csv"]:
            file_path = extract_dir / csv_file
            if file_path.exists():
                size_mb = file_path.stat().st_size / (1024 * 1024)
                print(f"   - {csv_file}: {size_mb:.1f} MB")

        # Ask if user wants to re-introduce NULL values
        print("\n💡 To re-introduce NULL values, delete the data directory and re-run.")
        return extract_dir

    print(f"📥 Downloading MovieLens {size} dataset")
    print(f"   Description: {config['description']} ({config['size_mb']})")
    print(f"   URL: {url}")
    print("   This may take a few minutes...")
    print()

    try:
        # Download with progress
        def report_progress(block_num, block_size, total_size):
            downloaded = block_num * block_size
            percent = min(100, (downloaded / total_size) * 100) if total_size > 0 else 0
            downloaded_mb = downloaded / (1024 * 1024)
            total_mb = total_size / (1024 * 1024)
            print(
                f"\r   Progress: {percent:.1f}% "
                f"({downloaded_mb:.1f}/{total_mb:.1f} MB)",
                end="",
            )

        urllib.request.urlretrieve(url, zip_path, reporthook=report_progress)
        print()  # New line after progress
        print(f"✅ Downloaded to: {zip_path}")

        # Extract
        print("📦 Extracting...")
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            # Extract to temporary directory first (since zip contains ml-latest or ml-latest-small folder)
            temp_extract = data_dir / "temp_extract"
            zip_ref.extractall(temp_extract)

            # Find the extracted folder (ml-latest or ml-latest-small)
            extracted_folders = list(temp_extract.iterdir())
            if len(extracted_folders) == 1 and extracted_folders[0].is_dir():
                source_dir = extracted_folders[0]
                # Move contents to our target directory
                if extract_dir.exists():
                    shutil.rmtree(extract_dir)
                shutil.move(str(source_dir), str(extract_dir))
                # Clean up temp directory
                shutil.rmtree(temp_extract)
            else:
                raise Exception(f"Unexpected zip structure in {temp_extract}")

        print(f"✅ Extracted to: {extract_dir}")

        # Introduce NULL values for testing
        introduce_null_values(extract_dir)

        # Show file sizes
        print("\n📊 Dataset contents:")
        for csv_file in extract_dir.glob("*.csv"):
            size_mb = csv_file.stat().st_size / (1024 * 1024)
            print(f"   - {csv_file.name}: {size_mb:.1f} MB")

        # Clean up zip file
        zip_path.unlink()
        print("\n🧹 Cleaned up zip file")

        return extract_dir

    except Exception as e:
        print(f"❌ Error downloading dataset: {e}")
        print(f"   You can manually download from: {url}")
        raise


def main():
    """Main function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Download MovieLens dataset for ArcadeDB examples",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python download_sample_data.py --size large  # Downloads large dataset (~265 MB)
  python download_sample_data.py --size small  # Downloads small dataset (~1 MB)

Dataset sizes:
  large - ml-large (~33M ratings, ~86K movies, ~265 MB)
  small - ml-small (~100K ratings, ~9K movies, ~1 MB)
        """,
    )
    parser.add_argument(
        "--size",
        choices=["small", "large"],
        required=True,
        help="Dataset size to download (required)",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("📥 MovieLens Dataset Download")
    print("=" * 70)
    print()

    extract_dir = download_movielens(size=args.size)

    print()
    print("=" * 70)
    print("✅ Dataset ready!")
    print("=" * 70)
    print()
    print("💡 Use this dataset in examples:")
    print(f"   data_dir = Path('{extract_dir}')")
    print("   movies_csv = data_dir / 'movies.csv'")
    print("   ratings_csv = data_dir / 'ratings.csv'")
    print()
    print("📚 Dataset info:")
    if args.size == "large":
        print("   - ~86,000 movies")
        print("   - ~33,000,000 ratings")
        print("   - ~280,000 users")
    else:
        print("   - ~9,000 movies")
        print("   - ~100,000 ratings")
        print("   - ~600 users")
    print("   - Licensed for educational use")
    print()
    if args.size == "large":
        print("💡 For quick testing with smaller dataset, try:")
        print("   python download_sample_data.py --size small")
    else:
        print("💡 For realistic performance testing with larger dataset, try:")
        print("   python download_sample_data.py --size large")
    print()


if __name__ == "__main__":
    main()
