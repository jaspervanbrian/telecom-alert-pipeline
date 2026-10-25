from pathlib import Path
import pyarrow.parquet as pq
import requests

DATASET_ID = "electricsheepafrica/africa-synth-telecom-network-event-logs-nigeria"
PARQUET_API_URL = f"https://huggingface.co/api/datasets/{DATASET_ID}/parquet/default/train"
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT_FILE = DATA_DIR / "raw_telecom_events.parquet"


def download_dataset() -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if OUTPUT_FILE.exists():
        print(f"[INFO] Dataset already exists at: {OUTPUT_FILE}")
        return OUTPUT_FILE

    print(f"[INFO] Fetching Parquet URL from Hugging Face for {DATASET_ID}...")
    resp = requests.get(PARQUET_API_URL, timeout=30)
    resp.raise_for_status()
    parquet_urls = resp.json()

    if not parquet_urls:
        raise RuntimeError(f"No Parquet files found at {PARQUET_API_URL}")

    target_url = parquet_urls[0]
    print(f"[INFO] Downloading {target_url} -> {OUTPUT_FILE} ...")

    with requests.get(target_url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(OUTPUT_FILE, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    print(f"[INFO] Download complete ({OUTPUT_FILE.stat().st_size / (1024 * 1024):.2f} MB)")
    return OUTPUT_FILE


def inspect_dataset(parquet_path: Path) -> None:
    table = pq.read_table(parquet_path)
    print("\n=== Dataset Summary ===")
    print(f"Total Rows: {table.num_rows:,}")
    print("Schema:")
    for field in table.schema:
        print(f"  - {field.name}: {field.type}")

    sample_rows = table.slice(0, 3).to_pylist()
    print("\n=== First 3 Rows ===")
    for i, row in enumerate(sample_rows, start=1):
        print(f"Row {i}: {row}")


if __name__ == "__main__":
    path = download_dataset()
    inspect_dataset(path)
