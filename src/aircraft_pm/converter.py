from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import h5py
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq


@dataclass
class ConversionIssue:
    file: str
    dataset: str
    reason: str


def _load_mapping(mapping_path: str | None) -> dict[str, Any]:
    if not mapping_path:
        return {}
    with open(mapping_path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _iter_datasets(h5f: h5py.File):
    def visitor(name: str, obj: Any):
        if isinstance(obj, h5py.Dataset):
            datasets.append((name, obj))

    datasets: list[tuple[str, h5py.Dataset]] = []
    h5f.visititems(visitor)
    return datasets


def _is_supported(dataset: h5py.Dataset) -> bool:
    return dataset.ndim in (1, 2) and dataset.dtype.kind in {"i", "u", "f"}


def _to_frame(
    chunk: Any,
    dataset_name: str,
    source_file: str,
    mapping: dict[str, Any],
    row_offset: int,
) -> pd.DataFrame:
    config = mapping.get("datasets", {}).get(f"/{dataset_name}", {})
    columns = config.get("columns")

    if getattr(chunk, "ndim", 1) == 1:
        frame = pd.DataFrame({"value": chunk})
    else:
        frame = pd.DataFrame(chunk)
    if columns and len(columns) == frame.shape[1]:
        frame.columns = columns
    elif frame.shape[1] > 1:
        frame.columns = [f"col_{i}" for i in range(frame.shape[1])]

    frame["source_file"] = source_file
    frame["source_dataset"] = f"/{dataset_name}"
    frame["row_index"] = range(row_offset, row_offset + len(frame))

    for candidate in ("engine_id", "unit_id"):
        if candidate in frame.columns:
            frame["engine_id"] = frame[candidate]
            break
    engine_col = config.get("engine_id_column")
    if engine_col and engine_col in frame.columns:
        frame["engine_id"] = frame[engine_col]

    return frame


def validate_frame(frame: pd.DataFrame) -> tuple[bool, str]:
    if frame.empty:
        return False, "empty chunk"
    if frame.columns.duplicated().any():
        return False, "duplicate columns"
    if "source_file" not in frame.columns or "source_dataset" not in frame.columns:
        return False, "missing metadata columns"
    return True, ""


def convert_hdf5_to_parquet(
    input_dir: str,
    output_dir: str,
    quarantine_report: str,
    chunk_size: int = 10_000,
    mapping_path: str | None = None,
) -> list[ConversionIssue]:
    mapping = _load_mapping(mapping_path)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    issues: list[ConversionIssue] = []

    files = list(Path(input_dir).rglob("*.h5")) + list(Path(input_dir).rglob("*.hdf5"))
    for file_path in files:
        with h5py.File(file_path, "r") as h5f:
            for dataset_name, dataset in _iter_datasets(h5f):
                if not _is_supported(dataset):
                    issues.append(
                        ConversionIssue(
                            str(file_path),
                            f"/{dataset_name}",
                            "unsupported shape or dtype",
                        )
                    )
                    continue

                total_rows = dataset.shape[0]
                for start in range(0, total_rows, chunk_size):
                    end = min(total_rows, start + chunk_size)
                    chunk = dataset[start:end] if dataset.ndim == 1 else dataset[start:end, :]
                    frame = _to_frame(chunk, dataset_name, str(file_path), mapping, start)
                    valid, reason = validate_frame(frame)
                    if not valid:
                        issues.append(ConversionIssue(str(file_path), f"/{dataset_name}", reason))
                        continue

                    table = pa.Table.from_pandas(frame, preserve_index=False)
                    pq.write_to_dataset(
                        table,
                        root_path=str(out),
                        partition_cols=["source_dataset"],
                        compression="snappy",
                        existing_data_behavior="overwrite_or_ignore",
                    )

    report_path = Path(quarantine_report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as fh:
        for issue in issues:
            fh.write(json.dumps(issue.__dict__) + "\n")
    return issues


def canonicalize_parquet(input_path: str, output_path: str) -> None:
    dataset = ds.dataset(input_path, format="parquet")
    table = dataset.to_table()
    pq.write_table(table, output_path, compression="snappy")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Convert HDF5 datasets to canonical Parquet")
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--quarantine-report", default="./reports/quarantine.jsonl")
    parser.add_argument("--chunk-size", type=int, default=10000)
    parser.add_argument("--mapping", default=None)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    convert_hdf5_to_parquet(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        quarantine_report=args.quarantine_report,
        chunk_size=args.chunk_size,
        mapping_path=args.mapping,
    )


if __name__ == "__main__":
    main()
