from pathlib import Path

import h5py
import pandas as pd

from aircraft_pm.converter import convert_hdf5_to_parquet, validate_frame


def test_validate_frame_rejects_empty():
    ok, reason = validate_frame(pd.DataFrame())
    assert not ok
    assert reason == "empty chunk"


def test_hdf5_to_parquet_conversion(tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "out"
    report = tmp_path / "quarantine.jsonl"
    input_dir.mkdir()

    with h5py.File(input_dir / "sample.h5", "w") as h5f:
        h5f.create_dataset("train/table", data=[[1, 1, 10.0, 20.0, 30.0, 99.0]])
        h5f.create_dataset("bad/strings", data=[b"a", b"b"])

    issues = convert_hdf5_to_parquet(str(input_dir), str(output_dir), str(report), chunk_size=1)

    assert any("unsupported" in issue.reason for issue in issues)
    assert report.exists()
    assert list(output_dir.rglob("*.parquet"))
