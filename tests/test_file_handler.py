import json
import os
import tempfile

from utils.file_handler import read_json, write_json, read_csv, write_csv


def test_json_roundtrip():
    data = [{"id": 1, "name": "test"}]
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
        path = f.name
    try:
        write_json(path, data)
        result = read_json(path)
        assert result == data
    finally:
        os.unlink(path)


def test_csv_roundtrip():
    data = [{"id": "1", "name": "a"}, {"id": "2", "name": "b"}]
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
        path = f.name
    try:
        write_csv(path, data)
        result = read_csv(path)
        assert result == data
    finally:
        os.unlink(path)
