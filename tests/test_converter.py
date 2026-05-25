from utils.converter import to_dataframe, to_json, to_csv, from_dataframe, from_json


def test_to_json():
    rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    result = to_json(rows)
    assert '"id": 1' in result
    assert '"name": "b"' in result


def test_from_json():
    text = '[{"id": 1, "name": "a"}]'
    rows = from_json(text)
    assert len(rows) == 1
    assert rows[0]["id"] == 1


def test_to_csv():
    rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    result = to_csv(rows)
    lines = result.strip().splitlines()
    assert lines[0] == "id,name"
    assert len(lines) == 3


def test_to_csv_empty():
    assert to_csv([]) == ""


def test_to_dataframe():
    rows = [{"id": 1, "val": "x"}, {"id": 2, "val": "y"}]
    df = to_dataframe(rows)
    assert list(df.columns) == ["id", "val"]
    assert len(df) == 2


def test_from_dataframe():
    import pandas as pd
    df = pd.DataFrame({"id": [1, 2], "val": ["x", "y"]})
    rows = from_dataframe(df)
    assert len(rows) == 2
    assert rows[0] == {"id": 1, "val": "x"}
