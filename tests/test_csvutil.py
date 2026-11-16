"""Unit tests for strict CSV parsing and median calculation."""
import pytest

from app.csvutil import CsvValidationError, median_nm, parse_thickness_csv


def test_basic_with_header():
    parsed = parse_thickness_csv(b"wafer_no,thickness_nm\nW1,500\nW2,510\n")
    assert parsed.count == 2
    assert parsed.rows == [("W1", 500), ("W2", 510)]


def test_headerless_and_chinese_header():
    p1 = parse_thickness_csv(b"W1,500\nW2,510\n")
    assert p1.rows == [("W1", 500), ("W2", 510)]
    p2 = parse_thickness_csv("晶圆号,厚度nm\n片1,10\n片2,20\n".encode())
    assert p2.rows[0] == ("片1", 10)


def test_empty_rejected():
    with pytest.raises(CsvValidationError):
        parse_thickness_csv(b"")
    with pytest.raises(CsvValidationError):
        parse_thickness_csv(b"wafer_no,thickness_nm\n")


@pytest.mark.parametrize("bad", [
    b"wafer_no,thickness_nm\nW1,0\n",          # zero thickness
    b"wafer_no,thickness_nm\nW1,-5\n",         # negative
    b"wafer_no,thickness_nm\nW1,500.5\n",      # float
    b"wafer_no,thickness_nm\nW1,\n",           # empty
    b"wafer_no,thickness_nm\nW1,abc\n",        # non numeric
    b"wafer_no,thickness_nm\nW1,500,9\n",      # three columns
    b"wafer_no,thickness_nm\nW1,1 2\n",        # internal space
])
def test_invalid_thickness_or_shape(bad):
    with pytest.raises(CsvValidationError):
        parse_thickness_csv(bad)


def test_duplicate_wafer_rejected():
    with pytest.raises(CsvValidationError):
        parse_thickness_csv(b"wafer_no,thickness_nm\nW1,10\nW1,11\n")


def test_limit_1000():
    ok = ("\n".join(f"W{i},100" for i in range(1000)) + "\n").encode()
    assert parse_thickness_csv(ok).count == 1000
    too_many = ("\n".join(f"W{i},100" for i in range(1001)) + "\n").encode()
    with pytest.raises(CsvValidationError):
        parse_thickness_csv(too_many)


def test_median_odd_and_even():
    assert median_nm([3, 1, 2]) == 2.0
    assert median_nm([4, 1, 3, 2]) == 2.5
    assert median_nm([5, 5, 5, 5, 5]) == 5.0
