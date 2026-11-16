"""Strict parsing and validation of the uploaded thickness CSV.

Expected layout (with or without a header row)::

    wafer_no,thickness_nm
    W001,512
    W002,508

Rules enforced here:
* 1..1000 wafers per batch;
* wafer numbers unique within the batch and non-empty;
* thickness is a strictly positive integer (floats, signs, spaces inside the
  number, empty cells are all rejected);
* exactly two columns per data row.
"""
from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from typing import List, Tuple

from .config import settings

WAFER_HEADER_TOKENS = {"wafer_no", "wafer", "waferid", "wafer_id", "晶圆号", "片号"}
THICKNESS_HEADER_TOKENS = {"thickness_nm", "thickness", "厚度nm", "厚度_nm", "厚度"}


class CsvValidationError(ValueError):
    """Raised when an uploaded CSV fails any structural rule."""


@dataclass(frozen=True)
class ParsedCsv:
    rows: List[Tuple[str, int]]

    @property
    def count(self) -> int:
        return len(self.rows)


def _is_header(first: List[str]) -> bool:
    if len(first) != 2:
        return False
    a, b = first[0].strip().lower(), first[1].strip().lower()
    return a in WAFER_HEADER_TOKENS and b in THICKNESS_HEADER_TOKENS


def parse_thickness_csv(raw: bytes) -> ParsedCsv:
    if not raw or not raw.strip():
        raise CsvValidationError("CSV 为空 / CSV is empty")
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CsvValidationError(f"CSV 必须为 UTF-8 编码 / invalid UTF-8: {exc}") from exc

    reader = csv.reader(io.StringIO(text))
    try:
        records = list(reader)
    except csv.Error as exc:
        raise CsvValidationError(f"CSV 结构错误 / malformed CSV: {exc}") from exc

    records = [r for r in records if r and not (len(r) == 1 and not r[0].strip())]
    if not records:
        raise CsvValidationError("CSV 无有效数据行 / no data rows")
    if _is_header(records[0]):
        records = records[1:]
    if not records:
        raise CsvValidationError("CSV 只有表头没有数据 / header without data rows")

    rows: List[Tuple[str, int]] = []
    seen: set[str] = set()
    for line_no, rec in enumerate(records, start=1):
        if len(rec) != 2:
            raise CsvValidationError(
                f"第 {line_no} 行应为 2 列，实际 {len(rec)} 列 / expected 2 columns")
        wafer_no = rec[0].strip()
        raw_thickness = rec[1].strip()
        if not wafer_no:
            raise CsvValidationError(f"第 {line_no} 行晶圆号为空 / empty wafer number")
        if wafer_no in seen:
            raise CsvValidationError(
                f"晶圆号批内重复: {wafer_no} / duplicate wafer number within batch")
        # int() accepts "+5" and whitespace; reject anything not [0-9]+ after strip.
        if not raw_thickness.isdigit():
            raise CsvValidationError(
                f"第 {line_no} 行厚度必须为正整数: {raw_thickness!r} / "
                "thickness must be a positive integer")
        thickness = int(raw_thickness)
        if thickness <= 0:
            raise CsvValidationError(
                f"第 {line_no} 行厚度必须 > 0 / thickness must be positive")
        seen.add(wafer_no)
        rows.append((wafer_no, thickness))

    if not rows:
        raise CsvValidationError("CSV 无有效数据行 / no data rows")
    if len(rows) > settings.max_wafers:
        raise CsvValidationError(
            f"每批最多 {settings.max_wafers} 片，实际 {len(rows)} 片 / too many wafers")
    return ParsedCsv(rows=rows)


def median_nm(values: List[int]) -> float:
    """Median; for even counts returns the mean of the two middle values."""
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2:
        return float(ordered[mid])
    return (ordered[mid - 1] + ordered[mid]) / 2.0
