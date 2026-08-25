from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any


def string_metrics(values: Iterable[Any], expected_patterns: Iterable[str] = ()) -> dict[str, Any]:
    raw = list(values)
    strings = [str(value) for value in raw if value is not None]
    lengths = [len(value) for value in strings]
    patterns = tuple(expected_patterns)
    signature_counts: dict[str, int] = {}
    for value in strings:
        signature = re.sub(r"[A-Z]", "A", re.sub(r"[a-z]", "a", re.sub(r"[0-9]", "9", value)))
        signature_counts[signature] = signature_counts.get(signature, 0) + 1
    return {
        "count": len(strings),
        "min_length": min(lengths) if lengths else None,
        "max_length": max(lengths) if lengths else None,
        "mean_length": sum(lengths) / len(lengths) if lengths else None,
        "empty_count": sum(value == "" for value in strings),
        "whitespace_only_count": sum(value.strip() == "" and value != "" for value in strings),
        "leading_trailing_whitespace_count": sum(value != value.strip() for value in strings),
        "uppercase_count": sum(value.isupper() for value in strings),
        "lowercase_count": sum(value.islower() for value in strings),
        "digit_only_count": sum(value.isdigit() for value in strings),
        "character_class_counts": {
            "alphabetic": sum(value.isalpha() for value in strings),
            "alphanumeric": sum(value.isalnum() for value in strings),
            "contains_whitespace": sum(any(char.isspace() for char in value) for value in strings),
        },
        "format_signatures": [
            {"signature": signature, "count": count, "share": count / len(strings)}
            for signature, count in sorted(
                signature_counts.items(), key=lambda item: (-item[1], item[0])
            )[:100]
        ],
        "format_match_rates": {
            pattern: sum(bool(re.fullmatch(pattern, value)) for value in strings) / len(strings)
            if strings
            else 0.0
            for pattern in patterns
        },
        "null_count": sum(value is None for value in raw),
        "punctuation_only_count": sum(
            bool(value)
            and not any(char.isalnum() for char in value)
            and not value.isspace()
            for value in strings
        ),
        "contains_punctuation_count": sum(
            any(not char.isalnum() and not char.isspace() for char in value)
            for value in strings
        ),
    }
