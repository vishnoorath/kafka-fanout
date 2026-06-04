"""Tests for the matcher module.

Pure unit tests — no asyncio, no Kafka, no DB. Run with:

    cd backend && pytest tests/test_matcher.py -v
"""
from __future__ import annotations

import pytest

from app.runtime.matcher import (
    MatchResult,
    build_headers,
    evaluate_condition,
    evaluate_match_condition,
    _preprocess,
)


# ---------- helpers ----------


def _msg(**kwargs) -> dict:
    return kwargs


def _eval(msg, **cond):
    """Shorthand — every test passes every condition field."""
    defaults = {
        "key_path": "Message.TableName",
        "operator": "equals",
        "value": "Cattles",
        "case_insensitive": True,
    }
    defaults.update(cond)
    return evaluate_condition(message=msg, **defaults)


# ---------- equals ----------


def test_equals_case_insensitive_match():
    r = _eval(_msg(Message={"TableName": "cattles"}))
    assert r.matched is True
    assert r.error is None
    assert r.resolved == "cattles"


def test_equals_case_sensitive_no_match():
    r = _eval(
        _msg(Message={"TableName": "CATTLES"}),
        case_insensitive=False,
    )
    assert r.matched is False
    assert "did not match" in r.error


def test_equals_case_sensitive_exact_match():
    r = _eval(
        _msg(Message={"TableName": "Cattles"}),
        case_insensitive=False,
    )
    assert r.matched is True


# ---------- not_equals ----------


def test_not_equals_mismatch():
    r = _eval(
        _msg(Message={"TableName": "Orders"}),
        operator="not_equals",
    )
    assert r.matched is True


def test_not_equals_match_returns_false():
    r = _eval(
        _msg(Message={"TableName": "Cattles"}),
        operator="not_equals",
    )
    assert r.matched is False


# ---------- contains ----------


def test_contains_substring():
    r = _eval(
        _msg(Message={"TableName": "CattleLog"}),
        operator="contains",
        value="cattle",
    )
    assert r.matched is True


def test_contains_case_sensitive_no_match():
    """Lowercase needle vs mixed-case haystack: case_sensitive=True means no match."""
    r = _eval(
        _msg(Message={"TableName": "CattleLog"}),
        operator="contains",
        value="cattle",  # lowercase
        case_insensitive=False,
    )
    assert r.matched is False


# ---------- scalar coercion ----------


def test_scalar_int_coerced_to_string():
    r = _eval(
        _msg(Message={"TableName": 42}),
        value="42",
    )
    assert r.matched is True
    assert r.resolved == 42


def test_scalar_bool_coerced_to_string():
    r = _eval(
        _msg(Message={"TableName": True}),
        value="true",
    )
    assert r.matched is True


# ---------- missing key ----------


def test_missing_key_does_not_match():
    r = _eval(_msg(Message={"Other": "x"}))
    assert r.matched is False
    assert r.error == "key not found"
    assert r.resolved is None


def test_explicit_none_does_not_match():
    r = _eval(_msg(Message={"TableName": None}))
    assert r.matched is False
    assert r.error == "key not found"


# ---------- non-scalar result (strict typing, 8.C) ----------


def test_list_result_does_not_match_with_non_scalar_error():
    r = _eval(_msg(Message={"TableName": ["a", "b"]}))
    assert r.matched is False
    assert r.error == "non-scalar result"
    assert r.resolved == ["a", "b"]


def test_object_result_does_not_match_with_non_scalar_error():
    r = _eval(_msg(Message={"TableName": {"nested": "x"}}))
    assert r.matched is False
    assert r.error == "non-scalar result"


def test_nested_list_path_treated_as_non_scalar():
    """JMESPath that returns a multi-element list is non-scalar."""
    r = _eval(
        _msg(items=[{"id": 1}, {"id": 2}]),
        key_path="items[].id",
        operator="equals",
        value="1",
    )
    # jmespath flattens this to [1, 2]; per strict typing, lists are non-scalar.
    assert r.matched is False
    assert r.error == "non-scalar result"


def test_single_element_list_still_non_scalar():
    r = _eval(
        _msg(items=[{"id": 1}]),
        key_path="items[].id",
        operator="equals",
        value="1",
    )
    assert r.matched is False
    assert r.error == "non-scalar result"


# ---------- invalid expression / operator ----------


def test_invalid_jmespath_syntax():
    r = _eval(_msg(Message={"a": 1}), key_path="Message.[bad")
    assert r.matched is False
    assert r.expression_invalid is True
    assert "invalid JMESPath" in r.error


def test_unknown_operator_does_not_match():
    r = _eval(
        _msg(Message={"TableName": "Cattles"}),
        operator="regex",
    )
    assert r.matched is False
    assert r.expression_invalid is True


# ---------- headers ----------


def test_static_header_literal():
    out = build_headers(
        [{"name": "X-Domain", "value": "Connector", "mode": "static"}],
        _msg(Message={"TableName": "Cattles"}),
    )
    assert out == [("X-Domain", b"Connector")]


def test_from_message_header_pulls_value():
    out = build_headers(
        [{"name": "X-TableName", "value": "Message.TableName", "mode": "from_message"}],
        _msg(Message={"TableName": "Cattles"}),
    )
    assert out == [("X-TableName", b"Cattles")]


def test_from_message_header_coerces_int():
    out = build_headers(
        [{"name": "X-N", "value": "count", "mode": "from_message"}],
        _msg(count=7),
    )
    assert out == [("X-N", b"7")]


def test_from_message_header_coerces_bool():
    out = build_headers(
        [{"name": "X-F", "value": "flag", "mode": "from_message"}],
        _msg(flag=True),
    )
    assert out == [("X-F", b"True")]


def test_from_message_header_missing_key_yields_empty():
    out = build_headers(
        [{"name": "X-X", "value": "no.such.key", "mode": "from_message"}],
        _msg(a=1),
    )
    assert out == [("X-X", b"")]


def test_from_message_header_bad_expression_yields_empty():
    out = build_headers(
        [{"name": "X-X", "value": "Message.[bad", "mode": "from_message"}],
        _msg(a=1),
    )
    assert out == [("X-X", b"")]


def test_headers_preserve_order():
    out = build_headers(
        [
            {"name": "A", "value": "1", "mode": "static"},
            {"name": "B", "value": "a", "mode": "from_message"},
            {"name": "C", "value": "3", "mode": "static"},
        ],
        _msg(a=2),
    )
    assert out == [
        ("A", b"1"),
        ("B", b"2"),
        ("C", b"3"),
    ]


def test_default_mode_is_static():
    out = build_headers(
        [{"name": "X", "value": "literal"}],  # no mode key
        _msg(a=1),
    )
    assert out == [("X", b"literal")]


# ---------- # root prefix ----------


def test_preprocess_strips_leading_hash():
    assert _preprocess("#Message.TableName") == "Message.TableName"
    assert _preprocess("#items[0].id") == "items[0].id"
    assert _preprocess("   #foo") == "   foo"  # preserves leading whitespace


def test_preprocess_bare_hash_becomes_root():
    assert _preprocess("#") == "@"


def test_preprocess_passthrough_when_no_hash():
    assert _preprocess("Message.TableName") == "Message.TableName"
    assert _preprocess("") == ""


def test_evaluate_condition_with_hash_prefix():
    """`#Message.TableName` should resolve to the same value as `Message.TableName`."""
    r_hash = _eval(_msg(Message={"TableName": "cattles"}), key_path="#Message.TableName")
    r_plain = _eval(_msg(Message={"TableName": "cattles"}), key_path="Message.TableName")
    assert r_hash.matched is True
    assert r_hash.resolved == r_plain.resolved == "cattles"


def test_evaluate_condition_with_hash_in_value():
    """`#` should also work in from_message header values."""
    out = build_headers(
        [{"name": "X", "value": "#Message.TableName", "mode": "from_message"}],
        _msg(Message={"TableName": "Cattles"}),
    )
    assert out == [("X", b"Cattles")]


def test_hash_only_resolves_to_root():
    """A bare `#` means "the whole document" and should match if the
    value being compared against is the JSON string of the doc.
    Mostly a sanity check that the substitution to `@` works."""
    r = _eval(
        _msg(Message={"TableName": "Cattles"}),
        key_path="#",
        value="Cattles",  # would never match a list/object — but we use a string
    )
    # `#` -> `@` resolves to the whole dict, which is not a scalar,
    # so the strict-typing rule returns non-scalar result.
    assert r.matched is False
    assert r.error == "non-scalar result"


# ---------- evaluate_match_condition (OR-list semantics) ----------


def test_mc_or_list_first_match_wins():
    r = evaluate_match_condition(
        key_path="Message.TableName",
        operator="equals",
        values=["Cattles", "Users", "Farms"],
        case_insensitive=True,
        message={"Message": {"TableName": "Users"}},
    )
    assert r.matched is True
    assert r.resolved == "Users"
    assert r.error is None


def test_mc_or_list_no_match():
    r = evaluate_match_condition(
        key_path="Message.TableName",
        operator="equals",
        values=["Cattles", "Users"],
        case_insensitive=True,
        message={"Message": {"TableName": "Farms"}},
    )
    assert r.matched is False
    assert r.error == "resolved value did not match"


def test_mc_or_list_one_invalid_value_is_skipped():
    """An empty string in the values list shouldn't poison the match.
    The non-empty value should still win."""
    r = evaluate_match_condition(
        key_path="Message.TableName",
        operator="equals",
        values=["", "Cattles"],
        case_insensitive=True,
        message={"Message": {"TableName": "Cattles"}},
    )
    assert r.matched is True
    assert r.resolved == "Cattles"


def test_mc_non_scalar_result_does_not_match():
    r = evaluate_match_condition(
        key_path="Message.TableName",
        operator="equals",
        values=["a", "b"],
        case_insensitive=True,
        message={"Message": {"TableName": ["a", "b"]}},
    )
    assert r.matched is False
    assert r.error == "non-scalar result"


def test_mc_empty_values_list_never_matches():
    r = evaluate_match_condition(
        key_path="Message.TableName",
        operator="equals",
        values=[],
        case_insensitive=True,
        message={"Message": {"TableName": "Cattles"}},
    )
    assert r.matched is False
    assert "no values" in (r.error or "")


def test_mc_key_not_found_does_not_match():
    r = evaluate_match_condition(
        key_path="Message.TableName",
        operator="equals",
        values=["Cattles"],
        case_insensitive=True,
        message={"Message": {"Other": "x"}},
    )
    assert r.matched is False
    assert r.error == "key not found"


def test_mc_invalid_jmespath_marks_expression_invalid():
    r = evaluate_match_condition(
        key_path="Message.[bad",
        operator="equals",
        values=["x"],
        case_insensitive=True,
        message={"Message": {"a": 1}},
    )
    assert r.matched is False
    assert r.expression_invalid is True
    assert "invalid JMESPath" in (r.error or "")
