"""Pure condition evaluation and header building.

`evaluate_condition` and `build_headers` are the only functions the
runtime consumer needs to call per message per mapping per destination.
They are intentionally side-effect-free so they can be unit-tested
without Kafka, without asyncio, and without a database.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

import jmespath


# ---------- expression preprocessing ----------

def _preprocess(expression: str) -> str:
    """Translate our `@` root prefix into JMESPath's implicit-root form.

    JMESPath itself uses `@` for the current node. If the expression
    starts with `@.`, we drop the `@.` to make it a normal implicit root expression.
    If it's just `@`, it stays `@`.

        @.Message.TableName  ->  Message.TableName
        @                    ->  @
        @.items[0].id        ->  items[0].id

    A bare `@` is mapped to `@` (the current/root node identifier).
    """
    if not expression:
        return expression
    stripped = expression.lstrip()
    if not stripped.startswith("@"):
        return expression
    # Preserve leading whitespace if any (we accept `   @foo` too).
    leading_ws = expression[: len(expression) - len(stripped)]
    body = stripped[1:]
    if body.startswith("."):
        body = body[1:]
    if body == "":
        return leading_ws + "@"
    return leading_ws + body


# ---------- condition evaluation ----------


@dataclass
class MatchResult:
    """The outcome of evaluating a single condition against a message.

    `matched` is the boolean the runtime uses to decide whether to route.
    `resolved` is the raw value jmespath returned (None if not found or
    on error). `error` is set when the expression was invalid, the key
    was missing, or the result was a non-scalar (per PRD §12 8.C).
    """

    matched: bool
    resolved: Any = None
    error: Optional[str] = None
    # Set to True when the JMESPath expression itself was invalid (parse
    # error). Distinct from a missing key or non-scalar result.
    expression_invalid: bool = field(default=False)


def evaluate_condition(
    *,
    key_path: str,
    operator: str,
    value: str,
    case_insensitive: bool,
    message: dict,
) -> MatchResult:
    """Evaluate one mapping's condition against one parsed JSON message.

    Strict typing (PRD §12 8.C): non-scalar results (list, dict) and
    missing keys do NOT match. They return `matched=False` with a
    descriptive `error`. The runtime and UI both use `error` to show
    "non-scalar result" or "key not found" rather than treating them as
    silent no-matches.
    """
    # 1. Evaluate the JMESPath expression.
    try:
        resolved = jmespath.search(_preprocess(key_path), message)
    except (jmespath.exceptions.ParseError, jmespath.exceptions.LexerError) as exc:
        return MatchResult(
            matched=False,
            resolved=None,
            error=f"invalid JMESPath: {exc}",
            expression_invalid=True,
        )
    except Exception as exc:  # noqa: BLE001 — jmespath can raise a few types
        return MatchResult(
            matched=False,
            resolved=None,
            error=f"JMESPath error: {exc}",
            expression_invalid=True,
        )

    # 2. Strict typing: missing or non-scalar -> no match, with reason.
    if resolved is None:
        return MatchResult(matched=False, resolved=None, error="key not found")
    if isinstance(resolved, (list, dict)):
        return MatchResult(
            matched=False,
            resolved=resolved,
            error="non-scalar result",
        )

    # 3. Coerce scalars to string for comparison.
    if not isinstance(resolved, str):
        resolved_str = str(resolved)
    else:
        resolved_str = resolved

    target = value
    if case_insensitive:
        resolved_str = resolved_str.casefold()
        target = target.casefold()

    # 4. Apply the operator.
    if operator == "equals":
        matched = resolved_str == target
    elif operator == "not_equals":
        matched = resolved_str != target
    elif operator == "contains":
        matched = (target in resolved_str) or (resolved_str in target)
    else:
        # Defensive: a malformed mapping should not match.
        return MatchResult(
            matched=False,
            resolved=resolved,
            error=f"unknown operator: {operator}",
            expression_invalid=True,
        )

    return MatchResult(
        matched=matched,
        resolved=resolved,
        error=None if matched else "resolved value did not match",
    )


def evaluate_match_condition(
    *,
    key_path: str,
    operator: str,
    values: Sequence[str],
    case_insensitive: bool,
    message: dict,
) -> MatchResult:
    """Evaluate a single match condition (1 key_path, N values, OR-list) against a message.

    Semantics:

      1. Validate the JMESPath expression once (fast path; propagates parse errors).
      2. If `resolved` is None → "key not found".
      3. If `resolved` is a list/dict → "non-scalar result" (strict typing).
      4. For each value in `values` (in order), call `evaluate_condition` with that
         single value. Return on the first match.
      5. If none match, return `matched=False, error="resolved value did not match"`.
      6. If `values` is empty, return `matched=False, error="no values to match against"`.
    """
    if not values:
        return MatchResult(
            matched=False, resolved=None, error="no values to match against"
        )
    try:
        resolved = jmespath.search(_preprocess(key_path), message)
    except (jmespath.exceptions.ParseError, jmespath.exceptions.LexerError) as exc:
        return MatchResult(
            matched=False,
            resolved=None,
            error=f"invalid JMESPath: {exc}",
            expression_invalid=True,
        )
    except Exception as exc:  # noqa: BLE001
        return MatchResult(
            matched=False,
            resolved=None,
            error=f"JMESPath error: {exc}",
            expression_invalid=True,
        )
    if resolved is None:
        return MatchResult(matched=False, resolved=None, error="key not found")
    if isinstance(resolved, (list, dict)):
        return MatchResult(matched=False, resolved=resolved, error="non-scalar result")
    # Strict-typing passed — `resolved` is a scalar. Try each value in order.
    for v in values:
        sub = evaluate_condition(
            key_path=key_path,
            operator=operator,
            value=v,
            case_insensitive=case_insensitive,
            message=message,
        )
        if sub.matched:
            return sub
        if sub.expression_invalid:
            return sub
    return MatchResult(
        matched=False, resolved=resolved, error="resolved value did not match"
    )


# ---------- header building ----------


def build_headers(
    headers: Sequence[Dict[str, Any]],
    message: dict,
) -> List[Tuple[str, bytes]]:
    """Resolve a list of header specs against a parsed message.

    `headers` is a sequence of dicts with `name`, `value`, `mode`.
    `mode == "static"` -> value is taken literally.
    `mode == "from_message"` -> value is a JMESPath expression evaluated
    against `message`; non-string results are coerced with `str(...)`
    (per PRD §12 8.D — header coercion stays as written).

    Returns a list of `(name, value_bytes)` tuples ready for
    `AIOKafkaProducer.send(headers=...)`.
    """
    out: List[Tuple[str, bytes]] = []
    for h in headers:
        name = h["name"]
        mode = h.get("mode", "static")
        spec = h["value"]
        if mode == "static":
            value_str = spec
        else:  # from_message
            try:
                resolved = jmespath.search(_preprocess(spec), message)
            except Exception:  # noqa: BLE001 — coerce even on bad expression
                resolved = None
            if resolved is None:
                value_str = ""
            elif isinstance(resolved, str):
                value_str = resolved
            else:
                value_str = str(resolved)
        out.append((name, value_str.encode("utf-8")))
    return out
