#!/usr/bin/env python3
"""
Validate metadata.json files written by kafka-spy for each topic directory.

Usage:
    python3 scripts/validate-metadata.py --inferred-schemas-dir <dir>

Exit codes:
    0  all expected topics PASS
    1  one or more expected topics FAIL
"""

import argparse
import json
import sys
from pathlib import Path

# Hard-coded expected values per topic.
# order/payment/user-events: run with explicit --discriminator eventType → probe skipped →
# KafkaSpy passes config.discriminatorField to SpyMetadataWriter → EXPLICIT_SINGLE + discriminatorField.
# inferred-events: no --discriminator → inference runs → ExplicitSingle on 'action' field.
# untyped-events: no --discriminator → inference runs → NoVariants → SINGLE_TYPE.
# shape-events: no --discriminator → inference runs → ImplicitShape.
EXPECTED = {
    # Original 6-topic regression
    "order-events":   {"discriminatorResultType": "EXPLICIT_SINGLE", "discriminatorField": "eventType"},
    "payment-events": {"discriminatorResultType": "EXPLICIT_SINGLE", "discriminatorField": "eventType"},
    "user-events":    {"discriminatorResultType": "EXPLICIT_SINGLE", "discriminatorField": "eventType"},
    "inferred-events": {
        "discriminatorResultType": "EXPLICIT_SINGLE",
        "discriminatorField": "action",
    },
    "untyped-events": {"discriminatorResultType": "SINGLE_TYPE"},
    "shape-events":   {"discriminatorResultType": "IMPLICIT_SHAPE", "minClusterSignatures": 2},
    # Phase 1 confirm — edge case topics (in inferred-schemas-edge/)
    # EC1: eventType wins over source via +10 semantic bias
    "competing-candidates-events": {
        "discriminatorResultType": "EXPLICIT_SINGLE", "discriminatorField": "eventType",
    },
    # EC2: eventType wins over status via +20 semantic gap (+10 vs -10)
    "false-positive-events": {
        "discriminatorResultType": "EXPLICIT_SINGLE", "discriminatorField": "eventType",
    },
    # EC3-strict: eventType fails 0.8 presence gate; cluster-2 has no unique signature → None
    "partial-field-events": {"discriminatorResultType": "SINGLE_TYPE"},
    # EC7: no enum-like field; all same shape → NoVariants → SINGLE_TYPE
    "generic-events": {"discriminatorResultType": "SINGLE_TYPE"},
    # Phase 2 confirm — EC5 small-sample (sample_size >= 2 per type, i.e. total >= 4)
    # At total=2 (sample_size=1): SINGLE_TYPE (both all-unique); total>=4: EXPLICIT_SINGLE eventType
    # The EXPECTED entry covers the large-N stable result
    "small-sample-events": {
        "discriminatorResultType": "EXPLICIT_SINGLE", "discriminatorField": "eventType",
    },
    # Negotiate-class: partial-field-literal-events, overlapping-values-events
    # Intentionally omitted from EXPECTED — validator WARN-skips unknown topics.
    # Add EXPECTED only after D1 is resolved.
}

VALID_TYPES = {"EXPLICIT_SINGLE", "EXPLICIT_COMPOSITE", "IMPLICIT_SHAPE", "SINGLE_TYPE"}


def validate_topic(topic_dir: Path) -> list[str]:
    """Returns list of error strings (empty = PASS)."""
    errors = []
    meta_file = topic_dir / "metadata.json"

    # kafka-spy writes to <output-root>/<topic-name>/, so if we pass
    # inferred-schemas/order-events/ as the output root the actual output
    # lands in inferred-schemas/order-events/order-events/.  Accept both.
    if not meta_file.exists():
        nested = topic_dir / topic_dir.name / "metadata.json"
        if nested.exists():
            meta_file = nested
        else:
            return [f"metadata.json not found in {topic_dir}"]

    try:
        meta = json.loads(meta_file.read_text())
    except json.JSONDecodeError as e:
        return [f"metadata.json is not valid JSON: {e}"]

    result_type = meta.get("discriminatorResultType")
    if result_type not in VALID_TYPES:
        errors.append(
            f"discriminatorResultType is '{result_type}', "
            f"expected one of {sorted(VALID_TYPES)}"
        )
        return errors

    # Structural validation per type
    if result_type == "EXPLICIT_SINGLE":
        field = meta.get("discriminatorField")
        if not field:
            errors.append("EXPLICIT_SINGLE requires non-empty 'discriminatorField'")
    elif result_type == "EXPLICIT_COMPOSITE":
        fields = meta.get("discriminatorFields")
        if not isinstance(fields, list) or len(fields) != 2 or not all(fields):
            errors.append("EXPLICIT_COMPOSITE requires 'discriminatorFields': [<str>, <str>]")
    elif result_type == "IMPLICIT_SHAPE":
        sigs = meta.get("clusterSignatures")
        if not isinstance(sigs, list) or len(sigs) < 2:
            errors.append("IMPLICIT_SHAPE requires 'clusterSignatures' with >= 2 entries")
        else:
            for i, sig in enumerate(sigs):
                if not sig.get("schemaFile"):
                    errors.append(f"clusterSignatures[{i}] missing 'schemaFile'")
                paths = sig.get("signaturePaths")
                if not isinstance(paths, list) or len(paths) == 0:
                    errors.append(f"clusterSignatures[{i}] missing non-empty 'signaturePaths'")
    # SINGLE_TYPE: no extra keys required

    # Hard-coded expected values check
    topic = topic_dir.name
    if topic not in EXPECTED:
        return errors  # Unknown topic — structural validation only

    expected = EXPECTED[topic]
    expected_type = expected["discriminatorResultType"]
    if result_type != expected_type:
        errors.append(
            f"Expected discriminatorResultType '{expected_type}' for '{topic}', got '{result_type}'"
        )
    elif expected_type == "EXPLICIT_SINGLE":
        expected_field = expected.get("discriminatorField")
        actual_field = meta.get("discriminatorField")
        if expected_field and actual_field != expected_field:
            errors.append(
                f"Expected discriminatorField '{expected_field}' for '{topic}', "
                f"got '{actual_field}'"
            )
    elif expected_type == "IMPLICIT_SHAPE":
        min_sigs = expected.get("minClusterSignatures", 2)
        sigs = meta.get("clusterSignatures", [])
        if len(sigs) < min_sigs:
            errors.append(
                f"Expected >= {min_sigs} clusterSignatures for '{topic}', got {len(sigs)}"
            )

    return errors


def main():
    parser = argparse.ArgumentParser(
        description="Validate metadata.json files written by kafka-spy"
    )
    parser.add_argument("--inferred-schemas-dir", required=True, type=Path)
    args = parser.parse_args()

    topic_dirs = sorted(d for d in args.inferred_schemas_dir.iterdir() if d.is_dir())
    any_fail = False

    for topic_dir in topic_dirs:
        topic = topic_dir.name
        if topic not in EXPECTED:
            print(f"  WARN  {topic}: unexpected topic directory, skipping")
            continue
        errors = validate_topic(topic_dir)
        if errors:
            print(f"  FAIL  {topic}:")
            for e in errors:
                print(f"          - {e}")
            any_fail = True
        else:
            print(f"  PASS  {topic}")

    # Check all expected topics were present
    found = {d.name for d in topic_dirs}
    for topic in EXPECTED:
        if topic not in found:
            print(f"  FAIL  {topic}: directory not found in {args.inferred_schemas_dir}")
            any_fail = True

    if any_fail:
        print("\nMetadata validation failed.")
        sys.exit(1)
    else:
        print("\nAll metadata files validated successfully.")


if __name__ == "__main__":
    main()
