#!/usr/bin/env python3
#
# Drops Meterian's "[stability] <pkg> is outdated" findings from a SARIF report before it is
# uploaded to GitHub code scanning.
#
# Meterian reports three dimensions - security, stability and licensing - and puts all of them in
# one SARIF. Stability findings are not vulnerabilities: they say only that a newer release of a
# dependency exists, which is what Dependabot already opens pull requests for. On the 2026-08-13
# scan of main they were 46 of the 65 alerts, so 71% of a tab whose whole purpose is triage was
# taken up by "this dependency is a version behind", and the security findings had to be picked
# out from among them.
#
# Meterian has no flag for this. --report-console takes a security|stability|licensing filter but
# --report-sarif takes a filename only, and the .meterian exclusions file is documented as acting
# on the scores rather than on report contents - which would be a no-op here, since the workflow
# already runs the scan with continue-on-error and gates nothing on the score. Filtering the
# report between the scan and the upload is the part that is ours to control.
#
# The findings are not lost: they stay in the Meterian report linked from every alert, and the
# dependencies themselves stay under Dependabot. What changes is that the GitHub security tab
# holds security findings only.
#
# A rule is dropped when it is tagged "stability" and not also tagged "security". Anything whose
# tags cannot be read, or whose rule is missing from the report altogether, is kept: this script
# must never be the reason a vulnerability stops being reported.
#
# Usage:
#   filter-meterian-sarif.py <report.sarif>     (rewritten in place)
import json
import sys
from pathlib import Path


def is_stability_only(rule: dict) -> bool:
    """True when a rule reports outdatedness and nothing else."""
    # `or {}` rather than a get() default: a rule carrying an explicit "properties": null is valid
    # JSON, and get("properties", {}) returns the null rather than the default for it. Reading tags
    # off that raises, which - given the step is continue-on-error - would silently pass the whole
    # unfiltered report through, the one outcome the "keep anything unreadable" rule is meant to
    # avoid stating and then not delivering.
    properties = rule.get("properties") or {}
    tags = properties.get("tags") if isinstance(properties, dict) else None
    if not isinstance(tags, list):
        return False
    return "stability" in tags and "security" not in tags


def filter_run(run: dict) -> tuple[int, int, int, int]:
    """Filters one SARIF run in place. Returns (dropped rules, dropped results, kept rules, kept results)."""
    rules = run.get("tool", {}).get("driver", {}).get("rules")
    if not isinstance(rules, list):
        return 0, 0, 0, len(run.get("results", []) or [])

    dropped_ids = {r.get("id") for r in rules if isinstance(r, dict) and is_stability_only(r)}
    if not dropped_ids:
        return 0, 0, len(rules), len(run.get("results", []) or [])

    kept_rules = [r for r in rules if not (isinstance(r, dict) and is_stability_only(r))]

    # Results reference their rule by array position as well as by id. Rebuilding the array
    # invalidates every one of those positions, so they are remapped from the id - a result whose
    # id is not in the report at all keeps whatever it had, since it is not ours to renumber.
    #
    # Rule ids are assumed unique, which they are in Meterian's output (64 rules, 64 distinct ids
    # in the committed fixture) and which SARIF requires. Were two kept rules ever to share one,
    # the later would win here and results meaning the earlier would be remapped onto it - but at
    # that point the id no longer identifies a rule and the report is already ambiguous.
    index_by_id = {r.get("id"): i for i, r in enumerate(kept_rules) if isinstance(r, dict)}

    results = run.get("results", []) or []
    kept_results = []
    for result in results:
        if not isinstance(result, dict):
            kept_results.append(result)
            continue
        if result.get("ruleId") in dropped_ids:
            continue
        new_index = index_by_id.get(result.get("ruleId"))
        if new_index is not None:
            # SARIF spells the position two ways and a report may use either: nested under
            # result.rule (what GitHub emits when it normalises a report) or as a top-level
            # result.ruleIndex. Both are remapped, or the shape this script does not happen to
            # look at is exactly the one that keeps a stale index.
            rule_ref = result.get("rule")
            if isinstance(rule_ref, dict) and "index" in rule_ref:
                rule_ref["index"] = new_index
            if "ruleIndex" in result:
                result["ruleIndex"] = new_index
        kept_results.append(result)

    run["tool"]["driver"]["rules"] = kept_rules
    if "results" in run:
        run["results"] = kept_results

    return len(rules) - len(kept_rules), len(results) - len(kept_results), len(kept_rules), len(kept_results)


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: filter-meterian-sarif.py <report.sarif>", file=sys.stderr)
        return 2

    report = Path(sys.argv[1])

    # A scan that produced no report is not this script's problem to report on: the workflow
    # already treats the upload as best-effort, and failing here would turn that step red for a
    # reason that has nothing to do with filtering.
    if not report.is_file():
        print(f"filter-meterian-sarif: {report} does not exist, nothing to filter")
        return 0

    try:
        doc = json.loads(report.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        print(f"filter-meterian-sarif: {report} is not valid JSON: {error}", file=sys.stderr)
        return 2

    # Well-formed JSON that is not an object at all - a bare array or string - would otherwise
    # reach doc.get() and raise, which is safe but reports nothing useful. Say the same kind of
    # thing the decode error above says.
    if not isinstance(doc, dict):
        print(f"filter-meterian-sarif: {report} is not valid JSON: expected an object at the top level", file=sys.stderr)
        return 2

    runs = doc.get("runs")
    if not isinstance(runs, list):
        print(f"filter-meterian-sarif: {report} has no runs, nothing to filter")
        return 0

    dropped_rules = dropped_results = kept_rules = kept_results = 0
    for run in runs:
        if not isinstance(run, dict):
            continue
        d_rules, d_results, k_rules, k_results = filter_run(run)
        dropped_rules += d_rules
        dropped_results += d_results
        kept_rules += k_rules
        kept_results += k_results

    report.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(
        f"filter-meterian-sarif: removed {dropped_results} stability "
        f"{'finding' if dropped_results == 1 else 'findings'} ({dropped_rules} "
        f"{'rule' if dropped_rules == 1 else 'rules'}); kept {kept_results} security "
        f"{'finding' if kept_results == 1 else 'findings'} ({kept_rules} "
        f"{'rule' if kept_rules == 1 else 'rules'})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
