#!/usr/bin/env python3
#
# Fails when the SQL grammar's `CHECK DATABASE` clause list and its documented syntax block have
# drifted apart (issue #6406 item 2).
#
# Context: RECORD (#5680), DELETE ORPHANS (#6090) and RECLAIM UNREFERENCED FILES (#6189) each
# landed a new clause in `checkDatabaseStatement` (SQLParser.g4) without a matching edit to the
# `[[sql-check-database]]` syntax block in arcadedb-docs's sql-database-admin.adoc - three separate
# PRs missing the same line, which is a process gap rather than three oversights: nothing noticed,
# because nothing looked. The next clause will miss it too unless something automated looks every
# time.
#
# What this checks: every clause in the grammar rule's TOP-LEVEL `(...)?` groups - identified by
# the leading run of ALL-CAPS keyword tokens in each group, e.g. `TYPE`, `BUCKET`, `RECORD`, `FIX`,
# `DELETE ORPHANS`, `RECLAIM UNREFERENCED FILES`, `DEEP`, `COMPRESS` - must appear as a `[ ... ]`
# clause in the docs' `[source,sql]` syntax block. The reverse (a clause the docs mention that the
# grammar no longer has) is checked too: it is the same drift, just noticed from the other side,
# and just as easy to leave stale once a clause is removed.
#
# What this deliberately does NOT check: clause ORDER, argument syntax (`<type-name>[,]*` vs
# `<rid>[,]*`), or prose accuracy. Those need a human; this only proves neither side forgot a
# clause exists.
#
# This generalises to any statement whose grammar rule and documented syntax are both one block
# (the issue names this explicitly), but is scoped to CHECK DATABASE for now - that is the
# statement with the history of drift, and a generic version needs a registry of which grammar
# rule maps to which doc anchor, which is more machinery than one statement's guard justifies today.
#
# Usage:
#   check-database-docs-sync.py <path-to-SQLParser.g4> <path-to-sql-database-admin.adoc>
#
# @author Luca Garulli (l.garulli@arcadedata.com)
#
import re
import sys

KEYWORD = re.compile(r'^[A-Z][A-Z_]*$')


def strip_antlr_comments(text):
    """Removes ANTLR `//...` and `/* ... */` comments so they cannot be mistaken for grammar tokens."""
    text = re.sub(r'/\*.*?\*/', ' ', text, flags=re.DOTALL)
    text = re.sub(r'//[^\n]*', ' ', text)
    return text


def extract_rule_body(grammar_text, rule_name):
    """Returns the text between a named rule's `:` and its terminating top-level `;`."""
    start = re.search(r'(?:^|\n)' + re.escape(rule_name) + r'\s*\n?\s*:', grammar_text)
    if not start:
        raise ValueError(f"rule '{rule_name}' not found in grammar")
    pos = start.end()
    depth = 0
    for i in range(pos, len(grammar_text)):
        c = grammar_text[i]
        if c == '(':
            depth += 1
        elif c == ')':
            depth -= 1
        elif c == ';' and depth == 0:
            return grammar_text[pos:i]
    raise ValueError(f"rule '{rule_name}' has no terminating ';' (unbalanced parentheses?)")


def top_level_optional_groups(rule_body):
    """Yields the inner text of every top-level `( ... )?` group in a rule body, in order."""
    i = 0
    n = len(rule_body)
    while i < n:
        if rule_body[i] == '(':
            depth = 1
            j = i + 1
            while j < n and depth > 0:
                if rule_body[j] == '(':
                    depth += 1
                elif rule_body[j] == ')':
                    depth -= 1
                j += 1
            # j is now just past the matching ')'
            if j < n and rule_body[j] == '?':
                yield rule_body[i + 1:j - 1]
            i = j
        else:
            i += 1


def leading_keywords(text):
    """Return the leading run of ALL-CAPS tokens in `text`, joined with spaces."""
    # This is the clause's own keyword phrase, stopping at the first token that is not a bare
    # keyword (a rule reference, a literal, a nested group, ...).
    #
    # `(`/`)` and `[`/`]` are split off as their own tokens rather than left glued to a neighbour,
    # for both callers: a grammar clause can nest a `( ... )` alternation right after its keywords
    # (`(identifier | INTEGER_LITERAL)`), and a docs clause can nest a `[,]*`-style placeholder
    # repetition the same way (`<type-name>[,]*`). Either glued form would fail the bare-keyword
    # regex as one token and silently truncate the phrase one word early - splitting them off
    # first means a token is exactly one keyword or exactly one punctuation mark, never both.
    tokens = text
    for bracket in '()[]':
        tokens = tokens.replace(bracket, f' {bracket} ')
    words = []
    for tok in tokens.split():
        if KEYWORD.match(tok):
            words.append(tok)
        else:
            break
    return ' '.join(words)


def grammar_clauses(grammar_path, rule_name='checkDatabaseStatement'):
    with open(grammar_path, 'r', encoding='utf-8') as f:
        text = strip_antlr_comments(f.read())
    body = extract_rule_body(text, rule_name)
    clauses = set()
    for group in top_level_optional_groups(body):
        phrase = leading_keywords(group)
        if phrase:
            clauses.add(phrase)
    return clauses


def extract_syntax_block(doc_text, anchor):
    """The first `[source,sql] ---- ... ----` block after `[[anchor]]`."""
    anchor_pos = doc_text.find(f'[[{anchor}]]')
    if anchor_pos < 0:
        raise ValueError(f"anchor '[[{anchor}]]' not found in docs")
    block_start = doc_text.find('[source,sql]', anchor_pos)
    if block_start < 0:
        raise ValueError(f"no [source,sql] block after '[[{anchor}]]'")
    fence_start = doc_text.find('----', block_start)
    fence_end = doc_text.find('----', fence_start + 4)
    if fence_start < 0 or fence_end < 0:
        raise ValueError(f"[source,sql] block after '[[{anchor}]]' is not fenced with '----'")
    return doc_text[fence_start + 4:fence_end]


def top_level_bracket_groups(text):
    r"""Yield the inner text of every top-level `[ ... ]` group in `text`, in order."""
    # Depth-tracked rather than a `[^\]]*` regex: a clause's own placeholder can carry a NESTED
    # bracket - `[ TYPE <type-name>[,]* ]` has one inside the type-name repetition - and a
    # non-greedy `\[([^\]]+?)\]` stops at that INNER `]`, not the clause's real closing one. That
    # happened to still resolve to the right clause name here only because `leading_keywords`
    # truncates at the first non-keyword token anyway, so the truncated match and the full one
    # produce the same phrase by coincidence of today's doc wording - not by construction. A
    # syntax block phrased differently (uppercase text inside the nested brackets, or brackets
    # before the clause's own keyword) would have silently misparsed. Depth tracking has no such
    # coincidence to rely on.
    i = 0
    n = len(text)
    while i < n:
        if text[i] == '[':
            depth = 1
            j = i + 1
            while j < n and depth > 0:
                if text[j] == '[':
                    depth += 1
                elif text[j] == ']':
                    depth -= 1
                j += 1
            yield text[i + 1:j - 1]
            i = j
        else:
            i += 1


def docs_clauses(doc_path, anchor='sql-check-database'):
    with open(doc_path, 'r', encoding='utf-8') as f:
        block = extract_syntax_block(f.read(), anchor)
    clauses = set()
    for group in top_level_bracket_groups(block):
        phrase = leading_keywords(group.strip())
        if phrase:
            clauses.add(phrase)
    return clauses


def main():
    if len(sys.argv) != 3:
        print('Usage: check-database-docs-sync.py <path-to-SQLParser.g4> <path-to-sql-database-admin.adoc>',
              file=sys.stderr)
        return 2

    grammar_path, doc_path = sys.argv[1], sys.argv[2]

    try:
        grammar = grammar_clauses(grammar_path)
    except ValueError as e:
        print(f"ERROR: could not read CHECK DATABASE clauses from the grammar: {e}", file=sys.stderr)
        return 2

    try:
        docs = docs_clauses(doc_path)
    except ValueError as e:
        print(f"ERROR: could not read CHECK DATABASE clauses from the docs: {e}", file=sys.stderr)
        return 2

    missing_from_docs = sorted(grammar - docs)
    missing_from_grammar = sorted(docs - grammar)

    if not missing_from_docs and not missing_from_grammar:
        print(f"✅ CHECK DATABASE: grammar and docs agree on {len(grammar)} clause(s)")
        return 0

    if missing_from_docs:
        print("❌ The grammar's checkDatabaseStatement rule has clause(s) the docs' syntax block "
              "does not mention:")
        for clause in missing_from_docs:
            print(f"   - {clause}")
        print("   Update the [[sql-check-database]] syntax block in sql-database-admin.adoc "
              "(arcadedb-docs).")

    if missing_from_grammar:
        print("❌ The docs' syntax block mentions clause(s) the grammar's checkDatabaseStatement "
              "rule no longer has:")
        for clause in missing_from_grammar:
            print(f"   - {clause}")
        print("   Either the grammar dropped a clause the docs still advertise, or the docs "
              "phrase a clause the check does not recognise as one of the grammar's own keywords.")

    return 1


if __name__ == '__main__':
    sys.exit(main())
