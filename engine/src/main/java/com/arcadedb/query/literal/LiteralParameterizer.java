/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.query.literal;

import com.arcadedb.utility.SegmentedLRUCache;

import java.util.HashMap;
import java.util.Map;

/**
 * A statement cache that lets queries differing only in their literal values share one parsed statement and one plan (issue
 * #8307). Text-to-query generators, GraphRAG tools and query builders write their values into the text, so without this every
 * value is a new cache key, parsed and planned from scratch.
 * <p>
 * A lookup first tries the text as it is, so a parameterized or repeated statement costs exactly what it did before. On a miss
 * the text is lexed (with the language's own lexer, so a literal is exactly what the grammar calls one) and the literals the
 * language's POLICY allows are replaced with generated parameters: the resulting text is the cache key, and the replaced values
 * are returned to be bound like any parameter the caller supplied.
 * <p>
 * The policy is decided once per SHAPE (see {@link LiteralScan}) on the parse tree of the first text of that shape, by
 * {@link #parseAndClassify}: a literal is extracted only where a parameter means exactly what the literal meant, which each
 * language spells out position by position (a projection the column name is taken from, SKIP and LIMIT, a DDL statement, and so
 * on stay literal). The safe answer is always "keep it": a kept literal stays in the key, which then behaves exactly like the
 * text-keyed cache this replaces.
 *
 * @param <S> the parsed statement type
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class LiteralParameterizer<S> {
  /** Prefix of every generated parameter name. A text that already mentions it is never parameterized. */
  public static final String PARAMETER_PREFIX = "__lit_";

  /** Policy bit: the literal becomes a parameter. */
  public static final byte STRIP      = 1;
  /** Policy bit: the minus token in front of the literal is part of it, and moves into the parameter value. */
  public static final byte WITH_MINUS = 2;
  /** Policy bit with a meaning of the language's own, read by {@link #literalValue}. */
  public static final byte LANGUAGE   = 4;

  private static final byte[] KEEP_ALL     = new byte[0];
  /** Beyond this many literals a text is keyed as written: the shape records literal positions in one character each. */
  private static final int    MAX_LITERALS = 4096;

  private final SegmentedLRUCache<String, Lookup<S>> statements;
  private final SegmentedLRUCache<String, byte[]>    policies;

  /**
   * A statement found or parsed for a query text.
   *
   * @param statement  the parsed statement, shared by every text with the same key
   * @param cacheKey   the key the statement is cached under: the text itself, or the text with the extracted literals replaced
   *                   by parameters. A plan cache must use this key, not the text
   * @param parameters the extracted literal values by generated parameter name, or null when nothing was extracted
   */
  public record Lookup<S>(S statement, String cacheKey, Map<String, Object> parameters) {

    /**
     * Merges the extracted literal values into the caller's parameters. Returns {@code callerParameters} itself when nothing
     * was extracted, so the common path allocates nothing.
     */
    public Map<String, Object> mergeParameters(final Map<String, Object> callerParameters) {
      if (parameters == null)
        return callerParameters;
      if (callerParameters == null || callerParameters.isEmpty())
        return parameters;
      final Map<String, Object> merged = new HashMap<>(callerParameters.size() + parameters.size() + 2);
      merged.putAll(callerParameters);
      merged.putAll(parameters);
      return merged;
    }
  }

  protected LiteralParameterizer(final int size) {
    this.statements = new SegmentedLRUCache<>(size);
    this.policies = new SegmentedLRUCache<>(size);
  }

  /**
   * Returns the statement for a text exactly as written, parsing and caching it on a miss. No literal is extracted.
   */
  public Lookup<S> lookupAsWritten(final String text) {
    Lookup<S> cached;
    synchronized (statements) {
      cached = statements.get(text);
    }
    return cached != null ? cached : parseAsWritten(text);
  }

  /**
   * Returns the statement for a text, extracting its literals into parameters when {@code parameterize} is true and the
   * language's policy allows it.
   */
  public Lookup<S> lookup(final String text, final boolean parameterize) {
    Lookup<S> cached;
    synchronized (statements) {
      cached = statements.get(text);
    }
    if (cached != null)
      return cached;

    if (!parameterize || !LiteralScan.isEligible(text))
      return parseAsWritten(text);

    final LiteralScan scan = scan(text);
    if (scan == null || scan.size() == 0 || scan.size() > MAX_LITERALS)
      return parseAsWritten(text);

    final String shape = scan.getShape();
    byte[] policy;
    synchronized (policies) {
      policy = policies.get(shape);
    }

    if (policy == null) {
      // FIRST TEXT OF THIS SHAPE: PARSE IT AS WRITTEN (ITS ERRORS ARE THE ONES THE CALLER MUST SEE) AND DECIDE ON ITS TREE
      final Classified<S> classified = parseAndClassify(text, scan);
      policy = classified.policy();
      keepIdenticalLiteralsTogether(scan, policy);
      if (!stripsAny(policy))
        policy = KEEP_ALL;
      synchronized (policies) {
        policies.put(shape, policy);
      }
      if (policy == KEEP_ALL)
        return cache(text, classified.statement());
    } else if (policy == KEEP_ALL)
      return parseAsWritten(text);

    final Map<String, Object> values = new HashMap<>();
    final String key = buildKey(scan, policy, values);
    if (key == null)
      // A VALUE THE LANGUAGE CANNOT REPRESENT (AN OVERFLOW): THE TEXT AS WRITTEN RAISES THE PARSER'S OWN ERROR
      return parseAsWritten(text);

    synchronized (statements) {
      cached = statements.get(key);
    }
    if (cached == null) {
      final S template;
      try {
        template = parse(key);
      } catch (final RuntimeException e) {
        // THE GRAMMAR REFUSES A PARAMETER WHERE THE POLICY PUT ONE: NEVER EXTRACT FROM THIS SHAPE AGAIN
        synchronized (policies) {
          policies.put(shape, KEEP_ALL);
        }
        return parseAsWritten(text);
      }
      cached = cache(key, template);
    }
    return new Lookup<>(cached.statement(), key, values);
  }

  public boolean contains(final String text) {
    synchronized (statements) {
      return statements.containsKey(text);
    }
  }

  public int size() {
    synchronized (statements) {
      return statements.size();
    }
  }

  public void clear() {
    synchronized (statements) {
      statements.clear();
    }
    synchronized (policies) {
      policies.clear();
    }
  }

  /** The text with the stripped literals replaced by parameter references, filling {@code values}; null if a value fails. */
  private String buildKey(final LiteralScan scan, final byte[] policy, final Map<String, Object> values) {
    final String text = scan.getText();
    final StringBuilder key = new StringBuilder(text.length() + 16);
    // names given so far, by literal index, so a literal written twice is one parameter: the two occurrences then stay the
    // same expression, which the projection-matching rules of both languages compare by text
    final String[] names = new String[scan.size()];
    int last = 0;
    int ordinal = 0;
    for (int i = 0; i < scan.size(); i++) {
      final byte action = policy[i];
      if ((action & STRIP) == 0)
        continue;

      final boolean withMinus = (action & WITH_MINUS) != 0;
      final int start = withMinus ? scan.getMinusStart(i) : scan.getStart(i);
      final int end = scan.getEnd(i);

      // an identical literal met before, extracted the same way, already has the name
      String name = null;
      final int group = scan.getDuplicateOf(i) < 0 ? i : scan.getDuplicateOf(i);
      for (int j = group; j < i && name == null; j++)
        if (names[j] != null && policy[j] == action && (j == group || scan.getDuplicateOf(j) == group))
          name = names[j];

      if (name == null) {
        final Object value;
        try {
          value = literalValue(text, scan.getStart(i), end, withMinus, (action & LANGUAGE) != 0);
        } catch (final RuntimeException e) {
          return null;
        }
        name = PARAMETER_PREFIX + kind(value) + ordinal++;
        values.put(name, value);
      }
      names[i] = name;

      key.append(text, last, start);
      appendParameter(key, name);
      last = end;
    }
    return key.append(text, last, text.length()).toString();
  }

  private Lookup<S> parseAsWritten(final String text) {
    return cache(text, parse(text));
  }

  private Lookup<S> cache(final String key, final S statement) {
    final Lookup<S> lookup = new Lookup<>(statement, key, null);
    synchronized (statements) {
      statements.put(key, lookup);
    }
    return lookup;
  }

  /**
   * Keeps every literal written identically to one that must stay: both languages compare expressions by their text, so an
   * extracted literal and its kept twin would make two equal expressions different.
   */
  private static void keepIdenticalLiteralsTogether(final LiteralScan scan, final byte[] policy) {
    final boolean[] keptGroup = new boolean[scan.size()];
    for (int i = 0; i < scan.size(); i++)
      if ((policy[i] & STRIP) == 0)
        keptGroup[scan.getDuplicateOf(i) < 0 ? i : scan.getDuplicateOf(i)] = true;
    for (int i = 0; i < scan.size(); i++)
      if (keptGroup[scan.getDuplicateOf(i) < 0 ? i : scan.getDuplicateOf(i)])
        policy[i] = 0;
  }

  private static boolean stripsAny(final byte[] policy) {
    for (final byte action : policy)
      if ((action & STRIP) != 0)
        return true;
    return false;
  }

  /**
   * A statement parsed from a text as written, and the policy decided on its parse tree.
   *
   * @param statement the parsed statement
   * @param policy    one action per literal of the scan: 0 to keep it, or {@link #STRIP} with {@link #WITH_MINUS} and
   *                  {@link #LANGUAGE} as needed
   */
  public record Classified<S>(S statement, byte[] policy) {
  }

  /** Lexes the text and records its literal tokens, or returns null when the text must not be parameterized. */
  protected abstract LiteralScan scan(String text);

  /** Parses a text, raising the language's own parsing errors. */
  protected abstract S parse(String text);

  /** Parses the text as written and decides, literal by literal, which ones become parameters. */
  protected abstract Classified<S> parseAndClassify(String text, LiteralScan scan);

  /**
   * The value the parser gives the literal written at {@code [start, end)} of {@code text}, negated when {@code withMinus}.
   * Must be exactly the value, and the Java type, the parser would put in the statement for it.
   *
   * @throws RuntimeException when the literal has no value (an overflow): the text is then parsed as written instead
   */
  protected abstract Object literalValue(String text, int start, int end, boolean withMinus, boolean languageFlag);

  /** A character naming the class of a value, embedded in the parameter name so values of different classes get different keys. */
  protected abstract char kind(Object value);

  /** Appends a reference to the generated parameter {@code name} in the language's syntax. */
  protected abstract void appendParameter(StringBuilder key, String name);
}
