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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.opencypher.temporal.CypherTemporalValue;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Throw-away hash index over one property of a full type scan, built when a chained MATCH with an inline equality
 * on a row-dependent value (e.g. {@code OPTIONAL MATCH (c:T {name: row.x})}) has no real index to use (issue #8695).
 * Without it every outer row re-scans the whole type, so the step costs rows x records.
 * <p>
 * It only narrows the candidates: the caller still runs the complete pattern/WHERE filter on each of them, so the
 * answer cannot differ from the scan's. To keep that true for the comparison rules of
 * {@code InlineProperties.matchesResolvedValue} (equals, or numeric equality across types) the lookup is offered
 * only for a String or an integral number; a stored value that could equal an integral number without being one
 * (a Double, a Float, a BigDecimal) is kept aside and returned for every numeric lookup. A null never matches, so
 * records without the property are not stored at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ScanPropertyHashIndex {
  /** Above this many stored Double/Float/BigDecimal values a numeric lookup would return them all, so it declines instead. */
  static final int MAX_NON_INTEGRAL = 64;
  /** A key with fewer distinct values than this over at least {@link #MIN_RECORDS_TO_JUDGE} records is not selective. */
  static final int MIN_DISTINCT_KEYS = 8;
  static final int MIN_RECORDS_TO_JUDGE = 64;

  // value: a RID for a key held by one record (the common case, no list allocated), else an ArrayList<RID>
  private final Map<Object, Object> byKey       = new HashMap<>();
  private final List<RID>           nonIntegral = new ArrayList<>();
  /** The records holding a temporal: a text is only read as one by a declaration, so they are kept aside for a text lookup. */
  private final List<RID>           temporals   = new ArrayList<>();
  private       int                 hashed;
  /** Whether any record holds a text, the only key an integer's digits can be found under. */
  private       boolean             anyText;
  /** Whether any record holds an integer or a boolean, the keys a text written into a declared property can be found under. */
  private       boolean             anyTyped;

  /** Drains {@code scan}, loading each record's {@code propertyName}; {@code guard} enforces the command deadline. The records' order is kept within a key. */
  ScanPropertyHashIndex(final Iterator<Identifiable> scan, final String propertyName, final WorkGuard guard) {
    while (scan.hasNext()) {
      guard.check();
      final Identifiable identifiable = scan.next();
      final Document record;
      try {
        record = identifiable.asDocument();
      } catch (final RecordNotFoundException e) {
        continue;
      }
      final Object value = record.get(propertyName);
      if (value == null)
        continue;

      final RID rid = record.getIdentity();
      hashed++;
      if (value instanceof String) {
        put(value, rid);
        anyText = true;
      } else if (isIntegral(value)) {
        put(((Number) value).longValue(), rid);
        anyTyped = true;
      } else if (value instanceof Number)
        nonIntegral.add(rid);
      else if (value instanceof Boolean) {
        put(value, rid);
        anyTyped = true;
      } else if (TemporalUtil.isNativeTemporal(value) || value instanceof CypherTemporalValue)
        temporals.add(rid);
    }
  }

  @SuppressWarnings("unchecked")
  private void put(final Object key, final RID rid) {
    final Object existing = byKey.putIfAbsent(key, rid);
    if (existing == null)
      return;
    if (existing instanceof List)
      ((List<RID>) existing).add(rid);
    else {
      final List<RID> list = new ArrayList<>(4);
      list.add((RID) existing);
      list.add(rid);
      byKey.put(key, list);
    }
  }

  /**
   * True when the key narrows the scan: a property with a handful of distinct values (a status, a kind) hands back a
   * bucket as large as a scan would have read, plus a reload by RID per candidate, so it is not worth answering from.
   */
  boolean isSelective() {
    return hashed < MIN_RECORDS_TO_JUDGE || byKey.size() >= MIN_DISTINCT_KEYS;
  }

  /** True when {@link #candidates(Object)} can answer for this expected value. */
  static boolean isSupported(final Object expected) {
    return expected instanceof String || isIntegral(expected);
  }

  /**
   * The records that may equal {@code expected} (a supported value), in scan order within a key (a numeric lookup lists the
   * exact matches first, then the texts spelling the number, then the non-integral numbers; a text lookup the exact texts
   * first, then the values the text can be read as), or null when the hash cannot answer cheaply - a lookup against a type
   * holding many values no key can find - and the caller must scan.
   */
  Iterator<Identifiable> candidates(final Object expected) {
    final boolean text = expected instanceof String;
    if (text)
      return textCandidates((String) expected);
    if (nonIntegral.size() > MAX_NON_INTEGRAL)
      return null;

    final Object exact = byKey.get(((Number) expected).longValue());
    // An integer written into a property declared STRING is stored as its digits ('1' for 1), and the pattern that wrote it
    // has to find it again (issue #9695): those records are candidates too. The caller's re-check keeps a schemaless '1' out
    final Object digits = anyText ? byKey.get(expected.toString()) : null;
    if (nonIntegral.isEmpty() && digits == null)
      return iterator(exact);

    final List<Identifiable> merged = new ArrayList<>(nonIntegral.size() + 2);
    addAll(merged, exact);
    addAll(merged, digits);
    merged.addAll(nonIntegral);
    return merged.iterator();
  }

  /**
   * A text written into a property declared with another type is stored as that type ({@code 5} for {@code '5'} in an
   * INTEGER, {@code true} for {@code 'true'} in a BOOLEAN), and the pattern that wrote it has to find it again (issue #9794):
   * the records holding a value the text can be read as are candidates too. The caller's re-check keeps out a value no
   * declaration reads the text as, such as a schemaless {@code 5} against {@code '5'}.
   */
  private Iterator<Identifiable> textCandidates(final String text) {
    final Object exact = byKey.get(text);
    if (!anyTyped && nonIntegral.isEmpty() && temporals.isEmpty())
      return iterator(exact);

    // the integer and the boolean a write of the text stores (Integer.parseInt and Boolean's text, see Type.convert)
    final Long integral = anyTyped ? integralOf(text) : null;
    final Object integralRecords = integral != null ? byKey.get(integral) : null;
    final Object booleanRecords = anyTyped && ("true".equalsIgnoreCase(text) || "false".equalsIgnoreCase(text)) ?
        byKey.get(Boolean.valueOf(text)) :
        null;
    // a FLOAT, DOUBLE or DECIMAL is only stored from a text a number parses, a temporal only from one of a temporal's shape
    final List<RID> fractional = !nonIntegral.isEmpty() && isDecimalText(text) ? nonIntegral : List.of();
    final List<RID> temporal = !temporals.isEmpty() && TemporalUtil.mayBeTemporalString(text) ? temporals : List.of();
    if (fractional.size() + temporal.size() > MAX_NON_INTEGRAL)
      return null;
    if (integralRecords == null && booleanRecords == null && fractional.isEmpty() && temporal.isEmpty())
      return iterator(exact);

    final List<Identifiable> merged = new ArrayList<>(fractional.size() + temporal.size() + 3);
    addAll(merged, exact);
    addAll(merged, integralRecords);
    addAll(merged, booleanRecords);
    merged.addAll(fractional);
    merged.addAll(temporal);
    return merged.iterator();
  }

  /**
   * Whether a FLOAT, DOUBLE or DECIMAL declaration can read the text as a number ({@code Double.parseDouble}, which
   * {@code Type.convert} uses, ignores surrounding blanks). Only a text starting the way a number does is parsed, so an
   * ordinary text never costs a thrown exception.
   */
  private static boolean isDecimalText(final String text) {
    final String trimmed = text.trim();
    if (trimmed.isEmpty())
      return false;
    final char first = trimmed.charAt(0);
    if (!(first >= '0' && first <= '9') && first != '-' && first != '+' && first != '.' && first != 'I' && first != 'N')
      return false;
    try {
      Double.parseDouble(trimmed);
      return true;
    } catch (final NumberFormatException e) {
      return false;
    }
  }

  /**
   * The integer a text is read as by an integral declaration (an optional sign, then digits), or null: a text of any other
   * shape is refused by the write, so no record holds an integer stored from it. Only that shape is parsed, so an ordinary
   * text never costs a thrown exception.
   */
  private static Long integralOf(final String text) {
    final int length = text.length();
    int start = length > 0 && (text.charAt(0) == '-' || text.charAt(0) == '+') ? 1 : 0;
    if (start == length)
      return null;
    for (int i = start; i < length; i++)
      if (text.charAt(i) < '0' || text.charAt(i) > '9')
        return null;
    // leading zeros are no digits of the value, as for the declaration's own reading of the text (InlineProperties)
    while (start < length - 1 && text.charAt(start) == '0')
      start++;
    if (length - start > 19)
      return null;
    try {
      return Long.parseLong(text);
    } catch (final NumberFormatException e) {
      return null; // out of the range of a long
    }
  }

  @SuppressWarnings("unchecked")
  private static Iterator<Identifiable> iterator(final Object bucket) {
    if (bucket == null)
      return Collections.emptyIterator();
    return bucket instanceof List ? (Iterator<Identifiable>) (Iterator<?>) ((List<RID>) bucket).iterator()
        : Collections.<Identifiable>singletonList((RID) bucket).iterator();
  }

  @SuppressWarnings("unchecked")
  private static void addAll(final List<Identifiable> merged, final Object bucket) {
    if (bucket instanceof List)
      merged.addAll((List<RID>) bucket);
    else if (bucket != null)
      merged.add((RID) bucket);
  }

  private static boolean isIntegral(final Object value) {
    return value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte
        || value instanceof BigInteger;
  }
}
