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
package com.arcadedb.mongo;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.utility.TimeBoundRegex;
import de.bwaldvogel.mongo.backend.DefaultQueryMatcher;
import de.bwaldvogel.mongo.backend.QueryMatcher;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * A MongoDB filter and how it is evaluated against the stored documents.
 * <p>
 * The SQL comparisons the translator relies on are not MongoDB's: they coerce across types ({@code n = 5} matches {@code "5"}
 * and {@code true}), see an array as one opaque value (so {@code tags = 'a'} never matches {@code ['a', 'b']}, and
 * {@code tags <> 'a'} matches it) and answer {@code IS DEFINED} on a nested path from the wrong set. A filter that can hit any of
 * those is therefore evaluated on the document, by the matcher of the MongoDB emulation the plugin is built on, which implements
 * MongoDB's own rules: type brackets, array traversal, {@code $elemMatch}, {@code $all}, {@code $size}, dotted paths, ordering.
 * <p>
 * Only a filter on the {@code _id} alone stays in SQL: an {@code _id} is never an array, so the SQL answers are exact and the
 * unique index on it keeps serving the lookup. Every other filter reads the documents of the type, which is what makes the answers
 * exact for any shape of stored data.
 * <p>
 * Regular expressions are searched through {@link TimeBoundRegex}, bounded by {@code arcadedb.command.regexTimeout} with one
 * deadline per filter, exactly like the SQL {@code MATCHES} the plugin used to rely on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class MongoFilter {
  private static final Pattern ALWAYS = Pattern.compile("");
  private static final Pattern NEVER  = Pattern.compile("(?!)");

  private final Document     original;
  private final boolean      empty;
  private final boolean      sql;
  private final Document     normalized;
  private final QueryMatcher matcher;

  MongoFilter(final Database database, final Document filter) {
    this.original = filter;
    this.empty = filter == null || filter.isEmpty();
    this.sql = empty || onlyId(filter);
    if (sql) {
      this.normalized = null;
      this.matcher = null;
    } else {
      final RegexBudget deadline = new RegexBudget(GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong(database));
      this.normalized = normalizeQuery(filter, deadline, true);
      this.matcher = new DefaultQueryMatcher();
    }
  }

  /**
   * @return true when the filter selects everything
   */
  boolean isEmpty() {
    return empty;
  }

  /**
   * @return true when SQL answers the filter exactly (no filter, or a filter on the {@code _id} alone), false when the stored
   * documents have to be tested one by one
   */
  boolean isSql() {
    return sql;
  }

  /**
   * Appends the {@code WHERE} clause of a filter {@link #isSql() answered by SQL}, nothing for an empty one.
   */
  void appendWhere(final StringBuilder sqlText, final Map<String, Object> params) {
    if (!empty) {
      sqlText.append(" WHERE ");
      MongoDBToSqlTranslator.buildExpression(sqlText, params, original);
    }
  }

  /**
   * Whether a stored record matches the filter. Only meaningful for a filter that is not {@link #isSql() answered by SQL}, which
   * has no clause left to test.
   */
  boolean matches(final Map<String, Object> storedProperties) {
    return matcher.matches(MongoDBToSqlTranslator.toMatchDocument(storedProperties), normalized);
  }

  boolean matches(final com.arcadedb.database.Document record) {
    return sql || matches(record.toMap(false));
  }

  boolean matchesRow(final Result result) {
    return sql || matches(result.toMap());
  }

  /**
   * The identities of the records matching the filter, up to {@code limit} of them (0 for no limit). The selection is a snapshot:
   * the caller applies its change to these records without evaluating the filter again, and skips the ones deleted meanwhile.
   */
  List<RID> select(final Database database, final String collectionName, final int limit) {
    final List<RID> rids = new ArrayList<>();
    if (sql) {
      final Map<String, Object> params = new HashMap<>();
      final StringBuilder text = new StringBuilder("SELECT @rid FROM ").append(Identifier.quote(collectionName));
      appendWhere(text, params);
      if (limit > 0)
        text.append(" LIMIT ").append(limit);
      try (final ResultSet rs = database.query("sql", text.toString(), params)) {
        while (rs.hasNext())
          rs.next().getIdentity().ifPresent(rids::add);
      }
    } else {
      for (final Iterator<Record> it = database.iterateType(collectionName, false); it.hasNext(); ) {
        final Record record = it.next();
        if (record instanceof com.arcadedb.database.Document document && matches(document.toMap(false))) {
          rids.add(record.getIdentity());
          if (limit > 0 && rids.size() >= limit)
            break;
        }
      }
    }
    return rids;
  }

  private static boolean onlyId(final Document filter) {
    for (final String key : filter.keySet())
      if (!"_id".equals(key))
        return false;
    return true;
  }

  /**
   * Prepares a query document for the matcher: a regular expression, however it is spelled, becomes a {@link BoundedRegex}, and
   * the {@code _id} operand takes the form the {@code _id} is stored in.
   */
  private static Document normalizeQuery(final Document query, final RegexBudget deadline, final boolean top) {
    final Document result = new Document();
    List<Object> lifted = null;
    for (final Map.Entry<String, Object> entry : query.entrySet()) {
      final String key = entry.getKey();
      final Object value = entry.getValue();

      if ("$and".equals(key) || "$or".equals(key) || "$nor".equals(key)) {
        if (!(value instanceof List<?> list))
          throw new IllegalArgumentException("Operator " + key + " requires an array");
        final List<Object> converted = new ArrayList<>(list.size());
        for (final Object item : list)
          converted.add(item instanceof Document document ? normalizeQuery(document, deadline, top) : item);
        result.put(key, converted);
        continue;
      }
      if (key.startsWith("$")) {
        result.put(key, value);
        continue;
      }

      if (top && "_id".equals(key)) {
        result.put(key, MongoBsonValues.idFilter(value));
        continue;
      }

      if (value instanceof BsonRegularExpression regex)
        result.put(key, new BoundedRegex(regex.getPattern(), regex.getOptions(), deadline));
      else if (value instanceof ObjectId objectId)
        result.put(key, new Document("$in", bothForms(objectId)));
      else if (value instanceof Document document && isOperatorDocument(document)) {
        if (document.containsKey("$regex")) {
          final Document rest = withoutRegex(document);
          final BoundedRegex regex = BoundedRegex.of(document, deadline);
          if (rest.isEmpty())
            result.put(key, regex);
          else {
            // {$regex, $ne, ...}: the regular expression becomes a condition of its own, next to the others
            result.put(key, normalizeOperators(rest, deadline));
            if (lifted == null)
              lifted = new ArrayList<>();
            lifted.add(new Document(key, regex));
          }
        } else
          result.put(key, normalizeOperators(document, deadline));
      } else
        result.put(key, value);
    }

    if (lifted == null)
      return result;
    final List<Object> all = new ArrayList<>(lifted.size() + 1);
    all.add(result);
    all.addAll(lifted);
    return new Document("$and", all);
  }

  /**
   * An ObjectId stored outside the {@code _id} is tagged, but data written before the tagging holds the bare hex string: both forms
   * are the same value. Only for the operands that compare one value ({@code $eq}, {@code $ne}, {@code $in}, {@code $nin}, a plain
   * equality): an array operand ({@code {refs: [oid]}}) and {@code $all} match the tagged form only.
   */
  private static List<Object> bothForms(final ObjectId objectId) {
    return List.of(objectId, objectId.getHexData());
  }

  /**
   * The operators applied to one field: only the places a regular expression can sit need a look.
   */
  private static Document normalizeOperators(final Document operators, final RegexBudget deadline) {
    final Document result = new Document();
    for (final Map.Entry<String, Object> entry : operators.entrySet()) {
      final String operator = entry.getKey();
      final Object operand = entry.getValue();
      switch (operator) {
      case "$not" -> {
        if (operand instanceof BsonRegularExpression regex)
          result.put(operator, new BoundedRegex(regex.getPattern(), regex.getOptions(), deadline));
        else if (operand instanceof Document document && isOperatorDocument(document))
          result.put(operator, regexOnly(document, deadline));
        else
          result.put(operator, operand);
      }
      case "$eq", "$ne" -> {
        if (operand instanceof ObjectId objectId)
          result.put("$eq".equals(operator) ? "$in" : "$nin", bothForms(objectId));
        else
          result.put(operator, operand);
      }
      case "$in", "$nin", "$all" -> {
        if (operand instanceof List<?> list) {
          final List<Object> converted = new ArrayList<>(list.size());
          for (final Object item : list)
            if (item instanceof BsonRegularExpression regex)
              converted.add(new BoundedRegex(regex.getPattern(), regex.getOptions(), deadline));
            else {
              converted.add(item);
              // not for $all, which asks for each element: the hex form of an ObjectId is not an extra one
              if (item instanceof ObjectId objectId && !"$all".equals(operator))
                converted.add(objectId.getHexData());
            }
          result.put(operator, converted);
        } else
          result.put(operator, operand);
      }
      case "$elemMatch" -> {
        if (operand instanceof Document document) {
          if (isOperatorDocument(document)) {
            // the matcher builds the expression of an operator document itself, out of reach of the time bound
            if (document.containsKey("$regex"))
              throw new IllegalArgumentException("$regex directly inside $elemMatch is not supported, match a field of the element");
            result.put(operator, normalizeOperators(document, deadline));
          } else
            result.put(operator, normalizeQuery(document, deadline, false));
        } else
          result.put(operator, operand);
      }
      default -> result.put(operator, operand);
      }
    }
    return result;
  }

  /**
   * An operator document that may be nothing but a regular expression, which is what the places that cannot take a second
   * condition (an operand of {@code $not} or {@code $elemMatch}) accept; any other operators are normalized as usual.
   */
  private static Object regexOnly(final Document document, final RegexBudget deadline) {
    if (!document.containsKey("$regex"))
      return normalizeOperators(document, deadline);
    if (!withoutRegex(document).isEmpty())
      throw new IllegalArgumentException("$regex cannot be combined with other operators here");
    return BoundedRegex.of(document, deadline);
  }

  private static Document withoutRegex(final Document document) {
    final Document rest = new Document();
    for (final Map.Entry<String, Object> entry : document.entrySet())
      if (!"$regex".equals(entry.getKey()) && !"$options".equals(entry.getKey()))
        rest.put(entry.getKey(), entry.getValue());
    return rest;
  }

  private static boolean isOperatorDocument(final Document document) {
    return !document.isEmpty() && document.keySet().iterator().next().startsWith("$");
  }

  /**
   * The time a filter may spend in regular expressions, shared by every search of the filter: {@code arcadedb.command.regexTimeout}
   * counts the time inside the expressions only, not the reading of the documents around them, so a long scan with a harmless
   * expression is not cut short while a pathological one still is. Not thread-safe: a filter is built per command and evaluated by
   * one thread.
   */
  private static final class RegexBudget {
    private final long timeoutNanos;
    private       long remainingNanos;

    RegexBudget(final long timeoutMillis) {
      this.timeoutNanos = timeoutMillis > 0 ? timeoutMillis * 1_000_000L : 0;
      this.remainingNanos = timeoutNanos;
    }

    boolean find(final Pattern pattern, final String input) {
      if (timeoutNanos <= 0)
        return TimeBoundRegex.findUntil(pattern, input, Long.MAX_VALUE);
      final long start = System.nanoTime();
      try {
        return TimeBoundRegex.findUntil(pattern, input, start + remainingNanos);
      } finally {
        remainingNanos = Math.max(1, remainingNanos - (System.nanoTime() - start));
      }
    }
  }

  /**
   * A regular expression whose search is bounded by a deadline shared by the whole filter. The matcher asks the expression for a
   * {@link Matcher} over the value; the answer is computed here, through {@link TimeBoundRegex}, and handed back as a matcher
   * that finds (or does not) whatever the value is.
   */
  private static final class BoundedRegex extends BsonRegularExpression {
    private final Pattern pattern;
    private final RegexBudget deadline;

    BoundedRegex(final String regex, final String options, final RegexBudget deadline) {
      super(regex, options);
      this.deadline = deadline;
      this.pattern = compile(regex, options);
    }

    static BoundedRegex of(final Document operators, final RegexBudget deadline) {
      final Object regex = operators.get("$regex");
      final Object options = operators.get("$options");
      // {$regex: /abc/i}: the literal brings its own flags, an explicit $options wins
      if (regex instanceof BsonRegularExpression literal)
        return new BoundedRegex(literal.getPattern(), options != null ? options.toString() : literal.getOptions(), deadline);
      return new BoundedRegex(String.valueOf(regex), options != null ? options.toString() : null, deadline);
    }

    private static Pattern compile(final String regex, final String options) {
      int flags = Pattern.UNICODE_CASE;
      if (options != null)
        for (int i = 0; i < options.length(); i++) {
          final char flag = options.charAt(i);
          switch (flag) {
          case 'i' -> flags |= Pattern.CASE_INSENSITIVE;
          case 'm' -> flags |= Pattern.MULTILINE;
          case 's' -> flags |= Pattern.DOTALL;
          case 'x' -> flags |= Pattern.COMMENTS;
          case 'u' -> {
            // always on
          }
          default -> throw new IllegalArgumentException("Unknown regular expression option '" + flag + "'");
          }
        }
      try {
        return Pattern.compile(regex, flags);
      } catch (final PatternSyntaxException e) {
        throw new IllegalArgumentException("Invalid regular expression '" + regex + "': " + e.getDescription(), e);
      }
    }

    @Override
    public Matcher matcher(final String string) {
      return (deadline.find(pattern, string) ? ALWAYS : NEVER).matcher(string);
    }
  }
}
