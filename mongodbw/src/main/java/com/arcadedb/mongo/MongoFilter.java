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
import com.arcadedb.exception.TimeoutException;
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
import java.util.function.Predicate;
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
 * Only a filter on the {@code _id} alone stays in SQL: an {@code _id} is never an array, so the SQL answers are exact for the
 * shapes of data an {@code _id} takes and the unique index on it keeps serving the lookup. (SQL still coerces across the types of an
 * {@code _id}, such as a number and its string, which share one index key: a collection mixing those is the one case it is not exact.) Every other filter reads the documents of the type, which is what makes the answers
 * exact for any shape of stored data.
 * <p>
 * Regular expressions are searched through {@link TimeBoundRegex}, bounded by {@code arcadedb.command.regexTimeout} with one
 * deadline per filter, exactly like the SQL {@code MATCHES} the plugin used to rely on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class MongoFilter {
  // com.arcadedb.database.Document is spelled out in full below: its simple name is the one of the MongoDB Document imported here
  private static final Pattern ALWAYS = Pattern.compile("");
  private static final Pattern NEVER  = Pattern.compile("(?!)");

  private final Document     original;
  private final boolean      empty;
  private final boolean      sql;
  private final Document     idPart;
  private final Document     normalized;
  private final QueryMatcher matcher;

  MongoFilter(final Database database, final Document filter) {
    this(database, filter, null);
  }

  /**
   * @param budget the regex budget of the whole command, shared by the filters of its entries (a bulk update or delete holds one
   *               filter per entry), or {@code null} for one of its own
   */
  MongoFilter(final Database database, final Document filter, final RegexBudget budget) {
    this.original = filter;
    this.empty = filter == null || filter.isEmpty();
    this.sql = empty || onlyId(filter);
    // the _id conjunct of a filter the SQL cannot answer as a whole narrows the candidates through the unique index: an _id is never
    // an array, so the SQL is exact for it, and the matcher still tests the whole filter on what it returns
    this.idPart = !sql && filter.containsKey("_id") ? new Document("_id", filter.get("_id")) : null;
    if (sql) {
      this.normalized = null;
      this.matcher = null;
    } else {
      this.normalized = normalizeQuery(filter, budget != null ? budget : RegexBudget.of(database), true);
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
    if (sql)
      throw new IllegalStateException("A filter answered by SQL has no clause to test a record against");
    return matcher.matches(MongoDBToSqlTranslator.toMatchDocument(storedProperties), normalized);
  }

  boolean matches(final com.arcadedb.database.Document record) {
    return matches(record.toMap(false));
  }

  boolean matchesRow(final Result result) {
    return matches(result.toMap());
  }

  /**
   * @return true when the filter is not answered by SQL as a whole but has an {@code _id} conjunct that SQL answers exactly, to
   * read the candidates through the index of the {@code _id} instead of the whole type
   */
  boolean narrowsById() {
    return idPart != null;
  }

  /**
   * Appends the {@code WHERE} clause that selects the candidates of the filter: all of it for a filter answered by SQL, its
   * {@code _id} conjunct for one that {@link #narrowsById() narrows by _id}, nothing for any other.
   */
  void appendCandidateWhere(final StringBuilder sqlText, final Map<String, Object> params) {
    if (sql)
      appendWhere(sqlText, params);
    else if (idPart != null) {
      sqlText.append(" WHERE ");
      MongoDBToSqlTranslator.buildExpression(sqlText, params, idPart);
    }
  }

  /**
   * Visits the identity of every record that matches a filter not answered by SQL, until the visitor answers {@code false}.
   */
  void scanMatches(final Database database, final String collectionName, final Predicate<RID> visitor) {
    if (idPart != null) {
      final Map<String, Object> params = new HashMap<>();
      final StringBuilder text = new StringBuilder("SELECT FROM ").append(Identifier.quote(collectionName));
      appendCandidateWhere(text, params);
      try (final ResultSet rs = database.query("sql", text.toString(), params)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          if (matchesRow(row) && row.getIdentity().isPresent() && !visitor.test(row.getIdentity().get()))
            return;
        }
      }
    } else
      for (final Iterator<Record> it = database.iterateType(collectionName, false); it.hasNext(); ) {
        final Record record = it.next();
        if (record instanceof com.arcadedb.database.Document document && matches(document) && !visitor.test(record.getIdentity()))
          return;
      }
  }

  /**
   * The identities of the records matching the filter, up to {@code limit} of them (0 for no limit). The selection is a snapshot:
   * the caller applies its change to these records without evaluating the filter again, and skips the ones deleted meanwhile. Both
   * steps run in the caller's transaction, which is what keeps a concurrent change from being applied half way.
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
    } else
      scanMatches(database, collectionName, rid -> {
        rids.add(rid);
        return limit <= 0 || rids.size() < limit;
      });
    return rids;
  }

  /**
   * Whether every condition of the filter is on the {@code _id}, alone or under {@code $and} / {@code $or} (the shapes a driver
   * emits for a batch lookup by key).
   */
  private static boolean onlyId(final Document filter) {
    for (final Map.Entry<String, Object> entry : filter.entrySet())
      if (!"_id".equals(entry.getKey()) && !isLogicalOverId(entry.getKey(), entry.getValue()))
        return false;
    return true;
  }

  private static boolean isLogicalOverId(final String key, final Object operand) {
    if (!("$and".equals(key) || "$or".equals(key)) || !(operand instanceof List<?> list) || list.isEmpty())
      return false;
    for (final Object item : list)
      if (!(item instanceof Document document) || document.isEmpty() || !onlyId(document))
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
          converted.add(logicalOperand(key, item, deadline, top));
        result.put(key, converted);
        continue;
      }
      if ("$expr".equals(key))
        // an aggregation expression can carry a regular expression of its own, out of reach of the time bound
        throw new IllegalArgumentException("The operator $expr is not supported");
      if (key.startsWith("$")) {
        result.put(key, value);
        continue;
      }

      // the _id operand takes its stored form first, then goes the way of any other field (its regular expressions are bounded too)
      final Object operand = top && "_id".equals(key) ? MongoBsonValues.idFilter(value) : value;

      if (operand instanceof BsonRegularExpression regex)
        result.put(key, new BoundedRegex(regex.getPattern(), regex.getOptions(), deadline));
      else if (operand instanceof ObjectId objectId)
        result.put(key, new Document("$in", bothForms(objectId)));
      else if (operand instanceof Document document && isOperatorDocument(document)) {
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
        result.put(key, operand);
    }

    if (lifted == null)
      return result;
    final List<Object> all = new ArrayList<>(lifted.size() + 1);
    all.add(result);
    all.addAll(lifted);
    return new Document("$and", all);
  }

  private static Document logicalOperand(final String operator, final Object item, final RegexBudget deadline, final boolean top) {
    if (!(item instanceof Document document))
      throw new IllegalArgumentException("Operator " + operator + " requires an array of documents");
    return normalizeQuery(document, deadline, top);
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
            else if (item instanceof Document document && isOperatorDocument(document))
              // {$all: [{$elemMatch: ...}]}
              converted.add(normalizeOperators(document, deadline));
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
      case "$and", "$or", "$nor" -> {
        // inside an $elemMatch operator document, for one: the regular expressions of the sub-queries are bounded too
        if (operand instanceof List<?> list) {
          final List<Object> converted = new ArrayList<>(list.size());
          for (final Object item : list)
            converted.add(logicalOperand(operator, item, deadline, false));
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
   * expression is not cut short while a pathological one still is. The clock is read every few hundred steps of a search, so a very
   * short value never trips it. Not thread-safe: a filter is built per command and evaluated by one thread.
   */
  static final class RegexBudget {
    private final long    timeoutNanos;
    private       long    remainingNanos;
    private       boolean exhausted;

    static RegexBudget ofMillis(final long timeoutMillis) {
      return new RegexBudget(timeoutMillis);
    }

    static RegexBudget of(final Database database) {
      return new RegexBudget(GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong(database));
    }

    private RegexBudget(final long timeoutMillis) {
      // an oversized timeout saturates (it never expires) instead of wrapping around into a deadline in the past
      long nanos = 0;
      if (timeoutMillis > 0)
        try {
          nanos = Math.multiplyExact(timeoutMillis, 1_000_000L);
        } catch (final ArithmeticException e) {
          nanos = Long.MAX_VALUE;
        }
      this.timeoutNanos = nanos;
      this.remainingNanos = nanos;
    }

    boolean find(final Pattern pattern, final String input) {
      if (timeoutNanos <= 0)
        return TimeBoundRegex.findUntil(pattern, input, Long.MAX_VALUE);
      if (exhausted)
        throw new TimeoutException("Regular expression time of the command exhausted (arcadedb.command.regexTimeout)");
      final long start = System.nanoTime();
      try {
        final long now = start + remainingNanos;
        // a deadline that wraps around (a budget that is effectively endless) is no deadline
        return TimeBoundRegex.findUntil(pattern, input, now < start ? Long.MAX_VALUE : now);
      } finally {
        if (remainingNanos != Long.MAX_VALUE) {
          remainingNanos -= System.nanoTime() - start;
          exhausted = remainingNanos <= 0;
        }
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

    /**
     * Assumes the matcher only calls {@link Matcher#find()} on what it gets back (true of the mongo-java-server this plugin is
     * built on): the answer is computed here, so only "found or not" is carried. Tests over every regex-bearing operator pin it
     * when the library is upgraded.
     */
    @Override
    public Matcher matcher(final String string) {
      return (deadline.find(pattern, string) ? ALWAYS : NEVER).matcher(string);
    }
  }
}
