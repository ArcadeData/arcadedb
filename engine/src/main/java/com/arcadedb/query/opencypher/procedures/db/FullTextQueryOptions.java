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
package com.arcadedb.query.opencypher.procedures.db;

import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * The trailing {@code options} map shared by {@code db.index.fulltext.queryNodes} and
 * {@code db.index.fulltext.queryRelationships}.
 * <p>
 * Neo4j declares both as {@code (indexName :: STRING, queryString :: STRING, options = {} :: MAP)} and documents
 * three keys: {@code skip}, {@code limit} and {@code analyzer}. ArcadeDB accepted two arguments and rejected the
 * three-argument form outright (issue #8103). {@code skip} and {@code limit} are honoured here; {@code analyzer}
 * and any other key are refused <b>by name</b>, because ArcadeDB resolves the analyzer from the metadata written
 * when the index was created and has no query-time override - accepting the call and ignoring the key would run
 * the search under an analyzer the caller did not ask for and report success, which is worse than the arity error
 * this replaces.
 */
final class FullTextQueryOptions {
  static final String SKIP      = "skip";
  static final String LIMIT     = "limit";
  /** A Neo4j key ArcadeDB cannot honour; called out separately so its refusal can say why rather than "unknown". */
  static final String ANALYZER  = "analyzer";

  private static final Set<String> SUPPORTED_KEYS = Set.of(SKIP, LIMIT);

  /** Means "no limit", the same sentinel {@code FullTextSearch.search} takes. */
  static final int UNBOUNDED = -1;

  private static final FullTextQueryOptions NONE = new FullTextQueryOptions(0, UNBOUNDED);

  /**
   * Descending score, ties broken by RID.
   * <p>
   * The tie-break is what lets {@code skip}/{@code limit} state a contract at all: the scores come back in a
   * {@code Map<RID, Float>}, so equally-scoring records would otherwise be ordered by hash iteration, which follows
   * hash distribution and table size and is promised by nothing. In practice today the map does iterate consecutive
   * RIDs in RID order, so this is a guarantee replacing a coincidence rather than a bug being fixed - no test can
   * separate the two, and {@code DbIndexFulltextQueryOptionsTest.equallyScoringRecordsAreOrderedByRid} says so
   * (issue #8103). Records that do not tie are unaffected, which is every assertion the pre-existing ranking tests
   * make. The direction matches the per-bucket tie-break {@code LSMTreeFullTextIndex.buildScoredCursor} already
   * applies.
   */
  private static final Comparator<Map.Entry<RID, Float>> BY_SCORE_THEN_RID =
      Comparator.<Map.Entry<RID, Float>, Float>comparing(Map.Entry::getValue, Comparator.reverseOrder())
          .thenComparing(Map.Entry::getKey);

  private final int skip;
  private final int limit;

  private FullTextQueryOptions(final int skip, final int limit) {
    this.skip = skip;
    this.limit = limit;
  }

  /**
   * Reads the options out of a call's argument array, defaulting to "no skip, no limit" when the trailing slot was
   * omitted or passed as {@code null}.
   * <p>
   * Every call site runs {@code validateArgs} first, so {@code args} is non-null and carries at least the index name
   * and the query string; the slot is possibly absent only because {@code getMinArgs()} stays at 2.
   */
  static FullTextQueryOptions parse(final String procedureName, final Object[] args) {
    if (args.length < 3 || args[2] == null)
      return NONE;

    if (!(args[2] instanceof final Map<?, ?> options))
      throw new CommandSemanticException(
          procedureName + "(): options must be a map, got " + args[2].getClass().getSimpleName());

    if (options.isEmpty())
      return NONE;

    for (final Object key : options.keySet()) {
      final String name = key == null ? "null" : key.toString();
      if (ANALYZER.equals(name))
        throw new CommandSemanticException(procedureName + "(): the 'analyzer' option is not supported - the analyzer "
            + "is fixed by the index metadata at index-creation time, so it cannot be overridden per query. "
            + "Supported options: " + SKIP + ", " + LIMIT);
      if (!SUPPORTED_KEYS.contains(name))
        throw new CommandSemanticException(procedureName + "(): unsupported option '" + name + "'. Supported options: "
            + SKIP + ", " + LIMIT);
    }

    final int skip = intOption(procedureName, options, SKIP, 0);
    final int limit = intOption(procedureName, options, LIMIT, UNBOUNDED);

    return skip == 0 && limit == UNBOUNDED ? NONE : new FullTextQueryOptions(skip, limit);
  }

  /**
   * Reads one non-negative integer option.
   * <p>
   * A fractional value is refused rather than truncated, because answering {@code limit: 1.5} with {@code limit: 1}
   * answers a question the caller did not ask. A whole number beyond {@code Integer.MAX_VALUE} is clamped to it
   * rather than refused: the search bound is an {@code int}, and no result set this side of that bound is affected
   * by the difference.
   * <p>
   * The order of the three checks is what makes them exact for every {@link Number} implementation, rather than for
   * the {@code Long} and {@code Integer} the Cypher runtime happens to produce today. {@code doubleValue()} decides
   * sign and magnitude for all of them - including a {@code BigInteger}, whose {@code longValue()} would otherwise
   * wrap around silently into a small positive bound. Only once the value is known to sit within {@code int} range
   * is {@code longValue()} read, and there the {@code double} round-trip is lossless, so it tells a whole number
   * from a fractional one exactly. Testing that round-trip first instead would have rejected a {@code Long} above
   * 2^53, which is a whole number this clamps.
   */
  private static int intOption(final String procedureName, final Map<?, ?> options, final String name,
      final int defaultValue) {
    final Object value = options.get(name);
    if (value == null)
      return defaultValue;

    if (!(value instanceof final Number number))
      throw new CommandSemanticException(
          procedureName + "(): option '" + name + "' must be an integer, got " + value.getClass().getSimpleName());

    final double asDouble = number.doubleValue();
    if (Double.isNaN(asDouble))
      throw new CommandSemanticException(procedureName + "(): option '" + name + "' must be an integer, got " + number);
    if (asDouble < 0)
      throw new CommandSemanticException(
          procedureName + "(): option '" + name + "' must not be negative, got " + number);
    if (asDouble > Integer.MAX_VALUE)
      return Integer.MAX_VALUE;

    final long asLong = number.longValue();
    if (asDouble != asLong)
      throw new CommandSemanticException(procedureName + "(): option '" + name + "' must be an integer, got " + number);

    return (int) asLong;
  }

  int skip() {
    return skip;
  }

  /** {@link #UNBOUNDED} when the caller set no limit. */
  int limit() {
    return limit;
  }

  /** A {@code limit} of zero asks for no rows at all, so the search itself can be skipped. */
  boolean returnsNothing() {
    return limit == 0;
  }

  /**
   * How many top-scoring matches the search has to produce for {@link #skip()}/{@link #limit()} to be applied to
   * them: {@code skip + limit}, saturating, or {@link #UNBOUNDED} when there is no limit.
   * <p>
   * Pushing the bound down is sound however many bucket indexes back the type index. A document in the global top-K
   * is necessarily in its own bucket's top-K - at most {@code K - 1} documents outscore it anywhere, so at most
   * {@code K - 1} outscore it within one bucket - so the union of the per-bucket top-K sets that
   * {@code FullTextSearch.search} returns always contains the global top-K, whatever the similarity in use.
   */
  int searchLimit() {
    if (limit == UNBOUNDED)
      return UNBOUNDED;
    final long total = (long) skip + limit;
    return total > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) total;
  }

  /**
   * Runs the search under this options' bound and returns the requested page.
   * <p>
   * The bound is {@link #searchLimit()}, which is what keeps a small page from scoring the whole match set. A
   * posting whose record is gone is dropped by {@link #page}, <b>after</b> that bound has already been applied, so a
   * stale posting inside the bounded window would otherwise cost the page a row that a live match further down could
   * have filled. When that happens - the page came back short although the bounded search came back full - the
   * search is repeated unbounded and re-paged, which is the only way to recover a posting the bound excluded. The
   * retry is reachable only when such a posting lands inside the window, so the bound still pays for itself on
   * every other call.
   * <p>
   * A single-threaded caller does not produce one: a delete removes the index posting along with the record, which
   * is why no test here executes this arm. A concurrent one can - another session's delete landing between this
   * search and the record load behind {@code loader} - so the arm is a live path under concurrency rather than
   * dead code, just not one a deterministic test can stage.
   *
   * @param search     runs the underlying search for a given limit ({@link #UNBOUNDED} for all matches)
   * @param yieldField the name of the record column this procedure yields ({@code node} or {@code relationship})
   * @param loader     turns a matched RID into the record to yield
   */
  List<Result> rows(final IntFunction<Map<RID, Float>> search, final String yieldField,
      final Function<RID, Object> loader) {
    final int bound = searchLimit();
    final Map<RID, Float> matches = search.apply(bound);
    final List<Result> page = page(matches, yieldField, loader);

    // matches.size() < bound means the index had nothing more to give, so a short page is the true answer. The
    // comparison is an over-approximation for a multi-bucket index, whose match set is a union of per-bucket top-K
    // sets and can exceed the bound; that only ever costs a retry that finds the same rows.
    if (bound != UNBOUNDED && page.size() < limit && matches.size() >= bound)
      return page(search.apply(UNBOUNDED), yieldField, loader);

    return page;
  }

  /**
   * Ranks {@code matches}, drops the postings whose record is gone, then applies {@code skip} and {@code limit} to
   * what is left, yielding one row per surviving record carrying {@code yieldField} and {@code score}.
   * <p>
   * Skip and limit are counted over the <b>live</b> records rather than over the raw postings, so a stale posting
   * inside the skipped prefix cannot shift the page by one. The loop stops as soon as the limit is reached, which is
   * what keeps a small page from materializing the whole match set; it is also why the ordering above has to be
   * total.
   * <p>
   * This method sees only the matches it is handed, so a stale posting inside a bounded window still costs the page
   * a row here; recovering that row needs a wider search, which is {@link #rows}'s job rather than this one's.
   *
   * @param loader turns a matched RID into the record to yield ({@code asDocument} or {@code asEdge}), throwing
   *               {@link RecordNotFoundException} when the posting is stale
   */
  List<Result> page(final Map<RID, Float> matches, final String yieldField, final Function<RID, Object> loader) {
    final List<Map.Entry<RID, Float>> sorted = new ArrayList<>(matches.entrySet());
    sorted.sort(BY_SCORE_THEN_RID);

    final List<Result> results = new ArrayList<>(limit == UNBOUNDED ? sorted.size() : Math.min(limit, sorted.size()));
    int skipped = 0;

    for (final Map.Entry<RID, Float> entry : sorted) {
      final Object record;
      try {
        record = loader.apply(entry.getKey());
      } catch (final RecordNotFoundException e) {
        // Stale posting: the record was deleted since the index was last updated, skip it (same handling as the
        // SEARCH_INDEX() SQL function's searchFromTarget()).
        continue;
      }

      if (skipped < skip) {
        ++skipped;
        continue;
      }

      final ResultInternal row = new ResultInternal();
      row.setProperty(yieldField, record);
      row.setProperty("score", entry.getValue());
      results.add(row);

      if (limit != UNBOUNDED && results.size() >= limit)
        break;
    }

    return results;
  }
}
