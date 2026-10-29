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
package com.arcadedb.function.sql.vector;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.PredicateRidSet;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.QueryOperatorEquals;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.BaseExpression;
import com.arcadedb.query.sql.parser.BaseIdentifier;
import com.arcadedb.query.sql.parser.BinaryCondition;
import com.arcadedb.query.sql.parser.BooleanExpression;
import com.arcadedb.query.sql.parser.EqualsCompareOperator;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.SuffixIdentifier;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.logging.Level;

/**
 * The allow-list of the {@code where} option of {@code vector.neighbors} / {@code vector.sparseNeighbors} (issue #10169): the
 * RIDs whose record satisfies a predicate, decided one candidate at a time as the search asks about it. Restricting a search by
 * a property no longer means materializing the set of every RID in scope up front (a {@code filter} subquery): only the
 * candidates the search actually considers are loaded and tested, each once per call.
 * <p>
 * The predicate is either a map of property to value, every entry of which must hold with the SQL {@code =} semantics
 * ({@code where: { embed_space: :space }}), or a SQL condition evaluated on the record ({@code where: "year >= :minYear"}),
 * whose parameters must be named: a positional {@code ?} is refused, since the condition is parsed on its own and could not tell
 * which of the query's positional values it meant. The {@code WHERE} of the Cypher {@code SEARCH} clause hands its own
 * predicate instead ({@link #matching}, issue #10782). A
 * {@code filter} given as well restricts the candidates further, and is checked first, so a candidate outside it is never
 * loaded.
 * <p>
 * The indexes consume an allow-list through {@link #contains} and {@link #isEmpty}, and enumerate it only when it is small
 * enough to scan its members instead of the graph. This set cannot be enumerated without the {@code filter}: its size is then
 * reported as unbounded, which never qualifies for that plan by itself (an index may still list its candidates, see below), and
 * {@link #iterator()} refuses. With a {@code filter} its size is the filter's, an upper bound, and enumerating it tests each
 * member of the filter.
 * <p>
 * Safe for the concurrent searches of a sparse fan-out: the memo is concurrent and the records are loaded outside any lock,
 * so a slow load holds no other worker back. Only a SQL condition is evaluated under this set's lock, because it evaluates in
 * the caller's {@link CommandContext}, which is not meant to be shared between threads. Two workers meeting the same candidate
 * may both test it; the answer is the same. A caller's predicate ({@link #matching}) is evaluated under the lock too, for the
 * same reason: the {@code SEARCH} clause's evaluates in the caller's context, rebinding one row to each candidate. The memo
 * holds one entry per candidate the search met and lives as long as the call. A selective predicate keeps the walk going
 * until it holds k members, so in the worst case (fewer than k matches) that is one entry per record of the index, each one
 * paid for by the record load that decided it, which dwarfs it; the statement's deadline bounds the walk (issue #10782).
 * <p>
 * The cost follows the predicate's selectivity. A predicate that keeps a fair share of the records is met within the graph walk,
 * after testing a few candidates per result. One that keeps few or none leaves the walk short of {@code k}, and the dense index
 * then completes the answer by testing every indexed vector, each record once: an exact answer, at the cost of the full load a
 * {@code filter} subquery pays on every call anyway. Capping that walk would answer fewer rows than exist, so it is not capped;
 * for a small, known set of RIDs the {@code filter} option remains the plan that scans only them.
 * <p>
 * A selective predicate an index can answer skips that walk (issue #10921): when its equalities ({@code where: {tenant: 1}}, or
 * the {@code tenant = :t} conjuncts of a condition) bind every property of an index on the searched bucket, the index lists the
 * candidates ({@link #listCandidates}), and when they are few enough the search scores them directly, the plan of a short
 * {@code filter}. Candidates the index answers exactly need no record loaded; the others are tested nearest first.
 * <p>
 * Records are read with the caller's permissions, as every other read of the search: a candidate the caller may not read is
 * not accepted, so the predicate can neither reveal it nor fail the search over it.
 * <p>
 * Only {@link #contains}, {@link #isEmpty}, {@link #size} and, with a {@code filter}, {@link #iterator} are meaningful.
 * {@link #equals}, {@link #hashCode} and {@link #toString} are identity-based and never enumerate the set.
 * <p>
 * Public for {@link #matching} alone, the entry point of a caller outside this package (the Cypher {@code SEARCH} clause);
 * the option parsing of {@code of} stays with the vector functions of this package.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RecordPredicateRidSet extends AbstractSet<RID> implements PredicateRidSet {
  private static final Lookup  NO_LOOKUP  = new Lookup(Map.of(), false);
  private static final Listing NOT_LISTED = new Listing(Integer.MAX_VALUE, null);

  private final BasicDatabase                   database;
  private final CommandContext                  context;
  private final Map<String, Object>             equalities;
  private final WhereClause                     condition;
  private final Predicate<Document>             predicate;
  private final Set<RID>                        restrictTo;
  private final WorkGuard                       guard;
  private final ConcurrentHashMap<RID, Boolean> decided = new ConcurrentHashMap<>();
  // What an index lists of each bucket this call asked about, kept for the call (issue #10921): the grouped plan and the
  // sparse fan-out ask a bucket more than once. An instance is built for one call of the function and dropped with it, so a
  // listing never outlives the statement, its transaction or the index it was read from, as the memo of `decided` does not
  private final ConcurrentHashMap<Integer, Listing> listings = new ConcurrentHashMap<>();
  // The equalities an index may answer, extracted on the first listing: null until then
  private volatile Lookup                       lookup;

  /**
   * The equalities of the predicate an index may answer, and whether they are the whole predicate.
   */
  private record Lookup(Map<String, Object> equalities, boolean whole) {
  }

  /**
   * What listing a bucket answered: its candidates, or null when they are more than {@code bound} or no index lists them
   * ({@code bound} is then {@link Integer#MAX_VALUE}).
   */
  private record Listing(int bound, Candidates candidates) {
  }

  private RecordPredicateRidSet(final CommandContext context, final Map<String, Object> equalities, final WhereClause condition,
      final Predicate<Document> predicate, final Set<RID> restrictTo) {
    this.database = context.getDatabase();
    this.context = context;
    this.equalities = equalities;
    this.condition = condition;
    this.predicate = predicate;
    this.restrictTo = restrictTo;
    // The statement's deadline and termination, asked before each record this set loads (issue #10782): a selective
    // predicate keeps the filtered walk going until it has met every vector it can reach, and the walk itself is
    // JVector's, with no check of its own. The interrupt flag is left alone, as on every generic executor path
    this.guard = WorkGuard.forCommandDeadline(context);
  }

  /**
   * The allow-list of the records {@code predicate} accepts, decided as the search meets each candidate: the {@code WHERE} of
   * the Cypher {@code SEARCH} clause, tested by the index while it walks rather than on a widening window of its nearest
   * candidates (issue #10782). The predicate is called under this set's lock, so it may rely on one call at a time.
   */
  public static Set<RID> matching(final Predicate<Document> predicate, final CommandContext context) {
    return new RecordPredicateRidSet(context, null, null, predicate, null);
  }

  /**
   * Builds the allow-list of a {@code where} option, or answers {@code filter} as it is when there is none.
   *
   * @param where        the option's value: a map of property to value, or a SQL condition; null when the option is absent
   * @param filter       the RIDs of the {@code filter} option, or null when it is absent
   * @param functionName the function the option belongs to, for the error messages
   */
  static Set<RID> of(final Object where, final Set<RID> filter, final String functionName, final CommandContext context) {
    if (where == null)
      return filter;
    if (filter != null && filter.isEmpty())
      // nothing can satisfy the filter: there is nothing to test
      return filter;

    if (where instanceof Map<?, ?> map) {
      if (map.isEmpty())
        throw new CommandSQLParsingException(functionName + " 'where' must name at least one property");
      final LinkedHashMap<String, Object> equalities = new LinkedHashMap<>(map.size());
      for (final Map.Entry<?, ?> e : map.entrySet())
        equalities.put(String.valueOf(e.getKey()), e.getValue());
      return new RecordPredicateRidSet(context, equalities, null, null, filter);
    }
    if (where instanceof String text) {
      if (text.isBlank())
        throw new CommandSQLParsingException(functionName + " 'where' condition is empty");
      // The condition is parsed on its own, so a `?` in it would be numbered from the first positional parameter of the
      // QUERY and read the wrong value without any error: only named parameters bind to what the caller meant
      if (hasPositionalParameter(text))
        throw new CommandSQLParsingException(functionName + " 'where' condition cannot use positional parameters ('?'): use named "
            + "parameters such as ':space', or pass a map of property to value");
      final WhereClause condition;
      try {
        condition = new SQLAntlrParser(context.getDatabase()).parseCondition(text);
      } catch (final CommandSQLParsingException e) {
        throw e;
      } catch (final Exception e) {
        throw new CommandSQLParsingException(functionName + " 'where' is not a valid condition: " + text, e);
      }
      return new RecordPredicateRidSet(context, null, condition, null, filter);
    }
    throw new CommandSQLParsingException(
        functionName + " 'where' must be a map of property to value or a SQL condition, got " + where.getClass().getSimpleName());
  }

  /**
   * Whether {@code condition} holds a {@code ?} outside a string literal or a quoted identifier.
   */
  static boolean hasPositionalParameter(final String condition) {
    char quote = 0;
    for (int i = 0; i < condition.length(); i++) {
      final char c = condition.charAt(i);
      if (quote != 0) {
        if (c == '\\')
          ++i; // an escaped character never closes the literal
        else if (c == quote)
          quote = 0;
      } else if (c == '\'' || c == '"' || c == '`')
        quote = c;
      else if (c == '?')
        return true;
    }
    return false;
  }

  @Override
  public boolean contains(final Object o) {
    if (!(o instanceof RID rid))
      return false;
    if (restrictTo != null && !restrictTo.contains(rid))
      return false;
    final Boolean known = decided.get(rid);
    if (known != null)
      return known;
    final boolean accepted = test(rid);
    decided.putIfAbsent(rid, accepted);
    return accepted;
  }

  private boolean test(final RID rid) {
    // One clock read against a record load: not worth throttling
    guard.check();
    final Document record;
    try {
      // A RID of an internal record (an edge segment, ...) has no properties to test
      if (!(database.lookupByRID(rid, true) instanceof Document document))
        return false;
      record = document;
    } catch (final RecordNotFoundException | SecurityException e) {
      // gone, or not readable by the caller: not a candidate the caller may be answered
      return false;
    }
    if (condition != null)
      synchronized (this) {
        return Boolean.TRUE.equals(condition.matchesFilters(record, context));
      }
    if (predicate != null)
      synchronized (this) {
        return predicate.test(record);
      }
    for (final Map.Entry<String, Object> e : equalities.entrySet())
      if (!QueryOperatorEquals.equals(SQLFunctionVectorAbstract.readNestedField(record, e.getKey()), e.getValue()))
        return false;
    return true;
  }

  /**
   * Lists the records of {@code bucketId} the predicate may accept through an index of that bucket that its equalities bind
   * entirely (issue #10921): {@code where: {tenant: 1}}, or {@code "tenant = :t AND year > 2000"} with an index on
   * {@code tenant}. Without it the index this set restricts has to walk its graph until the predicate accepted {@code k} of
   * the candidates it met, which for a selective predicate is most of the graph, a record loaded for each.
   * <p>
   * The candidates are exact when the index answers the whole predicate (every equality, nothing else, and keys compared as
   * the predicate compares them), so no record has to be loaded; otherwise they are a superset, tested by {@link #contains}
   * one at a time in the order the index ranks them. The lookup reads no more than {@code maxCandidates + 1} entries, so a
   * predicate that keeps a fair share of the records costs a bounded index read before the walk it then takes anyway.
   */
  @Override
  public Candidates listCandidates(final int bucketId, final int maxCandidates) {
    if (maxCandidates < 0)
      return null;
    final Listing known = listings.get(bucketId);
    if (known != null) {
      if (known.candidates != null)
        return known.candidates.rids().size() <= maxCandidates ? known.candidates : null;
      if (known.bound >= maxCandidates)
        return null;
    }
    final Listing listing = list(bucketId, maxCandidates);
    listings.put(bucketId, listing);
    return listing.candidates;
  }

  private Listing list(final int bucketId, final int maxCandidates) {
    // A short `filter` is a listing already: its members are scored, then tested nearest first, rather than each tested first
    if (restrictTo != null && restrictTo.size() <= maxCandidates)
      return new Listing(maxCandidates, new Candidates(restrictTo, false));
    return listThroughIndex(bucketId, maxCandidates);
  }

  @Override
  public boolean listsCandidates(final int bucketId) {
    final Listing known = listings.get(bucketId);
    if (known != null && known != NOT_LISTED)
      return true;
    return restrictTo != null || listingIndex(bucketId, lookup()) != null;
  }

  /**
   * The index of {@code bucketId} that binds the most of the equalities, a unique one first among equals: the shortest
   * listing. Null when none binds every one of its properties.
   */
  private IndexInternal listingIndex(final int bucketId, final Lookup equalitiesToLookUp) {
    if (equalitiesToLookUp.equalities.isEmpty())
      return null;
    final DatabaseInternal db = context.getDatabase();
    final DocumentType type = db.getSchema().getTypeByBucketId(bucketId);
    if (type == null)
      return null;
    IndexInternal best = null;
    for (final IndexInternal index : type.getPolymorphicBucketIndexByBucketId(bucketId, null))
      if (bindsEveryKey(db, type, index, equalitiesToLookUp.equalities) && (best == null
          || index.getPropertyNames().size() > best.getPropertyNames().size()
          || (index.getPropertyNames().size() == best.getPropertyNames().size() && index.isUnique() && !best.isUnique())))
        best = index;
    return best;
  }

  private Listing listThroughIndex(final int bucketId, final int maxCandidates) {
    final Lookup equalitiesToLookUp = lookup();
    final IndexInternal best = listingIndex(bucketId, equalitiesToLookUp);
    if (best == null)
      return NOT_LISTED;

    final DatabaseInternal db = context.getDatabase();
    try {
      db.checkPermissionsOnFile(bucketId, SecurityDatabaseUser.ACCESS.READ_RECORD);
    } catch (final SecurityException e) {
      // no record of the bucket is readable by the caller, so none is a member: what contains() answers for each of them
      return new Listing(Integer.MAX_VALUE, new Candidates(Set.of(), true));
    }

    final DocumentType type = db.getSchema().getTypeByBucketId(bucketId);
    final List<String> properties = best.getPropertyNames();
    final Type[] keyTypes = best.getKeyTypes();
    final Object[] keys = new Object[properties.size()];
    // A case-insensitive key lists the other spellings too: those candidates are tested
    boolean exact = equalitiesToLookUp.whole && properties.size() == equalitiesToLookUp.equalities.size();
    for (int i = 0; i < keys.length; i++) {
      // The value the records the equality accepts hold, and so the key the index holds them by
      keys[i] = storedKey(db, type, properties.get(i), equalitiesToLookUp.equalities.get(properties.get(i)), keyTypes[i]);
      if (keys[i] == null)
        return NOT_LISTED;
      if (best.getMetadata().isCaseInsensitive(i))
        exact = false;
    }

    long[] positions = new long[Math.min(maxCandidates, 64)];
    int count = 0;
    int read = 0;
    try {
      final IndexCursor cursor = best.get(keys, maxCandidates + 1);
      while (cursor.hasNext()) {
        final RID rid = cursor.next().getIdentity();
        if (++read > maxCandidates)
          return new Listing(maxCandidates, null);
        if (rid.getBucketId() != bucketId || (restrictTo != null && !restrictTo.contains(rid)))
          continue;
        if (count == positions.length)
          // count < read <= maxCandidates here, so the array never grows past what the listing may hold
          positions = Arrays.copyOf(positions, Math.min(maxCandidates, Math.max(1, count * 2)));
        positions[count++] = rid.getPosition();
      }
    } catch (final IndexException e) {
      // dropped or rebuilt under the lookup: the walk answers instead
      LogManager.instance().log(this, Level.FINE, "Index '%s' could not list the candidates of %s", e, best.getName(), this);
      return NOT_LISTED;
    }

    Arrays.sort(positions, 0, count);
    int distinct = 0;
    for (int i = 0; i < count; i++)
      if (distinct == 0 || positions[distinct - 1] != positions[i])
        positions[distinct++] = positions[i];
    // No candidate means no member, whatever is left to test
    return new Listing(maxCandidates, new Candidates(new BucketRidSet(bucketId, positions, distinct), exact || distinct == 0));
  }

  /**
   * Whether {@code index} can list candidates for {@code equalities}: an exact-key index ready for queries, every property
   * of which is a plain property the equalities bind to a value with a {@link #storedKey}, so the lookup misses no record the
   * equality accepts. A nested path or a {@code BY ITEM} property is left to the test.
   */
  private static boolean bindsEveryKey(final DatabaseInternal db, final DocumentType type, final IndexInternal index,
      final Map<String, Object> equalities) {
    try {
      final Schema.INDEX_TYPE indexType = index.getType();
      if (indexType == null || !indexType.isExactKeyLookup())
        return false;
      final TypeIndex typeIndex = index.getTypeIndex();
      if (typeIndex == null || !typeIndex.isReadyForQueries())
        return false;
      final List<String> properties = index.getPropertyNames();
      final Type[] keyTypes = index.getKeyTypes();
      if (properties.isEmpty() || keyTypes == null || keyTypes.length != properties.size())
        return false;
      for (int i = 0; i < keyTypes.length; i++) {
        final String property = properties.get(i);
        if (property.indexOf('.') >= 0 || property.indexOf(' ') >= 0
            || storedKey(db, type, property, equalities.get(property), keyTypes[i]) == null)
          return false;
      }
      return true;
    } catch (final IndexException e) {
      return false;
    }
  }

  /**
   * The value a record holds once {@code value} is written to {@code property}, when that record is the one the equality
   * accepts, or null when there is none: the value converted the way a write converts it (a DATE keeps the day of a datetime),
   * then held equal to {@code value} by the SQL {@code =} in the order {@link #test} asks it, stored value first. The index
   * keys a record by its stored value, so a lookup of that value lists exactly the records the equality accepts; a value
   * the write would change into one the equality refuses ({@code 2024-01-02T10:30} for a DATE) has none (issue #10921).
   */
  private static Object storedKey(final DatabaseInternal db, final DocumentType type, final String propertyName, final Object value,
      final Type keyType) {
    if (value == null || keyType == null || !Type.isScalarKeyClass(keyType.getDefaultJavaType()))
      return null;
    final Property property = type.getPolymorphicPropertyIfExists(propertyName);
    if (property == null || property.getType() != keyType)
      return null;
    final Object stored;
    try {
      stored = Type.convert(db, value, keyType.getJavaImplementation(db), property);
    } catch (final RuntimeException e) {
      return null;
    }
    return stored != null && Type.canBeIndexLookupKey(db, stored, keyType.getDefaultJavaType()) && QueryOperatorEquals.equals(stored,
        value) ? stored : null;
  }

  private Lookup lookup() {
    Lookup current = lookup;
    if (current == null) {
      current = extractLookup();
      lookup = current;
    }
    return current;
  }

  /**
   * The equalities of a property to a value the predicate holds whatever the record: the map's, or the {@code property =
   * value} conjuncts of a condition with no {@code OR}, whose value is a literal or a parameter.
   */
  private Lookup extractLookup() {
    if (equalities != null)
      return new Lookup(equalities, true);
    if (condition == null)
      return NO_LOOKUP;

    final List<AndBlock> blocks = condition.flatten();
    if (blocks.size() != 1)
      return NO_LOOKUP;

    final LinkedHashMap<String, Object> found = new LinkedHashMap<>();
    boolean whole = true;
    // the values are read in the caller's context, which is not meant to be shared between threads
    synchronized (this) {
      for (final BooleanExpression conjunct : blocks.getFirst().getSubBlocks()) {
        if (!(conjunct instanceof BinaryCondition binary) || !(binary.getOperator() instanceof EqualsCompareOperator)) {
          whole = false;
          continue;
        }
        String property = propertyName(binary.getLeft());
        Expression valueSide = binary.getRight();
        if (property == null) {
          property = propertyName(binary.getRight());
          valueSide = binary.getLeft();
        }
        if (property == null || valueSide == null || !valueSide.isLiteral(true) || found.containsKey(property)) {
          whole = false;
          continue;
        }
        found.put(property, valueSide.execute((Result) null, context));
      }
    }
    return found.isEmpty() ? NO_LOOKUP : new Lookup(found, whole);
  }

  /**
   * The name of the property {@code expression} reads, when it is a bare property and nothing else.
   */
  private static String propertyName(final Expression expression) {
    if (expression == null || !(expression.getMathExpression() instanceof BaseExpression base) || base.getModifier() != null)
      return null;
    final BaseIdentifier identifier = base.getIdentifier();
    if (identifier == null)
      return null;
    final SuffixIdentifier suffix = identifier.getSuffix();
    return suffix != null && suffix.identifier != null ? suffix.identifier.getStringValue() : null;
  }

  @Override
  public boolean equals(final Object o) {
    return this == o;
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(this);
  }

  @Override
  public String toString() {
    final Object test;
    if (condition != null)
      test = condition;
    else if (predicate != null)
      test = predicate;
    else
      test = equalities;
    return "where(" + test + (restrictTo != null ? ", filter of " + restrictTo.size() : "") + ", " + decided.size() + " decided)";
  }

  /** Never empty: an empty allow-list reads as "no filter" to the indexes. */
  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public int size() {
    return restrictTo != null ? restrictTo.size() : Integer.MAX_VALUE;
  }

  @Override
  public Iterator<RID> iterator() {
    if (restrictTo == null)
      throw new UnsupportedOperationException("The RIDs a 'where' predicate accepts are decided one candidate at a time and cannot be listed");
    final Iterator<RID> members = restrictTo.iterator();
    return new Iterator<>() {
      private RID next = advance();

      private RID advance() {
        while (members.hasNext()) {
          final RID candidate = members.next();
          if (contains(candidate))
            return candidate;
        }
        return null;
      }

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public RID next() {
        if (next == null)
          throw new NoSuchElementException();
        final RID current = next;
        next = advance();
        return current;
      }
    };
  }

  /**
   * The candidates of one bucket, as the sorted positions of their records: a primitive array instead of a set of RIDs, since
   * the listing can reach a fifth of the bucket's vectors. Read-only.
   */
  private static final class BucketRidSet extends AbstractSet<RID> {
    private final int    bucketId;
    private final long[] positions;
    private final int    size;

    private BucketRidSet(final int bucketId, final long[] positions, final int size) {
      this.bucketId = bucketId;
      this.positions = positions;
      this.size = size;
    }

    @Override
    public boolean contains(final Object o) {
      return o instanceof RID rid && rid.getBucketId() == bucketId && Arrays.binarySearch(positions, 0, size, rid.getPosition()) >= 0;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public Iterator<RID> iterator() {
      return new Iterator<>() {
        private int next;

        @Override
        public boolean hasNext() {
          return next < size;
        }

        @Override
        public RID next() {
          if (next >= size)
            throw new NoSuchElementException();
          return new RID(bucketId, positions[next++]);
        }
      };
    }
  }
}
