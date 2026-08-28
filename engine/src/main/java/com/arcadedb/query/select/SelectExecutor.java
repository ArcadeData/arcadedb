package com.arcadedb.query.select;/*
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

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.MultiIndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.utility.MultiIterator;
import com.arcadedb.utility.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Native Query engine is a simple query engine that covers most of the classic use cases, such as the retrieval of records
 * with a where condition. It could be much faster than the same SQL query because it does not use any parser and it is very
 * JIT friendly. Future versions could translate the query into bytecode to have an even faster execution.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SelectExecutor {
  final Select select;
  long            evaluatedRecords = 0;
  List<IndexInfo> usedIndexes      = null;
  // #6565: THE CANDIDATE CAP lookForIndexes() COMPUTED FOR THE WHOLE where-TREE, REUSED BY filterWithIndexesFinalNode()
  // FOR THE NESTED PER-VALUE CURSOR IT BUILDS FOR AN in_op LEAF, SO NEITHER PLACE HANDS A RAW select.limit (MISSING
  // skip) TO A MultiIndexCursor
  int             indexCandidateLimit = -1;

  // #6815: THE ABSOLUTE DEADLINE FOR THIS EXECUTION, OWNED BY THE CONSUMER INSTEAD OF BY THE SOURCE ITERATOR.
  // buildIterator() CAN RETURN FOUR DIFFERENT SOURCES AND ONLY ONE OF THEM (MultiIterator) CARRIES A TIMEOUT OF ITS
  // OWN, SO INSTALLING select.timeoutInMs THERE - THE ONLY THING THAT USED TO HAPPEN - SILENTLY DROPPED IT FOR EVERY
  // INDEX-ANSWERED PLAN (A MultiIndexCursor) AND FOR A SINGLE-BUCKET fromBuckets() (A BARE BUCKET ITERATOR), I.E. FOR
  // THE COMMONEST QUERY SHAPE OF ALL. Long.MAX_VALUE MEANS "NO TIMEOUT REQUESTED" AND KEEPS THE PER-RECORD CHECK A
  // SINGLE PERFECTLY PREDICTED COMPARE ON THE HOT PATH
  private long timeoutDeadline = Long.MAX_VALUE;

  // #6873: THE TIME SOURCE THE DEADLINE IS ARMED FROM AND CHECKED AGAINST. PRODUCTION ALWAYS USES
  // System.currentTimeMillis(); THE SEAM EXISTS SO THE REGRESSION TEST CAN DRIVE THE ELAPSED TIME INSTEAD OF RACING
  // THE WALL CLOCK. executeVector() IS EAGER - IT RETURNS A FULLY MATERIALIZED List - SO NO CONSUMER CAN PACE IT THE
  // WAY drainSlowly() PACES THE LAZY SelectIterator IN THE #6815 TESTS, AND AN "A 1 ms BUDGET EXPIRED" ASSERTION
  // WRITTEN AGAINST THE WALL CLOCK WOULD BE A RACE THAT A FASTER MACHINE TURNS RED. DRIVING THE CLOCK KEEPS THE TEST
  // DETERMINISTIC WHILE STILL EXERCISING THE REAL startTimeout() SATURATION AND THE REAL throw/truncate DECISION.
  // COSTS NOTHING ON THE HOT PATH: checkForTimeout() RETURNS ON THE Long.MAX_VALUE SENTINEL BEFORE READING IT, SO A
  // SELECT WITHOUT A TIMEOUT NEVER TOUCHES THE SUPPLIER AT ALL.
  // NOT volatile, AND IT DOES NOT NEED TO BE: EVERY CALL SITE BUILDS A FRESH SelectExecutor PER EXECUTION, THE FIELD
  // IS ONLY EVER ASSIGNED BEFORE THAT EXECUTION STARTS, AND THE ONE PLACE THAT HANDS THE EXECUTOR TO OTHER THREADS
  // (SelectParallelIterator'S PRODUCERS) READS select/evaluateWhere() AND NEVER THE DEADLINE. IF THAT EVER CHANGES,
  // THE PUBLICATION OF THIS FIELD HAS TO BE REVISITED ALONG WITH timeoutDeadline, WHICH IS IN THE SAME POSITION
  LongSupplier clock = System::currentTimeMillis;

  // #6577: THE SINGLE SOURCE OF TRUTH FOR "WHICH OPERATORS CAN filterWithIndexesFinalNode() ACTUALLY TURN INTO AN
  // IndexCursor" - isTheNodeFullyIndexed() MUST TREAT EXACTLY THIS SET, AND NO MORE, AS "THIS LEAF IS INDEXED".
  // TREATING A WIDER SET AS INDEXED IS WHAT LET #6577 THROUGH: A neq/like/ilike LEAF PASSED THE "UNDER AN or, BOTH
  // SIDES MUST BE INDEXED" GATE (BECAUSE ITS PROPERTY HAD AN INDEX) BUT THEN filterWithIndexesFinalNode()'S SWITCH
  // FELL THROUGH TO A BARE return WITH NO CURSOR - SO THE WHOLE BRANCH SILENTLY CONTRIBUTED ZERO ROWS INSTEAD OF
  // JUST RUNNING LESS EFFICIENTLY. KEEP BOTH CALL SITES READING THIS ONE FIELD SO THEY CANNOT DRIFT APART AGAIN.
  private static final Set<SelectOperator> CURSOR_BUILDABLE_OPERATORS = EnumSet.of(SelectOperator.eq, SelectOperator.in_op,
      SelectOperator.between, SelectOperator.gt, SelectOperator.ge, SelectOperator.lt, SelectOperator.le);

  static class IndexInfo {
    public final Index   index;
    public final String  property;
    public final boolean order;

    IndexInfo(final Index index, final String property, final boolean order) {
      this.index = index;
      this.property = property;
      this.order = order;
    }
  }

  public SelectExecutor(final Select select) {
    this.select = select;
  }

  /**
   * Arms the deadline for one execution. Called from the four entry points ({@link #execute()},
   * {@link #executeCount()}, {@link #executeExists()}, {@link #executeVector()}) rather than from the constructor, so
   * the window it covers is exactly the work the caller asked to bound - index lookup included, since that is part of
   * answering the query.
   */
  private void startTimeout() {
    if (select.timeoutInMs > 0) {
      // SATURATE INSTEAD OF WRAPPING: TimeUnit.toMillis() CLAMPS AN OUT-OF-RANGE CONVERSION TO Long.MAX_VALUE RATHER
      // THAN REJECTING IT, SO timeout(Long.MAX_VALUE, DAYS, ...) REACHES US AS Long.MAX_VALUE AND A PLAIN
      // now + timeoutInMs WOULD OVERFLOW INTO A NEGATIVE DEADLINE - TURNING AN EFFECTIVELY INFINITE BUDGET INTO ONE
      // THAT HAS ALREADY EXPIRED, SO THE FIRST RECORD WOULD THROW. CLAMPING TO Long.MAX_VALUE LANDS ON THE
      // "NO TIMEOUT" SENTINEL, WHICH IS EXACTLY WHAT A BUDGET THAT LARGE MEANS
      final long now = clock.getAsLong();
      timeoutDeadline = select.timeoutInMs > Long.MAX_VALUE - now ? Long.MAX_VALUE : now + select.timeoutInMs;
    }
  }

  /**
   * #6815: enforces the select deadline on the consumer side, where every source shape passes, instead of on the one
   * source that happens to know how to enforce it itself. Mirrors {@link com.arcadedb.utility.MultiIterator#checkForTimeout()}:
   * with {@code exceptionOnTimeout} the expiry is thrown, otherwise the caller is told to stop and keep what it has.
   *
   * @return {@code true} when the deadline expired and the caller asked NOT to be thrown at
   */
  boolean checkForTimeout() {
    if (timeoutDeadline == Long.MAX_VALUE || clock.getAsLong() <= timeoutDeadline)
      return false;
    if (select.exceptionOnTimeout)
      throw new TimeoutException("Timeout on iteration");
    return true;
  }

  <T extends Document> SelectIterator<T> execute() {
    startTimeout();
    final MultiIndexCursor iteratorFromIndexes = lookForIndexes();
    final Iterator<? extends Identifiable> iterator = buildIterator(iteratorFromIndexes);

    if (select.parallel)
      return new SelectParallelIterator<>(this, iterator, iteratorFromIndexes != null && iteratorFromIndexes.getCursors() > 1);

    return new SelectIterator<>(this, iterator, iteratorFromIndexes != null && iteratorFromIndexes.getCursors() > 1);
  }

  /**
   * #5662: the index cursor is released even though the scan stops at the LIMIT rather than on exhaustion. Only a
   * drained cursor releases its per-series file registrations on its own, so the early exit is exactly the case that
   * would keep a retired compacted file undroppable until the next restart.
   */
  long executeCount() {
    startTimeout();
    final MultiIndexCursor iteratorFromIndexes = lookForIndexes();
    try {
      final Iterator<? extends Identifiable> iterator = buildIterator(iteratorFromIndexes);
      final Set<RID> filterOutRecords = iteratorFromIndexes != null && iteratorFromIndexes.getCursors() > 1
          ? ConcurrentHashMap.newKeySet() : null;

      long count = 0;
      int skipped = 0;
      while (iterator.hasNext()) {
        // #6815: count() NEVER BUILDS A SelectIterator, IT DRAINS THE SOURCE IN THIS LOOP, SO THE DEADLINE HAS TO BE
        // CHECKED HERE TOO. A NON-THROWING TIMEOUT RETURNS THE PARTIAL TALLY, MATCHING THE TRUNCATED RESULT SET THE
        // STREAMING PATH RETURNS
        if (checkForTimeout())
          break;

        final Document record = iterator.next().asDocument();
        if (filterOutRecords != null && filterOutRecords.contains(record.getIdentity()))
          continue;
        if (select.rootTreeElement == null || evaluateWhere(record)) {
          if (skipped < select.skip) {
            skipped++;
            continue;
          }
          // #6579: A PRE-INCREMENT GUARD IS NEEDED FOR limit == 0 - OTHERWISE THE FIRST MATCH BUMPS count TO 1
          // BEFORE count >= limit (1 >= 0) EVER GETS THE CHANCE TO STOP THE LOOP. THIS GUARD ALONE CAN ONLY EVER
          // FIRE FOR limit == 0 THOUGH: FOR ANY limit > 0, count NEVER REACHES limit WITHOUT THE POST-INCREMENT
          // CHECK BELOW ALREADY BREAKING THE LOOP FIRST - SO KEEPING THAT SECOND CHECK IS NOT REDUNDANT, IT IS
          // WHAT PRESERVES THE SAME-ITERATION EARLY EXIT FOR limit > 0 (A REVIEW CAUGHT DROPPING IT: WITHOUT IT,
          // ONCE THE LIMIT IS REACHED THE LOOP CAN ONLY BREAK ON THE *NEXT* MATCHING RECORD - VIA THE break BEING
          // NESTED INSIDE THIS evaluateWhere() BRANCH - SO A SELECTIVE WHERE WITH NO (limit+1)TH MATCH DEGRADES A
          // BOUNDED count() INTO A FULL SCAN OF THE REST OF THE TYPE)
          if (select.limit > -1 && count >= select.limit)
            break;
          if (filterOutRecords != null)
            filterOutRecords.add(record.getIdentity());
          count++;
          if (select.limit > -1 && count >= select.limit)
            break;
        }
      }
      return count;
    } finally {
      // only the index cursor is released: the alternatives buildIterator() can return (a type or bucket scan, a
      // MultiIterator over several buckets) hold no per-series file registration and are not closeable at all
      if (iteratorFromIndexes != null)
        iteratorFromIndexes.close();
    }
  }

  /**
   * #5662: same as {@link #executeCount()}, and more acutely so - this method returns on the FIRST match, so the
   * cursor is abandoned partway on every positive answer.
   */
  boolean executeExists() {
    startTimeout();
    final MultiIndexCursor iteratorFromIndexes = lookForIndexes();
    try {
      final Iterator<? extends Identifiable> iterator = buildIterator(iteratorFromIndexes);

      while (iterator.hasNext()) {
        // #6815: SAME AS executeCount(). A NON-THROWING TIMEOUT ANSWERS false - THE SCAN NEVER FOUND A MATCH WITHIN
        // THE BUDGET IT WAS GIVEN, WHICH IS THE PARTIAL ANSWER THE CALLER ASKED TO BE GIVEN RATHER THAN AN EXCEPTION
        if (checkForTimeout())
          return false;

        final Document record = iterator.next().asDocument();
        if (select.rootTreeElement == null || evaluateWhere(record))
          return true;
      }
      return false;
    } finally {
      if (iteratorFromIndexes != null)
        iteratorFromIndexes.close();
    }
  }

  /**
   * #6873: the fifth plan shape. The four sources {@code buildIterator()} can return are all bounded by
   * {@link #checkForTimeout()} since #6815, but the vector k-NN search never goes through {@code buildIterator()} at
   * all and used to consult no deadline whatsoever - so {@code select().fromType(..).timeout(..).nearestTo(..)}
   * compiled and silently dropped the budget.
   * <p>
   * Granularity caveat: a single {@code LSMVectorIndex.findNeighborsFromVector()} call is not interruptible from here,
   * so the deadline is honoured <i>between</i> indexes, <i>before the merge sort of what they returned</i> and
   * <i>per assembled result</i> (each of which loads a record and may run the post-filter WHERE). That will not cut
   * short one long search inside a single index, but it is the difference between a no-op and a bound.
   * <p>
   * What a non-throwing expiry returns: the results <b>already assembled</b>, which is an empty list whenever the
   * deadline goes before the assembly loop starts - the neighbours the index searches accumulated are RIDs and
   * distances, not results, and promoting them would mean loading records past a deadline that has already expired.
   * That is the same rule {@link #executeCount()} and {@link SelectIterator} follow: hand back what was produced, do
   * not produce more.
   */
  @SuppressWarnings("unchecked")
  <T extends Document> List<SelectVectorResult<T>> executeVector() {
    startTimeout();

    if (select.fromType == null)
      throw new IllegalArgumentException("FromType must be set for vector search");
    if (select.vectorProperty == null)
      throw new IllegalArgumentException("Vector property must be set (call nearestTo())");

    final TypeIndex typeIndex = select.fromType.getPolymorphicIndexByProperties(select.vectorProperty);
    if (typeIndex == null)
      throw new IllegalArgumentException("No index found on property '" + select.vectorProperty + "' for type '" + select.fromType.getName() + "'");

    final IndexInternal[] bucketIndexes = typeIndex.getIndexesOnBuckets();
    if (bucketIndexes == null || bucketIndexes.length == 0)
      throw new IllegalArgumentException("Index '" + typeIndex.getName() + "' has no bucket indexes");

    final List<LSMVectorIndex> vectorIndexes = new ArrayList<>();
    for (final IndexInternal bucketIndex : bucketIndexes)
      if (bucketIndex instanceof LSMVectorIndex lsmIndex)
        vectorIndexes.add(lsmIndex);

    if (vectorIndexes.isEmpty())
      throw new IllegalArgumentException("Index '" + typeIndex.getName() + "' is not a vector index");

    final List<Pair<RID, Float>> allNeighbors = new ArrayList<>();
    for (final LSMVectorIndex lsmIndex : vectorIndexes) {
      // #6873: THE COARSEST OF THE THREE CHECKPOINTS - ONE PER BUCKET INDEX, SINCE THE SEARCH INSIDE ONE INDEX IS NOT
      // INTERRUPTIBLE FROM HERE. WHAT THE INDEXES SEARCHED SO FAR PUT IN allNeighbors IS *NOT* A PARTIAL ANSWER: IT IS
      // RIDs AND DISTANCES, AND TURNING ANY OF IT INTO A RESULT MEANS LOADING RECORDS AND RUNNING THE POST-FILTER
      // WHERE - MORE WORK, PAST A DEADLINE THAT HAS ALREADY GONE. SO A NON-THROWING EXPIRY ANYWHERE BEFORE THE
      // ASSEMBLY LOOP ANSWERS WITH AN EMPTY LIST, WHICH IS THE SAME RULE THE OTHER PATHS FOLLOW: KEEP WHAT WAS
      // ALREADY *PRODUCED* (RECORDS YIELDED BY SelectIterator, THE TALLY IN executeCount()), NEVER PRODUCE MORE
      if (checkForTimeout())
        break;

      final List<Pair<RID, Float>> neighbors;
      if (select.vectorApproximate)
        neighbors = lsmIndex.findNeighborsFromVectorApproximate(select.vectorQuery, select.vectorK);
      else
        neighbors = lsmIndex.findNeighborsFromVector(select.vectorQuery, select.vectorK);
      allNeighbors.addAll(neighbors);
    }

    // #6873: THE DEADLINE CAN ONLY EXPIRE INSIDE AN INDEX'S OWN (UNINTERRUPTIBLE) SEARCH, AND WHEN THAT IS THE *LAST*
    // INDEX THE LOOP ENDS ON ITS BOUND RATHER THAN ON THE CHECKPOINT ABOVE - SO WITHOUT A CHECKPOINT HERE THE SORT OF
    // UP TO indexes * k PAIRS AND THE RESULT-LIST ALLOCATION WOULD STILL RUN AFTER THE BUDGET IS GONE. IT ALSO MAKES
    // exceptionOnTimeout RAISE THE EXPIRY EVEN WHEN THE SEARCH FOUND NO NEIGHBOUR AT ALL, WHICH LEAVES THE ASSEMBLY
    // LOOP BELOW WITH NO ITERATION TO CHECK ON. THE EMPTY LIST IS DELIBERATE AND NOT A LOST PARTIAL RESULT - SEE THE
    // PER-INDEX CHECKPOINT ABOVE FOR WHY allNeighbors IS NOT AN ANSWER YET
    if (checkForTimeout())
      return new ArrayList<>();

    allNeighbors.sort(Comparator.comparing(Pair::getSecond));

    final int resultCount = Math.min(select.vectorK, allNeighbors.size());
    final List<SelectVectorResult<T>> results = new ArrayList<>(resultCount);

    for (int i = 0; i < resultCount; i++) {
      // #6873: ASSEMBLY IS NOT FREE - EVERY ITERATION LOADS A RECORD FROM DISK AND MAY RUN THE POST-FILTER WHERE, SO
      // THE BUDGET HAS TO BE HONOURED HERE TOO AND NOT ONLY AROUND THE NEIGHBOUR SEARCH
      if (checkForTimeout())
        break;

      final Pair<RID, Float> neighbor = allNeighbors.get(i);
      final T record = (T) neighbor.getFirst().asDocument();

      if (select.rootTreeElement != null && !evaluateWhere(record))
        continue;

      results.add(new SelectVectorResult<>(record, neighbor.getSecond()));
    }

    return results;
  }

  private Iterator<? extends Identifiable> buildIterator(final MultiIndexCursor iteratorFromIndexes) {
    final Iterator<? extends Identifiable> iterator;

    if (iteratorFromIndexes != null)
      iterator = iteratorFromIndexes;
    else if (select.fromType != null)
      iterator = select.database.iterateType(select.fromType.getName(), select.polymorphic);
    else if (select.fromBuckets.size() == 1)
      iterator = select.database.iterateBucket(select.fromBuckets.getFirst().getName());
    else {
      final MultiIterator<? extends Identifiable> multiIterator = new MultiIterator<>();
      for (Bucket b : select.fromBuckets)
        multiIterator.addIterator(b.iterator());
      iterator = multiIterator;
    }

    // #6815: KEPT AS THE FAST PATH ONLY - THE AUTHORITATIVE DEADLINE IS checkForTimeout(), WHICH EVERY SOURCE SHAPE
    // GOES THROUGH. THIS ALSO STAYS BECAUSE SelectParallelIterator READS THE MultiIterator'S OWN checkForTimeout() TO
    // DRIVE ITS PRODUCER SHUTDOWN
    if (select.timeoutInMs > 0 && iterator instanceof MultiIterator<? extends Identifiable> multiIterator)
      multiIterator.setTimeout(select.timeoutInMs, select.exceptionOnTimeout);

    return iterator;
  }

  // PACKAGE-PRIVATE (NOT private) SO Issue6565SelectIndexCandidateLimitTest CAN VERIFY THE COMPUTED CAP DIRECTLY:
  // A RESULT-COUNT ASSERTION CANNOT TELL A CORRECTLY APPLIED CAP APART FROM THE UNCAPPED FALLBACK, SINCE THE
  // LAZY-PULL CONSUMERS (SelectIterator, executeCount()) ALREADY STOP AT skip + limit ON THEIR OWN EITHER WAY
  MultiIndexCursor lookForIndexes() {
    if (select.fromType != null && select.rootTreeElement != null) {
      final List<IndexCursor> cursors = new ArrayList<>();

      // #6592: A COMPOSITE (MULTI-PROPERTY) INDEX IS REGISTERED UNDER ITS FULL PROPERTY LIST (SEE
      // LocalDocumentType.indexesByProperties), NOT UNDER ANY SUBSET OF IT - SO A PLAIN AND-CONJUNCTION OF EQUALITY
      // LEAVES THAT ONLY COVERS THE LEADING PROPERTIES OF SUCH AN INDEX CAN NEVER BE FOUND BY THE PER-LEAF,
      // SINGLE-PROPERTY LOOKUP BELOW (isTheNodeFullyIndexed()). TRY A PREFIX MATCH AGAINST EVERY COMPOSITE INDEX ON
      // THE TYPE FIRST; ONLY FALL BACK TO THE SINGLE-PROPERTY PATH WHEN NO SUCH INDEX COVERS ANY LEADING PROPERTY.
      final boolean compositeIndexUsed = matchCompositeIndex(cursors);

      if (!compositeIndexUsed) {
        // FIND AVAILABLE INDEXES AND ASSIGN node.index ON EVERY INDEXED LEAF: filterWithIndexesFinalNode() RELIES ON THAT
        // SIDE EFFECT TO KNOW WHICH LEAVES CAN BECOME A CURSOR
        isTheNodeFullyIndexed(select.rootTreeElement);

        // #6565: A CANDIDATE CAP IS SAFE ONLY WHEN THE INDEX SCAN EXACTLY REPRODUCES THE WHERE-TREE'S RESULT SET AND
        // THE ORDER BY (IF ANY) IS ALREADY SATISFIED BY IT - evaluateWhere(), skip AND fetchResultInCaseOfOrderBy()'s
        // FULL-DRAIN SORT ALL REDUCE THE STREAM FURTHER OTHERWISE, SO THE SCAN MUST RUN UNCAPPED TO SURVIVE THAT.
        // MUST RUN BEFORE filterWithIndexes() BELOW, SINCE soleExactLeaf() READS node.index BEFORE
        // filterWithIndexesFinalNode() PRUNES AN 'and' SIBLING'S - HARMLESS TODAY ONLY BECAUSE 'and' IS ALREADY
        // UNCONDITIONALLY DISQUALIFIED
        final SelectTreeNode exactLeaf = soleExactLeaf(select.rootTreeElement);
        indexCandidateLimit = exactLeaf != null && isOrderBySafeForCap(exactLeaf) ? computeExactCandidateLimit() : -1;

        filterWithIndexes(select.rootTreeElement, cursors);
      }

      if (!cursors.isEmpty())
        return new MultiIndexCursor(cursors, indexCandidateLimit, true);

      // NO CURSOR WAS ACTUALLY BUILT (E.G. A BARE is_null/is_not_null/neq/like/ilike LEAF, WHICH isTheNodeFullyIndexed()
      // CORRECTLY REFUSES TO TREAT AS INDEXED - SEE #6577 - SO node.index STAYS null AND filterWithIndexesFinalNode()
      // NEVER RUNS), SO NO CAP WAS EVER APPLIED EITHER: RESET THE TEST-VISIBLE FIELD RATHER THAN LEAVE IT HOLDING A
      // MISLEADING FINITE VALUE
      indexCandidateLimit = -1;
    }
    return null;
  }

  /**
   * Prefix-matches the where-tree against every composite index on {@link Select#fromType}: a plain AND-conjunction
   * of equality leaves (no {@code or}/{@code not} anywhere - see {@link #isPureAndConjunction}) covering the leading
   * N properties of an index registered on M &gt;= N properties can be answered with a single partial-key cursor,
   * exactly like the SQL executor's {@code IndexSearchDescriptor} already does for plain {@code SELECT} statements
   * (see {@code SelectExecutionPlanner.buildIndexSearchDescriptor}). Single-property indexes are left to the existing
   * {@link #isTheNodeFullyIndexed}/{@link #filterWithIndexes} path, which already handles them (including the
   * {@code or} and mixed-operator cases this method deliberately does not attempt).
   * <p>
   * When the trailing (N+1-th) index property is also the query's sole {@code ORDER BY} column, the cursor is built
   * as a range scan in the requested direction instead of a plain equality lookup, so the index itself already
   * returns the rows in the requested order - mirroring the SQL executor's {@code fullySorted} elision and letting
   * {@link SelectIterator#fetchResultInCaseOfOrderBy} skip its full materialize-and-sort fallback.
   *
   * @return {@code true} when a composite-index cursor was built and appended to {@code cursors}
   */
  private boolean matchCompositeIndex(final List<IndexCursor> cursors) {
    // CHEAP UPFRONT GUARD: A TYPE WITH ONLY SINGLE-PROPERTY INDEXES (THE COMMON CASE) CAN NEVER PRODUCE A COMPOSITE
    // MATCH, SO SKIP THE WHERE-TREE WALK (isPureAndConjunction()/collectAndEqLeaves()) AND THE PER-QUERY ALLOCATION
    // AND SORT BELOW ENTIRELY, LEAVING ONLY THIS ONE getAllIndexes(true) ITERATION (ALREADY NEEDED TO TELL WHETHER
    // ANY COMPOSITE INDEX EXISTS AT ALL) BEFORE FALLING THROUGH TO isTheNodeFullyIndexed()/filterWithIndexes()
    // ONLY AN INDEX THAT supportsOrderedIterations() CAN SERVE A PARTIAL-PREFIX MATCH: THAT BRANCH BUILDS A cursor
    // VIA range(), WHICH THROWS UnsupportedOperationException OTHERWISE (SEE TypeIndex.range()) - TRUE ONLY FOR
    // LSM_TREE, NOT FOR HASH/UNIQUE_HASH (A COMMON SHAPE FOR EDGE-UNIQUENESS INDEXES ON (@out, @in), E.G.
    // Issue5677HashIndexLinkKeyTest), FULL_TEXT, GEOSPATIAL, LSM_VECTOR OR LSM_SPARSE_VECTOR. EXCLUDING THOSE HERE,
    // RATHER THAN ONLY GUARDING THE range() CALL BELOW, ALSO AVOIDS get() DOING A RAW EQUALITY LOOKUP ON A
    // FULL-KEY-MATCHED FULL_TEXT INDEX INSTEAD OF A TOKENIZED SEARCH - A TYPE WITH ONLY SUCH INDEXES SIMPLY FALLS
    // BACK TO THE PRE-EXISTING isTheNodeFullyIndexed()/filterWithIndexes() PATH, EXACTLY AS BEFORE THIS METHOD EXISTED
    final List<TypeIndex> candidates = new ArrayList<>();
    for (final TypeIndex candidate : select.fromType.getAllIndexes(true))
      if (candidate.getPropertyNames().size() >= 2 && candidate.supportsOrderedIterations())
        candidates.add(candidate);

    if (candidates.isEmpty())
      return false;

    if (!isPureAndConjunction(select.rootTreeElement))
      return false;

    final Map<String, SelectTreeNode> andEqLeaves = new LinkedHashMap<>();
    collectAndEqLeaves(select.rootTreeElement, andEqLeaves);
    if (andEqLeaves.isEmpty())
      return false;

    // getAllIndexes(true) IS BACKED BY A HashSet FOR A POLYMORPHIC TYPE (LocalDocumentType.getAllIndexes()), SO ITS
    // ITERATION ORDER IS UNSPECIFIED - SORT BY NAME FIRST SO TWO INDEXES TIED ON PREFIX LENGTH AND UNIQUENESS (E.G.
    // TWO COMPOSITE INDEXES SHARING THE SAME LEADING PROPERTIES BUT DIFFERENT TRAILING ONES) ARE PICKED BETWEEN
    // DETERMINISTICALLY ACROSS RUNS, RATHER THAN LEAVING WHICH ONE'S ORDER BY GETS ELIDED TO HASH-BUCKET LUCK
    if (candidates.size() > 1)
      candidates.sort(Comparator.comparing(TypeIndex::getName));

    TypeIndex bestIndex = null;
    int bestPrefixLength = 0;

    for (final TypeIndex candidate : candidates) {
      final List<String> properties = candidate.getPropertyNames();

      int prefixLength = 0;
      while (prefixLength < properties.size() && andEqLeaves.containsKey(properties.get(prefixLength)))
        prefixLength++;

      if (prefixLength == 0)
        continue;

      // bestIndex IS GUARANTEED NON-null HERE WHEN prefixLength == bestPrefixLength: THE FIRST CANDIDATE TO EVER
      // QUALIFY (prefixLength > 0) ALWAYS TAKES THE prefixLength > bestPrefixLength BRANCH INSTEAD, SINCE
      // bestPrefixLength STARTS AT 0 AND ONLY A QUALIFYING CANDIDATE (prefixLength > 0) REACHES THIS LINE
      if (prefixLength > bestPrefixLength || (prefixLength == bestPrefixLength && !bestIndex.isUnique() && candidate.isUnique())) {
        bestIndex = candidate;
        bestPrefixLength = prefixLength;
      }
    }

    if (bestIndex == null)
      return false;

    final List<String> indexProperties = bestIndex.getPropertyNames();
    final Object[] keys = new Object[bestPrefixLength];
    for (int i = 0; i < bestPrefixLength; i++) {
      final SelectTreeNode leaf = andEqLeaves.get(indexProperties.get(i));
      keys[i] = leaf.right instanceof SelectParameterValue value ? value.eval(null) : leaf.right;
    }

    final String trailingProperty = bestPrefixLength < indexProperties.size() ? indexProperties.get(bestPrefixLength) : null;
    // trailingProperty == null IFF bestPrefixLength == indexProperties.size() - I.E. EVERY PROPERTY OF THE INDEX IS
    // BOUND BY EQUALITY, NOT JUST A LEADING SUBSET OF THEM
    final boolean fullKeyMatch = trailingProperty == null;

    if (!fullKeyMatch) {
      // A PARTIAL-PREFIX MATCH SCANS EVERY ROW SHARING THE MATCHED PREFIX AND LEAVES ANY EQ-BOUND PROPERTY OUTSIDE
      // THAT PREFIX TO evaluateWhere() - IF ONE OF THOSE PROPERTIES ALREADY HAS ITS OWN STANDALONE *UNIQUE* INDEX (A
      // REAL SHAPE: A NON-UNIQUE COMPOSITE INDEX SERVING ONE QUERY PATTERN COEXISTING WITH A UNIQUE SINGLE-PROPERTY
      // INDEX ENFORCING A CONSTRAINT ON ANOTHER COLUMN - LocalDocumentType.indexesByProperties HAPPILY LETS BOTH
      // COEXIST), THAT STANDALONE INDEX IS GUARANTEED TO ANSWER THE QUERY MORE PRECISELY (AT MOST ONE ROW) THAN
      // SCANNING THE WHOLE PREFIX RANGE, SO DEFER TO THE PRE-EXISTING isTheNodeFullyIndexed()/filterWithIndexes()
      // PATH IN THAT CASE. A NON-UNIQUE STANDALONE INDEX GIVES NO SUCH GUARANTEE - IT COULD BE LESS SELECTIVE THAN
      // THE COMPOSITE PREFIX'S OWN COMBINED LEADING PROPERTIES (E.G. A LOW-CARDINALITY FLAG), AND THIS METHOD HAS NO
      // COST-BASED WAY TO COMPARE THE TWO - SO IT IS DELIBERATELY NOT ENOUGH TO TRIGGER DEFERRAL ON ITS OWN. A
      // PROPERTY ALREADY COVERED BY THE MATCHED PREFIX ITSELF IS EXEMPT EITHER WAY: THE PREFIX SCAN IS AT LEAST AS
      // SELECTIVE FOR THAT PROPERTY AS A STANDALONE INDEX ON IT ALONE WOULD BE.
      final Set<String> matchedPrefixProperties = new HashSet<>(indexProperties.subList(0, bestPrefixLength));
      for (final String property : andEqLeaves.keySet()) {
        if (matchedPrefixProperties.contains(property))
          continue;
        final TypeIndex standaloneIndex = select.fromType.getPolymorphicIndexByProperties(property);
        if (standaloneIndex != null && standaloneIndex.isUnique())
          return false;
      }
    }

    final boolean orderByElided = select.orderBy != null && select.orderBy.size() == 1 && trailingProperty != null
        && select.orderBy.getFirst().getFirst().equals(trailingProperty);

    // A KEY SHORTER THAN THE INDEX'S FULL ARITY IS A PREFIX, NOT AN EXACT KEY: get() PERFORMS A SINGLE POSITIONAL
    // LOOKUP AND ONLY RETURNS EVERY MATCH WHEN THE KEY'S ARITY MATCHES THE INDEX'S OWN EXACTLY, SO A PREFIX MUST GO
    // THROUGH range() WITH EQUAL (INCLUSIVE) BEGIN/END BOUNDS INSTEAD - SEE LSMTreeIndexCompacted's "PARTIAL KEY
    // COMPARISON...MATCHES BY PREFIX" (PURPOSE=2).
    final boolean ascendingOrder = orderByElided ? select.orderBy.getFirst().getSecond() : true;
    final IndexCursor cursor = fullKeyMatch ? bestIndex.get(keys) : bestIndex.range(ascendingOrder, keys, true, keys, true);

    if (cursor == null)
      return false;

    if (usedIndexes == null)
      usedIndexes = new ArrayList<>();
    // fetchResultInCaseOfOrderBy()'S TRIVIAL-MATCH CHECK KEYS OFF usedIndexes.getFirst().property/order: RECORD THE
    // TRAILING PROPERTY (NOT THE LAST *MATCHED* ONE) WHEN THE ORDER BY WAS ELIDED, SO IT RECOGNIZES THE RANGE SCAN AS
    // ALREADY SATISFYING THE ORDER BY AND SKIPS THE FULL MATERIALIZE-AND-SORT FALLBACK.
    usedIndexes.add(new IndexInfo(bestIndex, orderByElided ? trailingProperty : indexProperties.get(bestPrefixLength - 1), ascendingOrder));
    cursors.add(cursor);

    // THE CANDIDATE CAP (SEE #6565's computeExactCandidateLimit()) IS SAFE ONLY WHEN THIS CURSOR EXACTLY REPRODUCES
    // THE WHOLE WHERE-TREE'S RESULT SET (EVERY LEAF WAS CONSUMED BY THE MATCHED PREFIX, NOTHING LEFT FOR
    // evaluateWhere() TO DISCARD) AND THE ORDER BY, IF ANY, IS ALREADY SATISFIED BY THE CURSOR ITSELF - OTHERWISE
    // fetchResultInCaseOfOrderBy()'S FULL-DRAIN SORT NEEDS EVERY MATCH, NOT JUST skip + limit OF THEM.
    final boolean exactMatch = bestPrefixLength == countAndLeaves(select.rootTreeElement);
    indexCandidateLimit = exactMatch && (select.orderBy == null || orderByElided) ? computeExactCandidateLimit() : -1;

    return true;
  }

  /**
   * True when {@code node} and everything under it is connected purely by {@code and} (through the synthetic
   * {@code run} wrapper {@link Select#compile()} adds around a single bare leaf - see {@link #soleExactLeaf}) with
   * no {@code or}/{@code not} anywhere. Only such a tree can be safely resolved through one composite-index prefix
   * match: an {@code or}/{@code not} would need the same per-branch reasoning {@link #isTheNodeFullyIndexed} already
   * does for the single-property case, which this method deliberately leaves untouched.
   */
  private boolean isPureAndConjunction(final SelectTreeNode node) {
    if (!(node.left instanceof SelectTreeNode))
      return true;
    if (node.operator == SelectOperator.run)
      return isPureAndConjunction((SelectTreeNode) node.left);
    if (node.operator != SelectOperator.and)
      return false;
    return isPureAndConjunction((SelectTreeNode) node.left) && (node.right == null || isPureAndConjunction((SelectTreeNode) node.right));
  }

  /**
   * Collects every {@code eq} leaf of a {@link #isPureAndConjunction} tree, keyed by property name. A leaf using any
   * other operator (a range, an {@code in_op}, ...) is simply left out of the map: a composite-index prefix match
   * only needs the leading properties bound by equality, and every leaf stays in the tree regardless for the final
   * {@link #evaluateWhere} pass.
   */
  private void collectAndEqLeaves(final SelectTreeNode node, final Map<String, SelectTreeNode> target) {
    if (!(node.left instanceof SelectTreeNode)) {
      if (node.operator == SelectOperator.eq && node.left instanceof SelectPropertyValue leftProperty
          && !(node.right instanceof SelectPropertyValue))
        target.putIfAbsent(leftProperty.propertyName, node);
      return;
    }
    collectAndEqLeaves((SelectTreeNode) node.left, target);
    if (node.right != null)
      collectAndEqLeaves((SelectTreeNode) node.right, target);
  }

  /**
   * Counts every leaf (regardless of operator) of a {@link #isPureAndConjunction} tree - used by
   * {@link #matchCompositeIndex} to tell whether the matched prefix consumed the whole where-tree.
   */
  private int countAndLeaves(final SelectTreeNode node) {
    if (!(node.left instanceof SelectTreeNode))
      return 1;
    int count = countAndLeaves((SelectTreeNode) node.left);
    if (node.right != null)
      count += countAndLeaves((SelectTreeNode) node.right);
    return count;
  }

  /**
   * The candidate cap for an exactly-indexed where-tree: {@code skip + limit} records are needed downstream (skip
   * consumed first, then limit), not just {@code limit}. On overflow this deliberately falls back to {@code -1}
   * (uncapped) rather than clamping to {@code Integer.MAX_VALUE}: a {@code skip}/{@code limit} pair that overflows
   * an {@code int} is already pathological, and {@code -1} is the same "run uncapped" value used everywhere else
   * in this class when the cap can't be trusted, instead of introducing a second sentinel with the same meaning.
   */
  private int computeExactCandidateLimit() {
    if (select.limit < 0)
      return -1;
    final long sum = (long) Math.max(0, select.skip) + select.limit;
    return sum > Integer.MAX_VALUE ? -1 : (int) sum;
  }

  /**
   * The tree's one and only indexed leaf whose index cursor, if built, would return exactly the records matching
   * the whole tree, with nothing left for {@code evaluateWhere()} to discard afterward - {@code null} if no such
   * leaf exists. Only a bare leaf qualifies (through the synthetic {@code run} wrapper, see below): an
   * {@code is_null}/{@code is_not_null} leaf never gets a cursor, and under an {@code and} at most one child's
   * cursor is kept when either side's index is unique (see {@link #filterWithIndexesFinalNode}) - when neither
   * side is unique, both children's cursors survive and {@link MultiIndexCursor} unions rather than intersects
   * them, a superset {@code evaluateWhere()} must still narrow down. Either way the discarded or extra conjunct is
   * still checked later against a stream a cap would have already truncated or under-counted. {@code not} is
   * conservatively treated the same way.
   * <p>
   * {@code or} is conservatively excluded too, even though {@link MultiIndexCursor} merges its children's cursors
   * into the same shape as the boolean union: that merge is a plain k-way merge with no RID dedup, so two branches
   * whose match sets overlap (same-property ranges that overlap, or two different properties a single record can
   * both satisfy - not provable disjoint from the where-tree's shape alone) make the same record surface as two
   * separate candidates, each burning one unit of the cap before {@link SelectIterator}/{@code executeCount()}'s
   * downstream {@code filterOutRecords} dedup ever sees it. That can exhaust the cap on duplicates before enough
   * distinct matches are found - the same "cap spent before the filtering it needs to survive" defect this class
   * exists to fix, just reachable again through the "or is exact" path. An {@code in_op} leaf's own internal
   * per-value cursors don't have this problem <i>for the indexes this method can actually resolve</i>: each value is
   * a distinct key on a single-valued property, so their RID sets are disjoint by construction - but that
   * assumption only holds because a {@code BY ITEM}/{@code BY KEY}/{@code BY VALUE} index (one document can
   * contribute multiple entries for the same list/map property, breaking disjointness the same way {@code or} does)
   * is registered under a property-name key carrying that literal suffix, which the fluent {@code Select} API this
   * class serves has no way to produce - see #6578 if that ever changes. That dedup-free merge happens once, inside
   * {@link #filterWithIndexesFinalNode}'s own {@code in_op} handling, not through a top-level {@code or}, so
   * {@code in_op} is unaffected by excluding {@code or} here. An {@code or} also always adds one {@link IndexInfo}
   * per leaf to {@link #usedIndexes}, so {@code usedIndexes.size() == 1} - the trivial-match precondition
   * {@link SelectIterator#fetchResultInCaseOfOrderBy} itself requires - can never hold for it either way.
   * <p>
   * {@code Select.compile()} always finalizes the tree with a trailing {@code setLogic(SelectOperator.run)}
   * ({@link Select#compile}); when the where-clause is a single bare condition with no {@code and()}/{@code or()},
   * that call runs its "1ST TIME ONLY" branch and wraps the leaf in a synthetic {@code run} node whose {@code right}
   * is always {@code null} ({@link Select#setLogic}) - {@code run} is otherwise never used as a tree operator, so it
   * is treated here as a transparent pass-through to its {@code left} child.
   * <p>
   * Callers must invoke this before {@link #filterWithIndexes}, which prunes a losing {@code and} sibling's
   * {@code node.index} as a side effect of building the winning side's cursor - harmless today only because
   * {@code and} is unconditionally disqualified above regardless of {@code node.index}. The moment that
   * disqualification is ever loosened, this ordering becomes load-bearing: calling this method after that pruning
   * has run could read a {@code node.index} the pruning already cleared.
   */
  private SelectTreeNode soleExactLeaf(final SelectTreeNode node) {
    if (node == null)
      return null;

    // node.index != null MEANS "isTheNodeFullyIndexed() FOUND AN INDEX ON THIS LEAF'S PROPERTY AND ITS OPERATOR IS
    // CURSOR-BUILDABLE" (SEE CURSOR_BUILDABLE_OPERATORS, FIXED BY #6577) - SO A BARE neq/like/ilike LEAF NO LONGER
    // REACHES HERE WITH node.index SET, AND THIS METHOD DOESN'T NEED TO KNOW ABOUT THAT DISTINCTION ITSELF.
    if (!(node.left instanceof SelectTreeNode))
      return node.index != null ? node : null;

    if (node.operator == SelectOperator.run)
      return soleExactLeaf((SelectTreeNode) node.left);

    return null;
  }

  /**
   * True when there is no {@code orderBy} at all, or when the single {@code leaf}'s ascending index scan trivially
   * satisfies it - mirroring {@link SelectIterator#fetchResultInCaseOfOrderBy}'s own trivial-match check
   * ({@code usedIndexes.size() == 1} and matching property/direction). When it does NOT trivially match, that
   * method drains the iterator fully and sorts in memory, so the candidate cap must stay off: capping at
   * {@code skip + limit} would let the sort see only the first few candidates in ascending scan order instead of
   * every match.
   */
  private boolean isOrderBySafeForCap(final SelectTreeNode leaf) {
    if (select.orderBy == null)
      return true;
    if (leaf == null || select.orderBy.size() != 1)
      return false;
    final Pair<String, Boolean> orderBy = select.orderBy.getFirst();
    // UNCHECKED CAST IS SAFE: leaf CAME FROM soleExactLeaf(), WHICH ONLY EVER RETURNS A NODE WHOSE node.index IS
    // NON-null - AND isTheNodeFullyIndexed() ONLY EVER SETS node.index AFTER THIS EXACT SAME CAST ON node.left
    // ALREADY SUCCEEDED
    return orderBy.getSecond() && orderBy.getFirst().equals(((SelectPropertyValue) leaf.left).propertyName);
  }

  private void filterWithIndexes(final SelectTreeNode node, final List<IndexCursor> cursors) {
    if (!(node.left instanceof SelectTreeNode))
      filterWithIndexesFinalNode(node, cursors);
    else {
      filterWithIndexes((SelectTreeNode) node.left, cursors);
      if (node.right != null)
        filterWithIndexes((SelectTreeNode) node.right, cursors);
    }
  }

  private void filterWithIndexesFinalNode(final SelectTreeNode node, final List<IndexCursor> cursors) {
    if (node.index == null)
      return;

    // #6577: node.index != null ONLY MEANS THE PROPERTY HAS AN INDEX, NOT THAT THIS OPERATOR CAN BE TURNED INTO A
    // CURSOR - isTheNodeFullyIndexed() NOW GUARDS AGAINST THIS TOO (SEE CURSOR_BUILDABLE_OPERATORS), BUT THIS CHECK
    // STAYS AS A CHEAP DEFENSIVE MIRROR OF THE SWITCH BELOW IN CASE THAT INVARIANT IS EVER LOOSENED ELSEWHERE.
    if (!CURSOR_BUILDABLE_OPERATORS.contains(node.operator))
      return;

    if (node.getParent().operator == SelectOperator.not)
      // #6575: A LEAF DIRECTLY UNDER not WOULD OTHERWISE BUILD A "POSITIVE" CURSOR FOR THE UN-NEGATED CONDITION
      // (E.G. AN eq CURSOR FOR NOT a = 'x' YIELDS THE RECORDS WHERE a = 'x' IS TRUE) - evaluateWhere() THEN
      // REJECTS EVERY ONE OF THOSE CANDIDATES SINCE THEY ALL SATISFY THE POSITIVE CONDITION BY CONSTRUCTION, SO
      // THE QUERY WOULD SILENTLY RETURN ZERO ROWS INSTEAD OF "EVERY RECORD WHERE a != 'x'". isTheNodeFullyIndexed()
      // STILL SETS node.index HERE (SEE ITS not BRANCH), SO REFUSE THE CURSOR HERE INSTEAD, THE SAME WAY
      // is_null/is_not_null LEAVES ARE ALREADY EXCLUDED THERE. NEITHER A FLUENT .not() NOR Select.json() REACH THIS
      // TODAY, BUT THE DEFENSIVE POSTURE MUST HOLD THE MOMENT EITHER PATH OPENS UP.
      return;

    if (node.getParent().operator == SelectOperator.or) {
      // UNDER AN 'OR' OPERATOR: BOTH SIDES MUST BE INDEXED, OTHERWISE CANNOT USE INDEXES
      if (node != node.getParent().right) {
        if (!isTheNodeFullyIndexed((SelectTreeNode) node.getParent().right))
          return;
      } else {
        if (node.getParent().left instanceof SelectTreeNode leftNode && !isTheNodeFullyIndexed(leftNode))
          return;
      }
    }

    final Object rightValue;
    if (node.right instanceof SelectParameterValue value)
      rightValue = value.eval(null);
    else
      rightValue = node.right;

    final String propertyName = ((SelectPropertyValue) node.left).propertyName;

    // ALWAYS SCAN THE INDEX IN ASCENDING ORDER TO SELECT THE MATCHING ROWS. THE SCAN DIRECTION IS DELIBERATELY DECOUPLED FROM
    // THE ORDER BY DIRECTION: A DESCENDING ORDER BY IS SERVED BY THE IN-MEMORY SORT IN SelectIterator (SEE ISSUE #5079). A
    // DESCENDING INDEX SCAN WITH AN OPEN (null) BOUND IS NOT A SUPPORTED CURSOR SHAPE HERE AND USED TO RETURN AN EMPTY RESULT.
    final boolean ascendingOrder = true;

    final IndexCursor cursor;
    if (node.operator == SelectOperator.eq)
      cursor = node.index.get(new Object[] { rightValue });
    else if (node.operator == SelectOperator.in_op) {
      // IN: multi-point index lookup
      if (rightValue instanceof Collection<?> collection) {
        final List<IndexCursor> inCursors = new ArrayList<>();
        for (final Object item : collection)
          inCursors.add(node.index.get(new Object[] { item }));
        // DELIBERATE, NOT JUST CONVENIENT REUSE: THE OUTER MultiIndexCursor NEVER PULLS MORE THAN indexCandidateLimit
        // CANDIDATES ACROSS ALL ITS CHILDREN COMBINED, SO NO SINGLE VALUE'S NESTED CURSOR CAN LEGITIMATELY NEED MORE
        cursor = inCursors.isEmpty() ? null : new MultiIndexCursor(inCursors, indexCandidateLimit, ascendingOrder);
      } else
        cursor = node.index.get(new Object[] { rightValue });
    } else if (node.operator == SelectOperator.between) {
      // BETWEEN: range scan
      if (rightValue instanceof Object[] range && range.length == 2)
        cursor = node.index.range(ascendingOrder, new Object[] { range[0] }, true, new Object[] { range[1] }, true);
      else
        return;
    } else if (node.operator == SelectOperator.gt)
      cursor = node.index.range(ascendingOrder, new Object[] { rightValue }, false, null, false);
    else if (node.operator == SelectOperator.ge)
      cursor = node.index.range(ascendingOrder, new Object[] { rightValue }, true, null, false);
    else if (node.operator == SelectOperator.lt)
      cursor = node.index.range(ascendingOrder, null, false, new Object[] { rightValue }, false);
    else if (node.operator == SelectOperator.le)
      cursor = node.index.range(ascendingOrder, null, false, new Object[] { rightValue }, true);
    else
      return;

    if (cursor == null)
      return;

    final SelectTreeNode parentNode = node.getParent();
    if (parentNode.operator == SelectOperator.and && parentNode.left == node) {
      if (!node.index.isUnique()) {
        // CHECK IF THERE IS ANOTHER INDEXED NODE ON THE SIBLING THAT IS UNIQUE (TO PREFER TO THIS)
        final TypeIndex rightIndex = ((SelectTreeNode) parentNode.right).index;
        if (rightIndex != null && rightIndex.isUnique()) {
          // DO NOT USE THIS INDEX (NOT UNIQUE), NOT WORTH IT
          node.index = null;
          return;
        }
      } else {
        // REMOVE THE INDEX ON THE SIBLING NODE
        // TODO CALCULATE WHICH ONE IS FASTER AND REMOVE THE SLOWER ONE
        ((SelectTreeNode) parentNode.right).index = null;
      }
    }

    if (usedIndexes == null)
      usedIndexes = new ArrayList<>();

    usedIndexes.add(new IndexInfo(node.index, propertyName, ascendingOrder));
    cursors.add(cursor);
  }

  /**
   * Considers a fully indexed node when both properties are indexed or only one with an AND operator.
   */
  private boolean isTheNodeFullyIndexed(final SelectTreeNode node) {
    if (node == null)
      return true;

    if (!(node.left instanceof SelectTreeNode)) {
      // #6577: A LEAF IS "INDEXED" ONLY WHEN filterWithIndexesFinalNode()'S SWITCH CAN ACTUALLY BUILD A CURSOR FOR
      // ITS OPERATOR - is_null/is_not_null WERE THE ONLY EXCLUSION HERE BEFORE, BUT neq/like/ilike NEVER PRODUCE A
      // CURSOR EITHER (SEE CURSOR_BUILDABLE_OPERATORS). A neq/like/ilike LEAF MUST FALL BACK TO A FULL SCAN, SAME AS
      // is_null/is_not_null, RATHER THAN LET AN or SIBLING TREAT IT AS INDEXED AND SILENTLY DROP ITS MATCHES.
      if (!CURSOR_BUILDABLE_OPERATORS.contains(node.operator))
        return false;

      if (!(node.right instanceof SelectPropertyValue)) {
        final TypeIndex propertyIndex = select.fromType.getPolymorphicIndexByProperties(
            ((SelectPropertyValue) node.left).propertyName);

        if (propertyIndex != null)
          node.index = propertyIndex;

        return propertyIndex != null;
      }
    } else {
      final boolean leftIsIndexed = isTheNodeFullyIndexed((SelectTreeNode) node.left);
      final boolean rightIsIndexed = isTheNodeFullyIndexed((SelectTreeNode) node.right);

      if (node.operator.equals(SelectOperator.and))
        // AND: ONE OR BOTH MEANS INDEXED
        return leftIsIndexed || rightIsIndexed;
      else if (node.operator.equals(SelectOperator.or))
        return leftIsIndexed || rightIsIndexed;
      else if (node.operator.equals(SelectOperator.not))
        return leftIsIndexed;
    }
    return false;
  }

  public static Object evaluateValue(final Document record, final Object value) {
    if (value == null)
      return null;
    else if (value instanceof SelectTreeNode node)
      return node.eval(record);
    else if (value instanceof SelectRuntimeValue runtimeValue)
      return runtimeValue.eval(record);
    return value;
  }

  public Map<String, Object> metrics() {
    return Map.of("evaluatedRecords", evaluatedRecords, "usedIndexes", usedIndexes != null ? usedIndexes.size() : 0);
  }

  boolean evaluateWhere(final Document record) {
    ++evaluatedRecords;
    final Object result = select.rootTreeElement.eval(record);
    if (result instanceof Boolean boolean1)
      return boolean1;
    throw new IllegalArgumentException("A boolean result was expected but '" + result + "' was returned");
  }
}
