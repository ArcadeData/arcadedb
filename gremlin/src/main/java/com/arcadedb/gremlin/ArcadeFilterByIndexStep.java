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
package com.arcadedb.gremlin;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.IntegralKeyBound;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.SelectExecutionPlanner;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.LongHashSet;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.logging.Level;

/**
 * Candidate generator for {@code g.V().hasLabel(X).has(key, predicate)} backed by one index. It is a pure candidate
 * generator: the {@code has()} containers stay in the {@code HasStep} that follows, which re-checks every emitted
 * element, so the step only has to emit a SUPERSET of the matches that belongs to the label.
 * <ul>
 *   <li>The label: the index may belong to a super type of {@code X} (it is found walking up the hierarchy) and then
 *   spans the super type and every sibling sub-type. Candidates outside {@code X}'s polymorphic buckets are dropped
 *   by bucket id, before their record is loaded (#8249).</li>
 *   <li>Timing: the cursor is opened when the step executes, once per incoming traverser, never when the traversal
 *   is compiled. A write made by an earlier step of the same traversal is therefore visible, and a branch that never
 *   runs never pays for a scan (#8299).</li>
 *   <li>Memory: the cursor is streamed, never drained into a set, so {@code limit(n)} stops the scan after
 *   {@code n} candidates.</li>
 *   <li>Concurrent DDL: the index the traversal was compiled against can be dropped or rebuilt before, or while, the step
 *   reads it (#9651). Each scan starts by checking it is still usable, and takes the index that now covers the same property
 *   when it is not, or scans the type when none does: the {@code HasStep} after the step re-checks every candidate, so a scan
 *   of the type is always a correct superset. When the index goes away in the middle of a scan, the scan is finished on the
 *   type, skipping the candidates already emitted, so a count stays exact rather than coming back short.</li>
 * </ul>
 * Like every TinkerPop step, an instance runs on one thread at a time: the index it re-resolves and the bucket filter it caches
 * are plain fields, so a compiled traversal must not be iterated from two threads at once.
 */
public class ArcadeFilterByIndexStep<S, E extends Element> extends AbstractStep<S, E> implements AutoCloseable, Configuring {
  /**
   * The candidates of one scan that are remembered, so the scan can be finished on the type when its index goes away in the
   * middle of it without emitting one twice. Beyond this many the scan stops remembering, and an index lost after that point
   * fails the traversal with a retryable {@link IndexException} rather than answering short.
   */
  static final    int                                  MAX_TRACKED_CANDIDATES = 1 << 16;
  private         TypeIndex                            index;
  private final   String[]                             indexProperties;
  private final   BiPredicate<?, ?>                    predicate;
  private final   Object[]                             keys;
  private final   String                               typeName;
  protected       Parameters                           parameters = new Parameters();
  protected final Class<E>                             returnClass;
  protected       boolean                              isStart;
  protected       boolean                              done       = false;
  private         Traverser.Admin<S>                   head       = null;
  private         Iterator<E>                          iterator   = EmptyIterator.instance();
  // COMPUTED ON THE FIRST EXECUTION AND KEPT: BUCKET MEMBERSHIP DEPENDS ON THE SCHEMA ONLY, NOT ON THE DATA, SO A STEP
  // RE-ENTERED PER TRAVERSER (local(), repeat(), A MID-TRAVERSAL V()) DOES NOT REBUILD IT EVERY TIME
  private         boolean[]                            allowedBuckets;
  private         boolean                              allowedBucketsResolved;

  /**
   * @param index     the index to scan; it may belong to {@code typeName} or to one of its super types
   * @param predicate one of {@link Compare#eq}, {@link Compare#gt}, {@link Compare#gte}, {@link Compare#lt} and
   *                  {@link Compare#lte}
   * @param keys      the index key the predicate compares against
   * @param typeName  the type named by {@code hasLabel()}: only its elements, or its sub-types', are emitted
   * @param indexProperties the properties the index covers, as the strategy matched them: not read from the index, which a
   *                        concurrent DDL can have dropped by now (#9651)
   */
  public ArcadeFilterByIndexStep(final Traversal.Admin traversal, final Class returnClass, final boolean isStart, final TypeIndex index,
      final BiPredicate<?, ?> predicate, final Object[] keys, final String typeName, final String... indexProperties) {
    super(traversal);
    if (!isSupported(predicate))
      throw new IllegalArgumentException("Unsupported index predicate '" + predicate + "'");
    this.index = index;
    this.indexProperties = indexProperties;
    this.predicate = predicate;
    this.keys = keys;
    this.typeName = typeName;
    this.returnClass = returnClass;
    this.isStart = isStart;
  }

  /** Whether the predicate can be answered by an index cursor. */
  public static boolean isSupported(final BiPredicate<?, ?> predicate) {
    return predicate == Compare.eq || predicate == Compare.gt || predicate == Compare.gte || predicate == Compare.lt
        || predicate == Compare.lte;
  }

  /**
   * Whether the index can generate the candidates of the predicate: it is usable by a query now (not dropped, rebuilt or still
   * being built) and answers by exact key, and for a range it iterates in key order over the keys as written. Asked by the
   * strategy when it picks the index, and by the step again when it opens a scan. An index a concurrent DDL takes away while
   * this is asked cannot (#9651).
   */
  public static boolean canAnswer(final TypeIndex index, final BiPredicate<?, ?> predicate) {
    try {
      if (index == null || index.getPropertyNamesIfExactKeyLookup() == null)
        return false; // A FULL_TEXT INDEX ANSWERS BY TOKEN AND MISSES A VALUE WITH NONE (#8439)
      if (predicate == Compare.eq)
        return true;
      // A HASH INDEX ANSWERS EQUALITY ONLY (#9144); A COLLATE ci INDEX HOLDS LOWER-CASED KEYS, ITS RANGE LOSES ROWS (#9302)
      return index.supportsOrderedIterations() && !SelectExecutionPlanner.holdsFoldedKeys(index);
    } catch (final IndexException e) {
      return false;
    }
  }

  /**
   * The index this scan reads: the one the traversal was compiled against while it can still answer, otherwise the one that
   * now covers the same properties on the type (a {@code DROP INDEX} followed by a {@code CREATE INDEX} leaves a new index
   * under the same name), otherwise null and the type is scanned.
   */
  private TypeIndex currentIndex(final Schema schema) {
    if (index.isValid() && canAnswer(index, predicate))
      return index;
    try {
      if (!schema.existsType(typeName))
        return null;
      final TypeIndex replacement = schema.getType(typeName).getPolymorphicIndexByProperties(indexProperties);
      if (!canAnswer(replacement, predicate))
        return null;
      if (replacement != index) {
        index = replacement;
        allowedBucketsResolved = false;
      }
      return replacement;
    } catch (final IndexException e) {
      return null;
    }
  }

  /**
   * Opens the cursor, or returns null when no key can satisfy the predicate. A bound no key of an integral index equals (29.5)
   * is moved onto the key that bounds the same keys, inclusive (the {@code HasStep} after the step re-checks the exact
   * predicate): the index would otherwise truncate it toward zero and drop a match for {@code lt} above zero and {@code gt}
   * below zero (#9206).
   */
  private IndexCursor openCursor(final TypeIndex index) {
    if (predicate == Compare.eq)
      return index.get(keys);

    final boolean lowerBound = predicate == Compare.gt || predicate == Compare.gte;
    boolean inclusive = predicate == Compare.gte || predicate == Compare.lte;
    Object[] bound = keys;

    final Type[] keyTypes = index.getKeyTypes();
    final byte leadingKeyType = IntegralKeyBound.binaryTypeOf(keyTypes != null && keyTypes.length > 0 ? keyTypes[0] : null);
    // isInexact is true only for a Number, so the cast below is safe
    if (bound.length > 0 && IntegralKeyBound.isInexact(leadingKeyType, bound[0])) {
      final Number mapped = lowerBound ?
          IntegralKeyBound.ceiling(leadingKeyType, (Number) bound[0]) :
          IntegralKeyBound.floor(leadingKeyType, (Number) bound[0]);
      if (mapped == null)
        return null;
      bound = bound.clone();
      bound[0] = mapped;
      inclusive = true;
    }
    return index.iterator(lowerBound, bound, inclusive);
  }

  /**
   * The bucket ids a candidate may live in, indexed by bucket id, or null when every entry of the index qualifies.
   * An index declared on the named type itself already spans exactly that type's polymorphic buckets.
   */
  private boolean[] allowedBuckets(final Schema schema, final TypeIndex index) {
    if (typeName.equals(index.getTypeName()))
      return null;

    final List<Bucket> buckets = schema.getType(typeName).getBuckets(true);
    int max = -1;
    for (final Bucket b : buckets)
      max = Math.max(max, b.getFileId());
    final boolean[] allowed = new boolean[max + 1];
    for (final Bucket b : buckets)
      allowed[b.getFileId()] = true;
    return allowed;
  }

  private Iterator<E> openIterator() {
    final ArcadeGraph graph = (ArcadeGraph) getTraversal().getGraph().get();
    final Schema schema = graph.getDatabase().getSchema();
    final TypeIndex current = currentIndex(schema);
    if (current == null)
      return new CandidateIterator(graph, null, null, null, null);

    final IndexCursor cursor;
    final IndexInternal[] subIndexes = current.getIndexesOnBuckets();
    try {
      if (!allowedBucketsResolved) {
        allowedBuckets = allowedBuckets(schema, current);
        allowedBucketsResolved = true;
      }
      cursor = openCursor(current);
    } catch (final RuntimeException e) {
      if (!isLostIndex(current, e))
        throw e;
      // NOTHING HAS BEEN EMITTED YET: THE TYPE ANSWERS INSTEAD OF THE INDEX DROPPED SINCE IT WAS CHECKED
      return new CandidateIterator(graph, null, null, null, null);
    }
    if (cursor == null)
      return EmptyIterator.instance();
    return new CandidateIterator(graph, current, subIndexes, cursor, allowedBuckets);
  }

  /**
   * The candidates of one scan: the entries of the index cursor, or, without one, every element of the type. A cursor whose
   * index goes away under it (it fails, or it ends after its index was dropped or rebuilt, when it may have missed the
   * entries of sub-indexes already gone) hands over to a scan of the type that skips what the cursor already emitted.
   */
  private final class CandidateIterator implements CloseableIterator<E> {
    private final ArcadeGraph      graph;
    private final TypeIndex        cursorIndex;
    // THE SUB-INDEXES OF THE INDEX WHEN THE CURSOR WAS OPENED: A DROP TAKES THEM AWAY ONE AT A TIME
    private final IndexInternal[]  subIndexes;
    private final boolean[]        allowed;
    private       IndexCursor      cursor;
    private       Iterator<Record> typeScan;
    // THE CANDIDATES THE CURSOR EMITTED, PACKED, UNTIL MAX_TRACKED_CANDIDATES: NULL ONCE THE SCAN STOPPED REMEMBERING
    private       LongHashSet      emitted;
    private       boolean          tracking;
    private       E                next;
    private       boolean          closed;

    private CandidateIterator(final ArcadeGraph graph, final TypeIndex cursorIndex, final IndexInternal[] subIndexes,
        final IndexCursor cursor, final boolean[] allowed) {
      this.graph = graph;
      this.cursorIndex = cursorIndex;
      this.subIndexes = subIndexes;
      this.cursor = cursor;
      this.allowed = allowed;
      this.tracking = cursor != null;
      if (cursor == null)
        this.typeScan = graph.getDatabase().iterateType(typeName, true);
    }

    @Override
    public boolean hasNext() {
      if (next != null)
        return true;
      if (closed)
        return false;

      if (cursor != null) {
        RuntimeException lost;
        try {
          while (cursor.hasNext()) {
            final Identifiable candidate = cursor.next();
            final int bucketId = candidate.getIdentity().getBucketId();
            if (allowed != null && (bucketId < 0 || bucketId >= allowed.length || !allowed[bucketId]))
              continue;
            if (load(candidate)) {
              remember(candidate);
              return true;
            }
          }
          // A DROP THAT STARTED SINCE THE CURSOR WAS OPENED MAY HAVE TAKEN SUB-INDEXES AWAY UNDER IT WITHOUT AN ERROR
          if (cursorIndex.isUnchangedSince(subIndexes)) {
            // #5662: RELEASE THE CURSOR AS SOON AS IT IS EXHAUSTED, NOT WHEN THE TRAVERSAL IS CLOSED: A COMPACTED-SERIES
            // CURSOR LEFT REGISTERED KEEPS A RETIRED INDEX FILE UNDROPPABLE UNTIL THE NEXT RESTART
            close();
            return false;
          }
          lost = null;
        } catch (final RuntimeException e) {
          if (!isLostIndex(cursorIndex, e))
            throw e;
          lost = e;
        }
        // THE INDEX WAS DROPPED OR REBUILT WHILE IT WAS READ: WHAT IT DID NOT EMIT IS FOUND ON THE TYPE
        handOverToTypeScan(lost);
      }

      while (typeScan.hasNext()) {
        final Record record = typeScan.next();
        if (emitted != null && emitted.contains(pack(record.getIdentity())))
          continue;
        if (load(record))
          return true;
      }
      close();
      return false;
    }

    /**
     * @param cause what the cursor raised, or null when it ended normally after its index changed under it
     */
    private void handOverToTypeScan(final RuntimeException cause) {
      final IndexCursor lostCursor = cursor;
      cursor = null;
      try {
        lostCursor.close();
      } catch (final RuntimeException ignored) {
        // THE CURSOR OF A DROPPED INDEX: NOTHING LEFT TO RELEASE
      }
      // LOGGED, SO A CURSOR THAT FAILS FOR ANOTHER REASON IS NOT HIDDEN BEHIND A SILENT SCAN OF THE TYPE
      LogManager.instance().log(ArcadeFilterByIndexStep.this, Level.FINE,
          "Index '%s' changed while traversal step read it (%s): finishing the scan on type '%s'", cause, cursorIndex.getName(),
          cause != null ? cause.toString() : "dropped or rebuilt", typeName);
      if (!tracking)
        throw new IndexException("Index '" + cursorIndex.getName() + "' was dropped or rebuilt while the traversal was reading it, "
            + "after more than " + MAX_TRACKED_CANDIDATES + " candidates: run the traversal again");
      typeScan = graph.getDatabase().iterateType(typeName, true);
    }

    private void remember(final Identifiable candidate) {
      if (!tracking)
        return;
      if (emitted == null)
        emitted = new LongHashSet();
      if (emitted.size() >= MAX_TRACKED_CANDIDATES) {
        emitted = null;
        tracking = false;
        return;
      }
      emitted.add(pack(candidate.getIdentity()));
    }

    /**
     * Loads the record of a candidate into {@link #next}, or answers false when it is gone: getRecord() either answers null or
     * raises RecordNotFoundException for an entry whose record was deleted, depending on where the lookup finds it missing.
     */
    @SuppressWarnings("unchecked")
    private boolean load(final Identifiable candidate) {
      final Record rec;
      try {
        rec = candidate instanceof Record record ? record : candidate.getRecord();
      } catch (final RecordNotFoundException e) {
        return false;
      }
      if (rec instanceof com.arcadedb.graph.Vertex vertex)
        next = (E) new ArcadeVertex(graph, vertex);
      else if (rec instanceof com.arcadedb.graph.Edge edge)
        next = (E) new ArcadeEdge(graph, edge);
      else if (rec != null)
        throw new IllegalStateException("Record of type '" + rec.getClass() + "' is not a graph element");
      return next != null;
    }

    @Override
    public E next() {
      if (!hasNext())
        throw new NoSuchElementException();
      final E result = next;
      next = null;
      return result;
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        if (cursor != null)
          cursor.close();
      }
    }
  }

  /**
   * Whether {@code e}, raised opening or reading the cursor of {@code index}, means the index went away under it. A
   * {@link NoSuchElementException} counts too: raised from inside a step, TinkerPop takes it for the end of the stream, so
   * letting it through would answer a short result as a success, which a scan of the type cannot do (#9651).
   */
  private static boolean isLostIndex(final TypeIndex index, final RuntimeException e) {
    return e instanceof NoSuchElementException || TypeIndex.isStaleRead(index, e);
  }

  /** A RID as one long: the bucket in the top 16 bits, the position in the other 48, which no bucket reaches. */
  private static long pack(final RID rid) {
    return ((long) rid.getBucketId() << 48) | rid.getPosition();
  }

  public String toString() {
    return StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(Locale.ENGLISH), typeName, index.getName(),
        predicate + "(" + Arrays.toString(keys) + ")");
  }

  @Override
  public Parameters getParameters() {
    return this.parameters;
  }

  @Override
  public void configure(final Object... keyValues) {
    this.parameters.set(null, keyValues);
  }

  public Class<E> getReturnClass() {
    return this.returnClass;
  }

  public boolean isStartStep() {
    return this.isStart;
  }

  public static boolean isStartStep(final Step<?, ?> step) {
    return step instanceof GraphStep gs && gs.isStartStep();
  }

  public boolean returnsVertex() {
    return this.returnClass.equals(Vertex.class);
  }

  public boolean returnsEdge() {
    return this.returnClass.equals(Edge.class);
  }

  @Override
  protected Traverser.Admin<E> processNextStart() {
    while (true) {
      if (this.iterator.hasNext()) {
        return this.isStart ?
            this.getTraversal().getTraverserGenerator().generate(this.iterator.next(), (Step) this, 1l) :
            this.head.split(this.iterator.next(), this);
      } else {
        if (this.isStart) {
          if (this.done)
            throw FastNoSuchElementException.instance();
          else {
            this.done = true;
            this.iterator = openIterator();
          }
        } else {
          this.head = this.starts.next();
          // ONE SCAN PER INCOMING TRAVERSER, AT THE TIME IT ARRIVES: WHAT THE PREVIOUS STEPS WROTE IS VISIBLE
          this.iterator = openIterator();
        }
      }
    }
  }

  @Override
  public void reset() {
    super.reset();
    // NOT CLOSED HERE: AbstractStep.clone() RESETS THE CLONE WHILE IT STILL SHARES THIS STEP'S ITERATOR
    this.head = null;
    this.done = false;
    this.iterator = EmptyIterator.instance();
  }

  @Override
  public int hashCode() {
    return Objects.hash(returnClass, index.getName(), predicate, Arrays.hashCode(keys), typeName);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (!(o instanceof ArcadeFilterByIndexStep))
      return false;
    if (!super.equals(o))
      return false;
    final ArcadeFilterByIndexStep<?, ?> that = (ArcadeFilterByIndexStep<?, ?>) o;
    return Objects.equals(index.getName(), that.index.getName()) && Objects.equals(predicate, that.predicate) && Arrays.equals(keys, that.keys)
        && Objects.equals(typeName, that.typeName) && Objects.equals(returnClass, that.returnClass);
  }

  /**
   * Attempts to close an underlying iterator if it is of type {@link CloseableIterator}. Graph providers may choose
   * to return this interface containing their vertices and edges if there are expensive resources that might need to
   * be released at some point.
   */
  @Override
  public void close() {
    CloseableIterator.closeIterator(iterator);
  }

  /**
   * Helper method for providers that want to "fold in" {@link HasContainer}'s based on id checking into the ids of the {@link GraphStep}.
   *
   * @param graphStep    the GraphStep to potentially {@link GraphStep#addIds(Object...)}.
   * @param hasContainer The {@link HasContainer} to check for id validation.
   *
   * @return true if the {@link HasContainer} updated ids and thus, was processed.
   */
  public static boolean processHasContainerIds(final GraphStep<?, ?> graphStep, final HasContainer hasContainer) {
    if (hasContainer.getKey().equals(T.id.getAccessor()) && graphStep.getIds().length == 0 && (hasContainer.getBiPredicate() == Compare.eq
        || hasContainer.getBiPredicate() == Contains.within)) {
      graphStep.addIds(hasContainer.getValue());
      return true;
    }
    return false;
  }
}
