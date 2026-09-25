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
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
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
 * </ul>
 */
public class ArcadeFilterByIndexStep<S, E extends Element> extends AbstractStep<S, E> implements AutoCloseable, Configuring {
  private final   TypeIndex                            index;
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
   */
  public ArcadeFilterByIndexStep(final Traversal.Admin traversal, final Class returnClass, final boolean isStart, final TypeIndex index,
      final BiPredicate<?, ?> predicate, final Object[] keys, final String typeName) {
    super(traversal);
    if (!isSupported(predicate))
      throw new IllegalArgumentException("Unsupported index predicate '" + predicate + "'");
    this.index = index;
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

  private IndexCursor openCursor() {
    if (predicate == Compare.eq)
      return index.get(keys);
    if (predicate == Compare.gt)
      return index.iterator(true, keys, false);
    if (predicate == Compare.gte)
      return index.iterator(true, keys, true);
    if (predicate == Compare.lt)
      return index.iterator(false, keys, false);
    return index.iterator(false, keys, true);
  }

  /**
   * The bucket ids a candidate may live in, indexed by bucket id, or null when every entry of the index qualifies.
   * An index declared on the named type itself already spans exactly that type's polymorphic buckets.
   */
  private boolean[] allowedBuckets(final Schema schema) {
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
    if (!allowedBucketsResolved) {
      allowedBuckets = allowedBuckets(graph.getDatabase().getSchema());
      allowedBucketsResolved = true;
    }
    final boolean[] allowed = allowedBuckets;
    final IndexCursor cursor = openCursor();

    return new CloseableIterator<>() {
      private E       next;
      private boolean closed;

      @Override
      public boolean hasNext() {
        if (next != null)
          return true;
        if (closed)
          return false;

        while (cursor.hasNext()) {
          final Identifiable candidate = cursor.next();
          final int bucketId = candidate.getIdentity().getBucketId();
          if (allowed != null && (bucketId < 0 || bucketId >= allowed.length || !allowed[bucketId]))
            continue;

          final Record rec = candidate.getRecord();
          if (rec instanceof com.arcadedb.graph.Vertex vertex)
            next = (E) new ArcadeVertex(graph, vertex);
          else if (rec instanceof com.arcadedb.graph.Edge edge)
            next = (E) new ArcadeEdge(graph, edge);
          else if (rec != null)
            throw new IllegalStateException("Record of type '" + rec.getClass() + "' is not a graph element");
          // A NULL RECORD IS AN INDEX ENTRY WHOSE RECORD IS GONE: SKIPPED, THE SAME AS A RECORD THAT NO LONGER MATCHES
          if (next != null)
            return true;
        }
        // #5662: RELEASE THE CURSOR AS SOON AS IT IS EXHAUSTED, NOT WHEN THE TRAVERSAL IS CLOSED: A COMPACTED-SERIES
        // CURSOR LEFT REGISTERED KEEPS A RETIRED INDEX FILE UNDROPPABLE UNTIL THE NEXT RESTART
        close();
        return false;
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
          cursor.close();
        }
      }
    };
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
