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

import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinAntlrToJava;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinParser;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinQueryParser;
import org.apache.tinkerpop.gremlin.language.grammar.TraversalRootVisitor;
import org.apache.tinkerpop.gremlin.language.grammar.VariableResolver;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.ReadWriting;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IoStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tells what a Gremlin script does without running it (issue #11182). The gremlin-lang script engine builds a traversal by
 * evaluating the text, and evaluating applies the terminal step it ends with ({@code iterate()}, {@code next()}, {@code toList()}...):
 * an analysis that went through it applied the write it was asked about. On an HA follower, which analyses a command to decide whether
 * to forward it, one write was then applied locally, forwarded and applied again on the leader, and failed the follower's commit.
 * <p>
 * Here every statement of the script is built from its parse tree and none is run:
 * <ul>
 * <li>a terminal step is never applied, and neither is a transaction control ({@code g.tx().commit()})</li>
 * <li>every statement of a script counts, while the script engine answers only the last one, so a write before a read is a write</li>
 * <li>a traversal that has to run to build another one, a terminated traversal passed as an argument
 * ({@code g.V(g.V().has('name','a').id().next())}), runs only when it reads: the source it is built from carries
 * {@link WriteRefusingStrategy}, which stops one that writes before its first step, and the script is then a write</li>
 * </ul>
 * A script the grammar cannot read is Groovy, which only the {@code auto} and {@code groovy} engines run: it is code, which only
 * evaluating tells the traversal of, so {@code ArcadeGremlin} evaluates it with a {@link #guarded} source, which lets a traversal
 * the script runs while it is evaluated run only when it reads.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class GremlinStatementAnalyzer {
  private static final Set<OperationType> READ      = Set.of(OperationType.READ);
  private static final Set<OperationType> ANY_WRITE = Set.of(OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE);

  private GremlinStatementAnalyzer() {
  }

  /**
   * What {@code query} does, read from the traversals it builds, none of which is run.
   *
   * @throws org.apache.tinkerpop.gremlin.language.grammar.GremlinParserException when the gremlin-lang grammar cannot read the script
   */
  static QueryEngine.AnalyzedQuery analyze(final GraphTraversalSource source, final String query, final Map<String, Object> parameters) {
    try {
      return analyze(traversalsOf(source, query, parameters));
    } catch (final WriteRefusedException e) {
      // THE OUTER TRAVERSAL CANNOT BE BUILT WITHOUT THE VALUE OF THE ONE THAT WRITES, SO WHAT IT DOES IS NOT KNOWN: ANY WRITE
      return anyWrite();
    }
  }

  /**
   * What {@code traversals}, built and not run, do.
   */
  static QueryEngine.AnalyzedQuery analyze(final List<Traversal.Admin<?, ?>> traversals) {
    final EnumSet<OperationType> writes = EnumSet.noneOf(OperationType.class);
    for (final Traversal.Admin<?, ?> traversal : traversals)
      collectWrites(traversal, writes);
    return writes.isEmpty() ? read() : result(Set.copyOf(writes));
  }

  /**
   * {@code source} with {@link WriteRefusingStrategy}: a traversal built from it that writes is stopped before its first step, with
   * an exception {@link #isWriteRefused} recognises.
   */
  static GraphTraversalSource guarded(final GraphTraversalSource source) {
    return source.withStrategies(WriteRefusingStrategy.INSTANCE);
  }

  /**
   * Whether {@code e} is, or was caused by, a traversal that {@link #guarded} stopped because it writes.
   */
  static boolean isWriteRefused(final Throwable e) {
    int depth = 0;
    for (Throwable t = e; t != null && depth++ < 16; t = t.getCause())
      if (t instanceof WriteRefusedException)
        return true;
    return false;
  }

  static QueryEngine.AnalyzedQuery read() {
    return result(READ);
  }

  /**
   * The traversal of every statement of {@code query}, built and not run. Package-private for the tests.
   */
  static List<Traversal.Admin<?, ?>> traversalsOf(final GraphTraversalSource source, final String query,
      final Map<String, Object> parameters) {
    final StatementCollector collector = new StatementCollector(guarded(source), resolverOf(parameters));
    GremlinQueryParser.parse(query, collector);
    return collector.traversals;
  }

  /**
   * The answer for a script that tried to run a write while it was built or evaluated: what else it does is not known, so it may
   * write anything.
   */
  static QueryEngine.AnalyzedQuery anyWrite() {
    return result(ANY_WRITE);
  }

  /**
   * Adds to {@code writes} what {@code traversal} writes, in its child traversals too: a write nested in {@code coalesce()},
   * {@code union()}, {@code sideEffect()}... is still a write, and no {@code TraversalParent} is itself {@link Mutating} (#8296).
   *
   * @return whether the traversal writes
   */
  static boolean collectWrites(final Traversal.Admin<?, ?> traversal, final EnumSet<OperationType> writes) {
    boolean writing = false;
    for (final Step<?, ?> step : TraversalHelper.getStepsOfAssignableClassRecursively(traversal, Mutating.class, IoStep.class)) {
      if (step instanceof IoStep<?> ioStep) {
        // io().read() LOADS A FILE INTO THE GRAPH WITHOUT BEING A Mutating STEP; io().write() ONLY READS THE GRAPH
        if (ioStep.getMode() != ReadWriting.Mode.WRITING) {
          writing = true;
          writes.add(OperationType.CREATE);
        }
        continue;
      }

      writing = true;
      // THE STEPS OF A TRAVERSAL NO STRATEGY WAS APPLIED TO ARE GValue PLACEHOLDERS (AddVertexStartStepPlaceholder...), RESOLVED
      // INTO THE CONCRETE ONES BY GValueReductionStrategy ONLY: BOTH IMPLEMENT THE *StepContract INTERFACE, SO THE CHECKS ARE ON
      // THAT (#5838). DropStep HAS NO PLACEHOLDER
      if (step instanceof AddVertexStepContract || step instanceof AddEdgeStepContract)
        writes.add(OperationType.CREATE);
      else if (step instanceof MergeStepContract) {
        // mergeV()/mergeE() CREATE OR UPDATE DEPENDING ON WHAT THEY FIND
        writes.add(OperationType.CREATE);
        writes.add(OperationType.UPDATE);
      } else if (step instanceof DropStep)
        writes.add(OperationType.DELETE);
      else if (step instanceof AddPropertyStepContract)
        writes.add(OperationType.UPDATE);
      else
        // AN UNKNOWN MUTATING STEP: ANY WRITE
        writes.addAll(ANY_WRITE);
    }
    return writing;
  }

  /**
   * The parameters of the call when it has them, else {@code null} for every variable: an HA follower analyses a statement without
   * its bindings (#5187), and the shape of the traversal, which is all the analysis reads, does not depend on their values.
   */
  private static VariableResolver<Object> resolverOf(final Map<String, Object> parameters) {
    if (parameters == null || parameters.isEmpty())
      return VariableResolver.NullVariableResolver.instance();
    return (name, context) -> parameters.get(name);
  }

  private static QueryEngine.AnalyzedQuery result(final Set<OperationType> operations) {
    final boolean idempotent = operations.equals(READ);
    return new QueryEngine.AnalyzedQuery() {
      @Override
      public boolean isIdempotent() {
        return idempotent;
      }

      @Override
      public boolean isDDL() {
        return false;
      }

      @Override
      public Set<OperationType> getOperationTypes() {
        return operations;
      }
    };
  }

  /**
   * Builds the traversal of every statement of the script and applies none of what would run it: the terminal step a statement ends
   * with, a transaction control. The base class answers the value of the last statement, after running it.
   */
  private static final class StatementCollector extends GremlinAntlrToJava {
    private final List<Traversal.Admin<?, ?>> traversals = new ArrayList<>(1);

    private StatementCollector(final GraphTraversalSource source, final VariableResolver<Object> resolver) {
      super(source, resolver);
    }

    @Override
    public Object visitQuery(final GremlinParser.QueryContext ctx) {
      if (ctx.rootTraversal() != null)
        // WITH OR WITHOUT ITS TERMINAL STEP, WHICH IS WHAT WOULD RUN IT
        traversals.add(new TraversalRootVisitor<>(this).visitRootTraversal(ctx.rootTraversal()).asAdmin());
      else if (ctx.query() != null)
        // <query>.toString()
        visitQuery(ctx.query());
      // A BARE TRAVERSAL SOURCE, A TRANSACTION CONTROL OR AN EMPTY STATEMENT: NOTHING THAT READS OR WRITES THE GRAPH
      return null;
    }
  }

  /**
   * On the source the analysis builds from, so it runs only for a traversal that is executed while the script is built: a terminated
   * traversal passed as an argument. One that writes is stopped before its first step.
   */
  private static final class WriteRefusingStrategy extends AbstractTraversalStrategy<TraversalStrategy.VerificationStrategy>
      implements TraversalStrategy.VerificationStrategy {
    private static final WriteRefusingStrategy INSTANCE = new WriteRefusingStrategy();

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
      // THE CHILDREN ARE READ FROM THEIR ROOT
      if (traversal.isRoot() && collectWrites(traversal, EnumSet.noneOf(OperationType.class)))
        throw new WriteRefusedException();
    }
  }

  private static final class WriteRefusedException extends RuntimeException {
    private WriteRefusedException() {
      super("A Gremlin traversal that writes was not run while its script was analysed", null, false, false);
    }
  }
}
