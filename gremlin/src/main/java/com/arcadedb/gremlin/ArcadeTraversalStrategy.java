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

import com.arcadedb.database.Database;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.GraphViewPins;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.BinaryComparator;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.ReadWriting;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CallStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.FailStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Replaces default traversal steps to speedup execution. This is used only when the traversal has a GraphStep (vertices or edges) and HasStep (label eq(X)).
 * <p>
 * Declared as a {@link TraversalStrategy.ProviderOptimizationStrategy} (not an
 * {@code OptimizationStrategy}) so that TinkerPop's category ordering guarantees it always runs AFTER
 * {@code GValueReductionStrategy}. A Gremlin query submitted as a string (the entry point used by the
 * HTTP endpoint, Studio, and the drivers) parses any hop naming an edge label into a
 * {@code VertexStepPlaceholder}, not a concrete {@code VertexStep}; {@code GValueReductionStrategy}
 * resolves those placeholders into concrete steps. Every {@code OptimizationStrategy} implementation
 * inherits a default {@code applyPost()} that adds {@code GValueReductionStrategy}, guaranteeing it
 * runs LAST within the {@code OptimizationStrategy} category - which meant, back when this class was
 * also declared {@code OptimizationStrategy}, that it always ran BEFORE the placeholders it needed
 * resolved were resolved. Category ordering, unlike {@code applyPrior()}/{@code applyPost()}, is
 * enforced unconditionally by {@code TraversalStrategies.sortStrategies}: every strategy in an earlier
 * category is a hard prerequisite for every strategy in a later one, so moving this strategy to the
 * {@code ProviderOptimizationStrategy} category (which sorts strictly after
 * {@code OptimizationStrategy}) fixes the ordering deterministically. See issue #5840.
 * <p>
 * The {@code applyPost()} default this relies on lives in TinkerPop 3.8.1's
 * {@code org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.OptimizationStrategy}
 * (gremlin-core, {@code TraversalStrategy.java} around line 179): {@code default
 * Set<Class<? extends OptimizationStrategy>> applyPost() { ... set.add(GValueReductionStrategy.class);
 * ... }}, documented there as "The {@code GValueReductionStrategy} should be the last TinkerPop
 * optimization strategy applied. Adding it here ensures will help to enforce that more globally." If a
 * future TinkerPop release changes that default, re-verify this strategy's placement still runs after
 * {@code GValueReductionStrategy}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadeTraversalStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
    implements TraversalStrategy.ProviderOptimizationStrategy {

  private static final String LABEL_KEY = "~label";

  // THE STEPS OF TINKERPOP DO NOTHING BUT READ UNLESS THEY ARE MARKED OTHERWISE (SEE hasNoEffect()), AND SO DO THESE OF THIS MODULE,
  // NAMED ONE BY ONE: A STEP NOT LISTED, A FUTURE ONE OF THIS MODULE INCLUDED, IS UNKNOWN, AND AN UNKNOWN STEP IS ASSUMED TO HAVE AN
  // EFFECT
  private static final String           TINKERPOP_STEP_PACKAGE = "org.apache.tinkerpop.gremlin.process.traversal.step.";
  private static final Set<Class<?>>    ARCADE_READ_ONLY_STEPS = Set.of(ArcadeCountGlobalStep.class, ArcadeEdgeCountFilterStep.class,
      ArcadeFilterByIndexStep.class, ArcadeFilterByTypeStep.class, ArcadeGAVFusedStep.class, ArcadeGAVVertexStep.class);

  @Override
  public void apply(final Traversal.Admin<?, ?> traversal) {
    // BEFORE ANY REWRITE: THE REWRITES BELOW TAKE THE LABEL OUT OF THE has() STEP (#10442)
    if (traversal.getGraph().orElse(null) instanceof ArcadeGraph arcadeGraph)
      TimeSeriesLabelGuard.check(arcadeGraph.database.getSchema(), traversal);

    // A FILTER THAT CAN NEVER LET A TRAVERSER THROUGH ENDS THE WALK IN FRONT OF IT, BEFORE ANY OF THE REWRITES BELOW LOOKS AT THE STEPS
    applyAlwaysEmptyFilterOptimization(traversal);

    boolean replacedWithFilterByType = false;
    final List<Step> steps = traversal.getSteps();
    for (int i = 1; i < steps.size(); i++) {
      final Step step = steps.get(i);
      if (step instanceof HasStep) {
        final Step prevStep = steps.get(i - 1);
        if (prevStep instanceof GraphStep prevStepGraph) {

          if (prevStepGraph.getIds().length != 0)
            continue;

          final List<HasContainer> hasContainers = new ArrayList<>(((HasStep) step).getHasContainers());

          String typeNameToMatch = null;

          final Set<HasContainer> containersToRemove = new HashSet<>();

          int totalLabels = 0;
          for (final HasContainer c : hasContainers) {
            final String key = c.getKey();
            if (BinaryComparator.equalsString(key, LABEL_KEY)) {
              ++totalLabels;

              if (totalLabels > 1) {
                typeNameToMatch = null;
                containersToRemove.clear();
                break;
              }

              if (c.getBiPredicate().equals(Compare.eq) && c.getValue() != null) {
                typeNameToMatch = c.getValue().toString();
                containersToRemove.add(c);
              }
            }
          }

          for (final HasContainer c : containersToRemove)
            ((HasStep<?>) step).removeHasContainer(c);

          if (totalLabels == 1 && typeNameToMatch != null) {
            // LOOKING FOR INDEX LOOKUP
            final ArcadeGraph graph = (ArcadeGraph) traversal.getGraph().get();

            // ONLY USE AN INDEX WHEN THE LABEL MATCHES THE TRAVERSAL ELEMENT KIND (g.V() -> vertex type, g.E() -> edge type).
            // OTHERWISE A WRONG-KIND INDEX SCAN WOULD RETURN ELEMENTS OF THE OPPOSITE CATEGORY (ISSUE #5223). THE EMPTY
            // indexCursors LIST THEN FALLS THROUGH TO ArcadeFilterByTypeStep/ArcadeCountGlobalStep, WHICH RETURN NOTHING.
            final boolean kindMatches;
            if (graph.database.getSchema().existsType(typeNameToMatch)) {
              final DocumentType type = graph.database.getSchema().getType(typeNameToMatch);
              kindMatches = Vertex.class.isAssignableFrom(prevStepGraph.getReturnClass()) ?
                  type instanceof VertexType :
                  type instanceof EdgeType;
            } else
              kindMatches = false;

            // PICK ONE INDEXED CONTAINER TO GENERATE THE CANDIDATES FROM, PREFERRING A UNIQUE EQUALITY, THEN ANY EQUALITY,
            // THEN A RANGE. EVERY has() CONTAINER, THE CHOSEN ONE INCLUDED, STAYS IN THE HasStep AND IS RE-CHECKED ON EACH
            // CANDIDATE, SO INTERSECTING A SECOND INDEX WOULD ONLY REPEAT THAT WORK WHILE MATERIALISING BOTH SCANS (#8299)
            TypeIndex chosenIndex = null;
            HasContainer chosen = null;
            int chosenRank = Integer.MAX_VALUE;
            if (kindMatches)
              for (final HasContainer c : hasContainers) {
                final String key = c.getKey();
                if (key.startsWith("~") || c.getValue() == null || !ArcadeFilterByIndexStep.isSupported(c.getBiPredicate()))
                  continue;
                // WALKS UP THE HIERARCHY: A SUPER TYPE'S INDEX SPANS ITS SIBLING SUB-TYPES TOO, WHICH THE STEP FILTERS
                // OUT BY BUCKET (#8249)
                final TypeIndex index = graph.database.getSchema().getType(typeNameToMatch).getPolymorphicIndexByProperties(key);
                // EXACT KEY, ORDERED AND UNFOLDED FOR A RANGE, AND USABLE NOW: AN INDEX A CONCURRENT DDL IS CREATING OR
                // DROPPING IS NOT A CANDIDATE, AND ITS METADATA IS NOT READ AGAIN BELOW (#9651)
                if (!ArcadeFilterByIndexStep.canAnswer(index, c.getBiPredicate()))
                  continue;
                boolean unique;
                try {
                  unique = index.isUnique();
                } catch (final IndexException e) {
                  continue;
                }
                final int rank = c.getBiPredicate() == Compare.eq ? (unique ? 0 : 1) : 2;
                if (rank < chosenRank) {
                  chosenRank = rank;
                  chosenIndex = index;
                  chosen = c;
                }
              }

            final Step replaceWith;
            // #8258: true only for the count rewrite, which is a REDUCING BARRIER - real TinkerPop's own
            // CountGlobalStep does not carry a label from before it forward either (confirmed empirically:
            // the unoptimized g.V().as('v').hasLabel(X).count().select('v') throws NoSuchElementException,
            // it does not return the vertex). So a label on the GraphStep/HasStep this rewrite also removes
            // must NOT be copied onto ArcadeCountGlobalStep - only the count's OWN label (from countStep,
            // handled below) may survive, since that one names the traverser the count itself produces.
            boolean isCountRewrite = false;
            if (chosenIndex == null) {
              // #9140: only a count that starts the traversal can be answered from the type. Mid-traversal the
              // GraphStep runs once per incoming traverser and the steps in front must stay in place and execute
              if (prevStepGraph.isStartStep() && ((HasStep<?>) step).getHasContainers().isEmpty() &&
                  i + 1 < steps.size() && steps.get(i + 1) instanceof CountGlobalStep) {
                // #8258: keep a handle on the CountGlobalStep so its own label (if any) survives the rewrite below
                final Step countStep = steps.get(i + 1);
                traversal.removeStep(i - 1);
                traversal.removeStep(i - 1);
                isCountRewrite = true;
                replaceWith = new ArcadeCountGlobalStep(step.getTraversal(), prevStepGraph.getReturnClass(), typeNameToMatch);
                TraversalHelper.copyLabels(countStep, replaceWith, false);
              } else
                replaceWith = new ArcadeFilterByTypeStep(prevStepGraph.getTraversal(), prevStepGraph.getReturnClass(),
                    prevStepGraph.isStartStep(), typeNameToMatch);
              replacedWithFilterByType = true;
            } else
              replaceWith = new ArcadeFilterByIndexStep(prevStepGraph.getTraversal(), prevStepGraph.getReturnClass(),
                  prevStepGraph.isStartStep(), chosenIndex, chosen.getBiPredicate(),
                  chosen.getValue().getClass().isArray() ? (Object[]) chosen.getValue() : new Object[] { chosen.getValue() },
                  typeNameToMatch, chosen.getKey());

            if (replaceWith != null) {
              // #8258: a label on the GraphStep being replaced must survive on the new step, otherwise
              // select()/path()/where() naming it later finds nothing - except for the count rewrite, see above
              if (!isCountRewrite)
                TraversalHelper.copyLabels(prevStepGraph, replaceWith, false);
              //traversal.removeStep(i); // IF THE HAS-LABEL STEP IS REMOVED, FOR SOME REASON DOES NOT WORK
              traversal.removeStep(i - 1);
              traversal.addStep(i - 1, replaceWith);
            }

            if (replacedWithFilterByType)
              break;
          }
        }
      }
    }

    // GAV optimization: replace VertexStep with CSR-accelerated steps, then fuse consecutive ones
    applyGAVOptimization(traversal);

    // GAV optimization: replace where(outE().count().is(...)) with O(1) degree check
    applyEdgeCountFilterOptimization(traversal);
  }

  /**
   * Replaces {@code filter(limit(0))} and {@code where(limit(0))} with a {@code limit(0)} of their own (issue #10307).
   * <p>
   * The child of such a filter is empty for every input, so the filter drops every traverser - but only after the traversal in front
   * of it produced each one: {@code g.V(x).out('E').filter(__.limit(0))} expanded every neighbor of {@code x}, to throw each of them
   * away. A {@code limit(0)} in the traversal itself stops at the first traverser it is offered, which is the same answer: nothing.
   * <p>
   * Only when nothing in front of the filter has an effect of its own ({@link #hasNoEffect}): stopping at the first traverser also
   * stops what the steps in front would have done for the others, and TinkerPop's answer includes it. A mutation, a side effect that
   * is read back ({@code aggregate('x')...cap('x')}), a lambda, a service call and any step this module does not know all keep the
   * filter where it is.
   */
  private static void applyAlwaysEmptyFilterOptimization(final Traversal.Admin<?, ?> traversal) {
    final List<Step> steps = traversal.getSteps();
    for (int i = 0; i < steps.size(); i++) {
      final Step<?, ?> step = steps.get(i);
      if (!(step instanceof TraversalFilterStep<?> || step instanceof WhereTraversalStep<?>))
        continue;

      final List<?> children = ((TraversalParent) step).getLocalChildren();
      // THE CHILD TOO: ITS STEPS IN FRONT OF THE limit(0) RUN ONCE FOR EVERY TRAVERSER THE FILTER IS OFFERED, AND THE FILTER IS GONE
      if (children.size() != 1 || !neverEmitsAnything((Traversal.Admin<?, ?>) children.get(0))
          || !hasNoEffect(((Traversal.Admin<?, ?>) children.get(0)).getSteps()) || !hasNoEffect(steps.subList(0, i)))
        continue;

      final RangeGlobalStep<?> stop = new RangeGlobalStep<>(traversal, 0L, 0L);
      // A LABEL ON THE FILTER (as('x') AFTER IT) STAYS ON THE STEP THAT TAKES ITS PLACE
      TraversalHelper.copyLabels(step, stop, false);
      traversal.removeStep(i);
      traversal.addStep(i, stop);
    }
  }

  /**
   * Whether the traversal emits nothing whatever it is fed: it ends with a {@code limit(0)}, which lets no traverser through. The
   * steps in front of it still run, once for each traverser the traversal is offered, before the {@code limit(0)} stops it: whoever
   * drops the traversal has to know they have no effect ({@link #hasNoEffect}).
   */
  private static boolean neverEmitsAnything(final Traversal.Admin<?, ?> child) {
    final List<? extends Step> childSteps = child.getSteps();
    return !childSteps.isEmpty() && childSteps.get(childSteps.size() - 1) instanceof RangeGlobalStep<?> range
        && range.getHighRange() == 0L;
  }

  /**
   * Whether running the steps does nothing but read the graph, so that running them for fewer traversers changes nothing but the
   * work: no mutation, no side effect another step can read, no lambda, no service call, no step that is not TinkerPop's or this
   * module's. The children of a step are asked too.
   */
  private static boolean hasNoEffect(final Iterable<? extends Step> steps) {
    for (final Step<?, ?> step : steps) {
      // A FailStep IS AN EFFECT TOO: IT ENDS THE TRAVERSAL WITH AN EXCEPTION THE REWRITE WOULD NEVER LET IT RAISE
      if (step instanceof Mutating || step instanceof SideEffectCapable || step instanceof ReadWriting || step instanceof LambdaHolder
          || step instanceof CallStep || step instanceof FailStep)
        return false;

      if (!step.getClass().getName().startsWith(TINKERPOP_STEP_PACKAGE) && !ARCADE_READ_ONLY_STEPS.contains(step.getClass()))
        return false;

      if (step instanceof TraversalParent parent) {
        for (final Object child : parent.getLocalChildren())
          if (!hasNoEffect(((Traversal.Admin<?, ?>) child).getSteps()))
            return false;
        for (final Object child : parent.getGlobalChildren())
          if (!hasNoEffect(((Traversal.Admin<?, ?>) child).getSteps()))
            return false;
      }
    }
    return true;
  }

  /**
   * Replaces TinkerPop VertexStep instances with GAV-accelerated steps when a CSR provider
   * is available. Then fuses 2+ consecutive GAV steps into a single fused chain step.
   */
  private void applyGAVOptimization(final Traversal.Admin<?, ?> traversal) {
    if (traversal.getGraph().isEmpty())
      return;
    final ArcadeGraph graph = (ArcadeGraph) traversal.getGraph().get();
    if (!(graph.getDatabase() instanceof Database db))
      return;

    // One pin per view for the whole traversal: the steps of a view then share it, which is what lets Phase 2 fuse them
    final GraphViewPins pins = new GraphViewPins();

    // Phase 1: Replace VertexStep → ArcadeGAVVertexStep when provider covers the edge labels
    final List<Step> steps = traversal.getSteps();
    for (int i = 0; i < steps.size(); i++) {
      final Step step = steps.get(i);
      if (step instanceof VertexStep<?> vertexStep && vertexStep.returnsVertex()) {
        final String[] edgeLabels = vertexStep.getEdgeLabels();
        // Pinned when the traversal is about to run: the step maps, walks and maps back in one snapshot for the whole traversal,
        // whatever the view does meanwhile (issue #10415)
        final GraphTraversalProvider provider = pins.pin(edgeLabels.length == 0
            ? GraphTraversalProviderRegistry.findProvider(db)
            : GraphTraversalProviderRegistry.findProvider(db, edgeLabels));
        if (provider != null) {
          final ArcadeGAVVertexStep gavStep = new ArcadeGAVVertexStep(
              graph, vertexStep, provider, vertexStep.getDirection(), edgeLabels);
          TraversalHelper.copyLabels(vertexStep, gavStep, false);
          traversal.removeStep(i);
          traversal.addStep(i, gavStep);
        }
      }
    }

    // Phase 2: Fuse consecutive ArcadeGAVVertexStep instances into a single ArcadeGAVFusedStep
    final List<Step> updatedSteps = traversal.getSteps();
    int i = 0;
    while (i < updatedSteps.size()) {
      if (updatedSteps.get(i) instanceof ArcadeGAVVertexStep firstGavStep) {
        // Collect consecutive GAV steps with the same provider
        final List<ArcadeGAVVertexStep> chain = new ArrayList<>();
        chain.add(firstGavStep);
        int j = i + 1;
        while (j < updatedSteps.size() && updatedSteps.get(j) instanceof ArcadeGAVVertexStep nextGavStep
            && nextGavStep.getProvider() == firstGavStep.getProvider()) {
          chain.add(nextGavStep);
          j++;
        }

        if (chain.size() >= 2) {
          // #8258: the fused step emits only the traverser at the END of the chain, so it cannot honour a
          // label attached to an intermediate hop (select()/path()/where() naming it would find nothing), nor
          // can it feed a traversal that needs the whole path (an explicit path()/simplePath()/cyclicPath()).
          // The last hop's label is safe: it names exactly the traverser the fused step produces. The path
          // requirement is the ROOT traversal's: a child traversal (local(), repeat(), ...) runs on traversers the
          // root's generator creates, so a path() on the root needs every hop of a chain inside the child too, and the
          // child's own requirements do not list it.
          boolean canFuse = !TraversalHelper.getRootTraversal(traversal).getTraverserRequirements()
              .contains(TraverserRequirement.PATH);
          for (int k = 0; canFuse && k < chain.size() - 1; k++)
            if (!chain.get(k).getLabels().isEmpty())
              canFuse = false;

          if (canFuse) {
            // Fuse the chain into a single step
            final ArcadeGAVFusedStep fusedStep = new ArcadeGAVFusedStep(
                traversal, graph, firstGavStep.getProvider(), chain);
            TraversalHelper.copyLabels(chain.get(chain.size() - 1), fusedStep, false);
            // Remove all steps in the chain (backwards to preserve indices)
            for (int k = j - 1; k >= i; k--)
              traversal.removeStep(k);
            traversal.addStep(i, fusedStep);
            // Don't increment i — check if next step is also fusible (it won't be, but safe)
          }
        }
      }
      i++;
    }
  }

  /**
   * Detects {@code where(outE('X').count().is(predicate))} patterns and replaces the entire
   * where step with an {@link ArcadeEdgeCountFilterStep} that uses GAV's O(1) countEdges.
   */
  @SuppressWarnings("unchecked")
  private void applyEdgeCountFilterOptimization(final Traversal.Admin<?, ?> traversal) {
    if (traversal.getGraph().isEmpty())
      return;
    final ArcadeGraph graph = (ArcadeGraph) traversal.getGraph().get();
    if (!(graph.getDatabase() instanceof Database db))
      return;

    final List<Step> steps = traversal.getSteps();
    for (int i = 0; i < steps.size(); i++) {
      final Step step = steps.get(i);
      // Match WhereTraversalStep or TraversalFilterStep containing the edge count pattern
      if (!(step instanceof TraversalParent parent))
        continue;

      final List<?> children;
      if (step instanceof WhereTraversalStep<?>)
        children = ((WhereTraversalStep<?>) step).getLocalChildren();
      else if (step instanceof TraversalFilterStep<?>)
        children = ((TraversalFilterStep<?>) step).getLocalChildren();
      else
        continue;

      if (children.size() != 1)
        continue;

      final List<Step> subSteps = ((Traversal.Admin<?, ?>) children.get(0)).getSteps();
      if (subSteps.size() != 3)
        continue;

      // Check pattern: VertexStep(Edge) → CountGlobalStep → IsStep
      if (!(subSteps.get(0) instanceof VertexStep<?> vertexStep) || !vertexStep.returnsEdge())
        continue;
      if (!(subSteps.get(1) instanceof CountGlobalStep))
        continue;
      if (!(subSteps.get(2) instanceof IsStep<?> isStep))
        continue;

      final String[] edgeLabels = vertexStep.getEdgeLabels();
      // Pinned, as for the vertex steps above (issue #10415)
      final GraphTraversalProvider provider = GraphViewPins.pinAlone(edgeLabels.length == 0
          ? GraphTraversalProviderRegistry.findProvider(db)
          : GraphTraversalProviderRegistry.findProvider(db, edgeLabels));
      if (provider == null)
        continue;

      // Extract predicate from IsStep
      final P<?> rawPredicate = isStep.getPredicate();
      final P<Long> predicate = (P<Long>) rawPredicate;

      // Replace the where/filter step with our O(1) degree check
      final ArcadeEdgeCountFilterStep filterStep = new ArcadeEdgeCountFilterStep(
          traversal, provider, vertexStep.getDirection(), edgeLabels, predicate::test);
      TraversalHelper.copyLabels(step, filterStep, false);
      traversal.removeStep(i);
      traversal.addStep(i, filterStep);
    }
  }

  // No explicit applyPrior() override is needed: ProviderOptimizationStrategy category ordering
  // already guarantees this strategy runs strictly after every OptimizationStrategy, including
  // InlineFilterStrategy (which sometimes removes the need for a TraversalFilterStep) and
  // GValueReductionStrategy (see class javadoc).
  //
  // #5841 previously fixed this strategy's ordering against TinkerPop's CountStrategy with an
  // explicit applyPost(CountStrategy.class), forcing CountStrategy to run AFTER this strategy so
  // applyEdgeCountFilterOptimization's pattern match saw where(outE(X).count().is(pred)) before
  // CountStrategy could rewrite it. That override is gone: it does not type-check once this class
  // moved to ProviderOptimizationStrategy (applyPost's declared type is
  // Set<Class<? extends OptimizationStrategy>>, and CountStrategy is an OptimizationStrategy, not a
  // ProviderOptimizationStrategy), and category ordering makes it moot anyway - category ordering is
  // unconditional and now places CountStrategy (OptimizationStrategy) before this strategy on every
  // JVM, the opposite of what #5841 arranged. The net effect is still deterministic, just reversed:
  // CountStrategy always wins now, so applyEdgeCountFilterOptimization can no longer fire for the
  // where(outE(X).count().is(boundedPredicate)) shape. See issue #5840 and PR #5899 ("Known trade-off")
  // for why this is accepted rather than fixed here.
}
