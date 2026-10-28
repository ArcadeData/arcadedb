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
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.GraphViewPins;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.serializer.BinaryComparator;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.AbstractLambdaTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ConstantStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LabelStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalFlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TraversalSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;

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

  // WHAT A STEP IS KNOWN TO BE FED OR TO PRODUCE, AS FAR AS THE ALWAYS-EMPTY REWRITE NEEDS TO KNOW IT (SEE readOnlyOutput()): A STEP
  // THAT READS AN ELEMENT RAISES A ClassCastException OR AN IllegalStateException WHEN IT IS FED ANYTHING ELSE
  private static final int NOT_READ_ONLY = -1;
  private static final int ANY           = 0;
  private static final int VERTEX        = 1;
  private static final int EDGE          = 2;
  // ELEMENT IS "A VERTEX OR AN EDGE, NOT KNOWN WHICH": A STEP THAT NEEDS ONE OF THE TWO (current != VERTEX) REJECTS IT TOO, ON PURPOSE
  private static final int ELEMENT       = VERTEX | EDGE;

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
            // A 'bucket:' LABEL IS NEVER ANSWERED FROM AN INDEX: THE INDEX SPANS THE WHOLE TYPE, NOT THE ONE BUCKET. THE LABEL IS
            // RESOLVED BY THE SAME ArcadeLabels THE STEPS BUILT BELOW USE (#10763)
            final DocumentType labelType = ArcadeLabels.bucketName(typeNameToMatch) == null ?
                ArcadeLabels.typeOf(graph.database.getSchema(), typeNameToMatch, prevStepGraph.getReturnClass()) :
                null;
            final boolean kindMatches = labelType != null;

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
                final TypeIndex index = labelType.getPolymorphicIndexByProperties(key);
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
   * Only when nothing in front of the filter, and nothing in its child, has an effect of its own ({@link #readOnlyOutput}): the
   * {@code limit(0)} stops before the steps in front run even once, and that also stops what they would have done, and TinkerPop's
   * answer includes it. A mutation, a side effect that is read back ({@code aggregate('x')...cap('x')}), a lambda, a service call, an
   * exception ({@code math('1/0')}, {@code values()} over a number) and any step not known to do nothing but read all keep the filter
   * where it is.
   */
  private static void applyAlwaysEmptyFilterOptimization(final Traversal.Admin<?, ?> traversal) {
    final List<Step> steps = traversal.getSteps();
    int input = NOT_READ_ONLY;
    for (int i = 0; i < steps.size(); i++) {
      final Step<?, ?> step = steps.get(i);
      if (!(step instanceof TraversalFilterStep<?> || step instanceof WhereTraversalStep<?>))
        continue;

      final List<?> children = ((TraversalParent) step).getLocalChildren();
      if (children.size() != 1 || !neverEmitsAnything((Traversal.Admin<?, ?>) children.get(0)))
        continue;

      // THE STEPS IN FRONT OF THE FILTER, AND WHAT THEY FEED IT. THE CHILD TOO: ITS STEPS IN FRONT OF THE limit(0) RUN ONCE FOR EVERY
      // TRAVERSER THE FILTER IS OFFERED, AND THE FILTER IS GONE
      if (input == NOT_READ_ONLY)
        input = inputOf(traversal);
      final int fed = readOnlyOutput(steps.subList(0, i), input);
      if (fed == NOT_READ_ONLY || readOnlyOutput(((Traversal.Admin<?, ?>) children.get(0)).getSteps(), fed) == NOT_READ_ONLY)
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
   * drops the traversal has to know they have no effect ({@link #readOnlyOutput}).
   */
  private static boolean neverEmitsAnything(final Traversal.Admin<?, ?> child) {
    final List<? extends Step> childSteps = child.getSteps();
    return !childSteps.isEmpty() && childSteps.get(childSteps.size() - 1) instanceof RangeGlobalStep<?> range
        && range.getHighRange() == 0L;
  }

  /**
   * Whether running the steps does nothing but read the graph, so that running them for fewer traversers changes nothing but the
   * work, and what they produce: the kind of traverser that comes out of the last one ({@link #VERTEX}, {@link #EDGE},
   * {@link #ELEMENT} or {@link #ANY}), or {@link #NOT_READ_ONLY}.
   * <p>
   * An effect is the default and doing nothing but read the exception (issue #10587): raising an exception is an effect too, the one
   * the rewrite would never let the step raise, and almost every step can raise one, over the data it is fed ({@code math()} over a
   * string, {@code values()} over a number) as much as over its own arguments ({@code math('1/0')}). So the steps are named one by one,
   * each with what it must be fed not to raise, and any other step, a future one of TinkerPop or of this module included, is assumed to
   * have an effect. The children of a step are asked too, fed what the step itself is fed.
   */
  private static int readOnlyOutput(final List<? extends Step> steps, final int input) {
    int current = input;
    for (final Step<?, ?> step : steps) {
      if (step instanceof TraversalParent parent && !childrenAreReadOnly(parent, current))
        return NOT_READ_ONLY;

      final Class<?> c = step.getClass();
      if (c == GraphStep.class) {
        // A GraphStep IGNORES WHAT IT IS FED, BUT NOT ITS IDS: A STRING THAT IS NOT A RID RAISES
        if (!idsAreRids(((GraphStep<?, ?>) step).getIds()))
          return NOT_READ_ONLY;
        current = ((GraphStep<?, ?>) step).returnsVertex() ? VERTEX : EDGE;
      } else if (c == ArcadeFilterByTypeStep.class || c == ArcadeFilterByIndexStep.class)
        // THE REPLACEMENTS OF A GraphStep AND ITS has(): THEY IGNORE WHAT THEY ARE FED
        current = ELEMENT;
      else if (c == VertexStep.class) {
        if (current != VERTEX)
          return NOT_READ_ONLY;
        current = ((VertexStep<?>) step).returnsVertex() ? VERTEX : EDGE;
      } else if (c == EdgeVertexStep.class) {
        // NOT otherV(): IT READS THE PATH FOR THE VERTEX THE EDGE WAS REACHED FROM, AND RAISES ON AN EDGE THAT WAS NOT REACHED FROM ONE
        if (current != EDGE)
          return NOT_READ_ONLY;
        current = VERTEX;
      } else if (c == ArcadeGAVVertexStep.class || c == ArcadeGAVFusedStep.class || c == ArcadeEdgeCountFilterStep.class) {
        if (current != VERTEX)
          return NOT_READ_ONLY;
      } else if (c == HasStep.class) {
        if (current == ANY)
          return NOT_READ_ONLY;
      } else if (c == PropertiesStep.class || c == IdStep.class || c == LabelStep.class) {
        if (current == ANY)
          return NOT_READ_ONLY;
        current = ANY;
      } else if (c == DedupGlobalStep.class) {
        // dedup('a') READS THE PATH, AND A LABEL THE PATH DOES NOT HOLD RAISES
        if (!((DedupGlobalStep<?>) step).getScopeKeys().isEmpty())
          return NOT_READ_ONLY;
      } else if (c == WhereTraversalStep.WhereStartStep.class || c == WhereTraversalStep.WhereEndStep.class) {
        // THE SAME FOR where()'S OWN ENDS: ONE THAT NAMES A LABEL READS THE PATH
        if (!((Scoping) step).getScopeKeys().isEmpty())
          return NOT_READ_ONLY;
      } else if (c == InjectStep.class || c == ConstantStep.class || c == CountGlobalStep.class || c == ArcadeCountGlobalStep.class)
        current = ANY;
      else if (c == StartStep.class) {
        if (((StartStep<?>) step).getStart() != null)
          current = ANY;
      } else if (c != IdentityStep.class && c != NoOpBarrierStep.class && c != RangeGlobalStep.class && c != IsStep.class
          && c != TraversalFilterStep.class && c != WhereTraversalStep.class && c != NotStep.class)
        return NOT_READ_ONLY;
    }
    return current;
  }

  private static boolean childrenAreReadOnly(final TraversalParent parent, final int input) {
    for (final Traversal.Admin<?, ?> child : parent.getLocalChildren())
      if (child instanceof AbstractLambdaTraversal || readOnlyOutput(child.getSteps(), input) == NOT_READ_ONLY)
        return false;
    for (final Traversal.Admin<?, ?> child : parent.getGlobalChildren())
      if (child instanceof AbstractLambdaTraversal || readOnlyOutput(child.getSteps(), input) == NOT_READ_ONLY)
        return false;
    return true;
  }

  private static boolean idsAreRids(final Object[] ids) {
    for (final Object id : ids) {
      if (id instanceof String string) {
        try {
          new RID(string);
        } catch (final RuntimeException e) {
          return false;
        }
      } else if (id instanceof Element element) {
        if (!(element.id() instanceof RID))
          return false;
      }
      // ANY OTHER ID, A RID INCLUDED, IS LOOKED UP OR SKIPPED BY ArcadeGraph.vertices()/edges() WITHOUT RAISING
    }
    return true;
  }

  /**
   * What the traversal is fed: what flows into the step that owns it when that step feeds its children its own input (a filter, a
   * union, a local()...), and {@link #ANY} for the traversal at the root or any other child.
   */
  private static int inputOf(final Traversal.Admin<?, ?> traversal) {
    final Step<?, ?> parent = traversal.getParent().asStep();
    if (parent instanceof EmptyStep)
      return ANY;
    final Class<?> c = parent.getClass();
    if (c != TraversalFilterStep.class && c != NotStep.class && c != UnionStep.class && c != LocalStep.class
        && c != TraversalMapStep.class && c != TraversalFlatMapStep.class && c != TraversalSideEffectStep.class)
      return ANY;

    final Traversal.Admin<?, ?> owner = parent.getTraversal();
    final List<Step> ownerSteps = owner.getSteps();
    // BY IDENTITY: TWO STEPS OF THE SAME SHAPE ARE equals()
    int index = -1;
    for (int i = 0; i < ownerSteps.size() && index < 0; i++)
      if (ownerSteps.get(i) == parent)
        index = i;
    if (index < 0)
      return ANY;
    final int output = readOnlyOutput(ownerSteps.subList(0, index), inputOf(owner));
    return output == NOT_READ_ONLY ? ANY : output;
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
