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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.parser.MatchPathItem;
import com.arcadedb.query.sql.parser.MatchPathItemFirst;
import com.arcadedb.query.sql.parser.MethodCall;
import com.arcadedb.query.sql.parser.MultiMatchPathItem;
import com.arcadedb.query.sql.parser.WhereClause;

import java.util.*;

/**
 * Created by luigidellaquila on 14/10/16.
 */
public class MatchMultiEdgeTraverser extends MatchEdgeTraverser {
  public MatchMultiEdgeTraverser(final Result lastUpstreamRecord, final EdgeTraversal edge) {
    super(lastUpstreamRecord, edge);
  }

  protected Iterable<ResultInternal> traversePatternEdge(final Identifiable startingPoint, final CommandContext iCommandContext) {

    final Iterable possibleResults = null;
    //    if (this.edge.edge.item.getFilter() != null) {
    //      String alias = this.edge.edge.item.getFilter().getAlias();
    //      Object matchedNodes = iCommandContext.getVariable(MatchPrefetchStep.PREFETCHED_MATCH_ALIAS_PREFIX + alias);
    //      if (matchedNodes != null) {
    //        if (matchedNodes instanceof Iterable) {
    //          possibleResults = (Iterable) matchedNodes;
    //        } else {
    //          possibleResults = Collections.singleton(matchedNodes);
    //        }
    //      }
    //    }

    final MultiMatchPathItem item = (MultiMatchPathItem) this.item;
    List<ResultInternal> result = new ArrayList<>();

    List<Object> nextStep = new ArrayList<>();
    nextStep.add(startingPoint);

    final Object oldCurrent = iCommandContext.getVariable("current");
    for (final MatchPathItem sub : item.getItems()) {
      final List<ResultInternal> rightSide = new ArrayList<>();
      for (final Object o : nextStep) {
        final WhereClause whileCond = sub.getFilter() == null ? null : sub.getFilter().getWhileCondition();

        MethodCall method = sub.getMethod();
        if (sub instanceof MatchPathItemFirst) {
          method = ((MatchPathItemFirst) sub).getFunction().toMethod();
        }

        if (whileCond != null) {
          Object current = o;
          if (current instanceof Result) {
            current = ((Result) current).getElement().orElse(null);
          }
          final MatchEdgeTraverser subtraverser = new MatchEdgeTraverser(null, sub);
          subtraverser.executeTraversal(iCommandContext, sub, (Identifiable) current, 0, null).forEach(x -> rightSide.add(x));

        } else {
          iCommandContext.setVariable("current", o);
          final Object nextSteps = method.execute(o, possibleResults, iCommandContext);
          if (nextSteps instanceof Collection) {
            ((Collection) nextSteps).stream().map(x -> toOResultInternal(x)).filter(Objects::nonNull).forEach(i -> rightSide.add((ResultInternal) i));
          } else if (nextSteps instanceof Document) {
            rightSide.add(new ResultInternal((Document) nextSteps));
          } else if (nextSteps instanceof ResultInternal) {
            rightSide.add((ResultInternal) nextSteps);
          } else if (nextSteps instanceof Iterable) {
            for (final Object step : (Iterable) nextSteps) {
              final ResultInternal converted = toOResultInternal(step);
              if (converted != null) {
                rightSide.add(converted);
              }
            }
          } else if (nextSteps instanceof Iterator) {
            final Iterator iterator = (Iterator) nextSteps;
            while (iterator.hasNext()) {
              final ResultInternal converted = toOResultInternal(iterator.next());
              if (converted != null) {
                rightSide.add(converted);
              }
            }
          }
        }
      }
      nextStep = (List) rightSide;
      result = rightSide;
    }

    iCommandContext.setVariable("current", oldCurrent);
    //    return (qR instanceof Iterable) ? (Iterable) qR : Collections.singleton((PIdentifiable) qR);
    return result;
  }

  private ResultInternal toOResultInternal(final Object x) {
    if (x instanceof ResultInternal) {
      return (ResultInternal) x;
    }
    if (x instanceof Document) {
      return new ResultInternal((Document) x);
    }
    throw new CommandExecutionException("Cannot execute traversal on " + x);
  }
}
