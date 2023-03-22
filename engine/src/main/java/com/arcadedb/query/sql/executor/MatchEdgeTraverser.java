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
import com.arcadedb.query.sql.parser.MatchPathItem;
import com.arcadedb.query.sql.parser.Rid;
import com.arcadedb.query.sql.parser.WhereClause;

import java.util.*;

/**
 * Created by luigidellaquila on 23/09/16.
 */
public class MatchEdgeTraverser {
  protected final Result        sourceRecord;
  protected       EdgeTraversal edge;
  protected final MatchPathItem item;

  Iterator<ResultInternal> downstream;

  public MatchEdgeTraverser(final Result lastUpstreamRecord, final EdgeTraversal edge) {
    this.sourceRecord = lastUpstreamRecord;
    this.edge = edge;
    this.item = edge.edge.item;
  }

  public MatchEdgeTraverser(final Result lastUpstreamRecord, final MatchPathItem item) {
    this.sourceRecord = lastUpstreamRecord;
    this.item = item;
  }

  public boolean hasNext(final CommandContext context) {
    init(context);
    return downstream.hasNext();
  }

  public Result next(final CommandContext context) {
    init(context);
    if (!downstream.hasNext()) {
      throw new NoSuchElementException();
    }
    final String endPointAlias = getEndpointAlias();
    final ResultInternal nextR = downstream.next();
    final Document nextElement = nextR.getElement().get();
    final Object prevValue = sourceRecord.getProperty(endPointAlias);
    if (prevValue != null && !equals(prevValue, nextElement)) {
      return null;
    }
    final ResultInternal result = new ResultInternal();
    for (final String prop : sourceRecord.getPropertyNames()) {
      result.setProperty(prop, sourceRecord.getProperty(prop));
    }
    result.setProperty(endPointAlias, toResult(nextElement));
    if (edge.edge.item.getFilter().getDepthAlias() != null) {
      result.setProperty(edge.edge.item.getFilter().getDepthAlias(), nextR.getMetadata("$depth"));
    }
    if (edge.edge.item.getFilter().getPathAlias() != null) {
      result.setProperty(edge.edge.item.getFilter().getPathAlias(), nextR.getMetadata("$matchPath"));
    }
    return result;
  }

  protected boolean equals(Object prevValue, Identifiable nextElement) {
    if (prevValue instanceof Result) {
      prevValue = ((Result) prevValue).getElement().orElse(null);
    }
    if (nextElement instanceof Result) {
      nextElement = ((Result) nextElement).getElement().orElse(null);
    }
    return prevValue != null && prevValue.equals(nextElement);
  }

  protected Object toResult(final Document nextElement) {
    final ResultInternal result = new ResultInternal();
    result.setElement(nextElement);
    return result;
  }

  protected String getStartingPointAlias() {
    return this.edge.edge.out.alias;
  }

  protected String getEndpointAlias() {
    if (this.item != null) {
      return this.item.getFilter().getAlias();
    }
    return this.edge.edge.in.alias;
  }

  protected void init(final CommandContext context) {
    if (downstream == null) {
      Identifiable startingElem = sourceRecord.getElementProperty(getStartingPointAlias());
      if (startingElem instanceof Result) {
        startingElem = ((Result) startingElem).getElement().orElse(null);
      }
      downstream = executeTraversal(context, this.item, startingElem, 0, null).iterator();
    }
  }

  protected Iterable<ResultInternal> executeTraversal(final CommandContext iCommandContext, final MatchPathItem item, final Identifiable startingPoint,
      final int depth, final List<Identifiable> pathToHere) {

    WhereClause filter = null;
    WhereClause whileCondition = null;
    Integer maxDepth = null;
    String className = null;
    Integer clusterId = null;
    Rid targetRid = null;
    if (item.getFilter() != null) {
      filter = getTargetFilter(item);
      whileCondition = item.getFilter().getWhileCondition();
      maxDepth = item.getFilter().getMaxDepth();
      className = targetClassName(item, iCommandContext);
      final String clusterName = targetClusterName(item, iCommandContext);
      if (clusterName != null) {
        clusterId = iCommandContext.getDatabase().getSchema().getBucketByName(clusterName).getId();
      }
      targetRid = targetRid(item, iCommandContext);
    }

    final Iterable<ResultInternal> result;

    if (whileCondition == null && maxDepth == null) { // in this case starting point is not returned and only one level depth is
      // evaluated

      final Iterable<ResultInternal> queryResult = traversePatternEdge(startingPoint, iCommandContext);
      final WhereClause theFilter = filter;
      final String theClassName = className;
      final Integer theClusterId = clusterId;
      final Rid theTargetRid = targetRid;
      result = () -> {
        final Iterator<ResultInternal> iter = queryResult.iterator();

        return new Iterator() {

          private ResultInternal nextElement = null;

          @Override
          public boolean hasNext() {
            if (nextElement == null) {
              fetchNext();
            }
            return nextElement != null;
          }

          @Override
          public Object next() {
            if (nextElement == null) {
              fetchNext();
            }
            if (nextElement == null) {
              throw new NoSuchElementException();
            }
            final ResultInternal res = nextElement;
            nextElement = null;
            return res;
          }

          public void fetchNext() {
            final Object previousMatch = iCommandContext.getVariable("currentMatch");
            final ResultInternal matched = (ResultInternal) iCommandContext.getVariable("matched");
            if (matched != null) {
              matched.setProperty(getStartingPointAlias(), sourceRecord.getProperty(getStartingPointAlias()));
            }
            while (iter.hasNext()) {
              final ResultInternal next = iter.next();
              final Document elem = next.toElement();
              iCommandContext.setVariable("currentMatch", elem);
              if (matchesFilters(iCommandContext, theFilter, elem) && matchesClass(theClassName, elem) && matchesCluster(theClusterId, elem) && matchesRid(
                  iCommandContext, theTargetRid, elem)) {
                nextElement = next;
                break;
              }
            }
            iCommandContext.setVariable("currentMatch", previousMatch);
          }
        };
      };

    } else { // in this case also zero level (starting point) is considered and traversal depth is
      // given by the while condition
      result = new ArrayList<>();
      iCommandContext.setVariable("depth", depth);
      final Object previousMatch = iCommandContext.getVariable("currentMatch");
      iCommandContext.setVariable("currentMatch", startingPoint);

      if (matchesFilters(iCommandContext, filter, startingPoint) && matchesClass(className, startingPoint) && matchesCluster(clusterId, startingPoint)
          && matchesRid(iCommandContext, targetRid, startingPoint)) {
        final ResultInternal rs = new ResultInternal((Document) startingPoint.getRecord());
        // set traversal depth in the metadata
        rs.setMetadata("$depth", depth);
        // set traversal path in the metadata
        rs.setMetadata("$matchPath", pathToHere == null ? Collections.emptyList() : pathToHere);
        // add the result to the list
        ((List) result).add(rs);
      }

      if ((maxDepth == null || depth < maxDepth) && (whileCondition == null || whileCondition.matchesFilters(startingPoint, iCommandContext))) {

        final Iterable<ResultInternal> queryResult = traversePatternEdge(startingPoint, iCommandContext);

        for (final ResultInternal origin : queryResult) {
          //          if(origin.equals(startingPoint)){
          //            continue;
          //          }
          // TODO consider break strategies (eg. re-traverse nodes)

          final List<Identifiable> newPath = new ArrayList<>();
          if (pathToHere != null) {
            newPath.addAll(pathToHere);
          }

          final Document elem = origin.toElement();
          newPath.add(elem.getIdentity());

          final Iterable<ResultInternal> subResult = executeTraversal(iCommandContext, item, elem, depth + 1, newPath);
          if (subResult instanceof Collection) {
            ((List) result).addAll((Collection<? extends ResultInternal>) subResult);
          } else {
            for (final ResultInternal i : subResult) {
              ((List) result).add(i);
            }
          }
        }
      }
      iCommandContext.setVariable("currentMatch", previousMatch);
    }
    return result;
  }

  protected WhereClause getTargetFilter(final MatchPathItem item) {
    return item.getFilter().getFilter();
  }

  protected String targetClassName(final MatchPathItem item, final CommandContext iCommandContext) {
    return item.getFilter().getTypeName(iCommandContext);
  }

  protected String targetClusterName(final MatchPathItem item, final CommandContext iCommandContext) {
    return item.getFilter().getBucketName(iCommandContext);
  }

  protected Rid targetRid(final MatchPathItem item, final CommandContext iCommandContext) {
    return item.getFilter().getRid(iCommandContext);
  }

  private boolean matchesClass(final String className, final Identifiable origin) {
    if (className == null) {
      return true;
    }
    Document element = null;
    if (origin instanceof Document) {
      element = (Document) origin;
    } else {
      final Object record = origin.getRecord();
      if (record instanceof Document) {
        element = (Document) record;
      }
    }
    if (element != null) {
      final Object typez = element.getTypeName();
      if (typez == null) {
        return false;
      }
      return typez.equals(className);
    }
    return false;
  }

  private boolean matchesCluster(final Integer bucketId, final Identifiable origin) {
    if (bucketId == null) {
      return true;
    }
    if (origin == null) {
      return false;
    }

    if (origin.getIdentity() == null) {
      return false;
    }
    return bucketId.equals(origin.getIdentity().getBucketId());
  }

  private boolean matchesRid(final CommandContext iCommandContext, final Rid rid, final Identifiable origin) {
    if (rid == null) {
      return true;
    }
    if (origin == null) {
      return false;
    }

    if (origin.getIdentity() == null) {
      return false;
    }
    return origin.getIdentity().equals(rid.toRecordId(origin, iCommandContext));
  }

  protected boolean matchesFilters(final CommandContext iCommandContext, final WhereClause filter, final Identifiable origin) {
    return filter == null || filter.matchesFilters(origin, iCommandContext);
  }

  //TODO refactor this method to receive the item.

  protected Iterable<ResultInternal> traversePatternEdge(final Identifiable startingPoint, final CommandContext iCommandContext) {

    Iterable possibleResults = null;
    if (this.item.getFilter() != null) {
      final String alias = getEndpointAlias();
      final Object matchedNodes = iCommandContext.getVariable(MatchPrefetchStep.PREFETCHED_MATCH_ALIAS_PREFIX + alias);
      if (matchedNodes != null) {
        if (matchedNodes instanceof Iterable) {
          possibleResults = (Iterable) matchedNodes;
        } else {
          possibleResults = Collections.singleton(matchedNodes);
        }
      }
    }

    final Object prevCurrent = iCommandContext.getVariable("current");
    iCommandContext.setVariable("current", startingPoint);
    Object qR;
    try {
      qR = this.item.getMethod().execute(startingPoint, possibleResults, iCommandContext);
    } finally {
      iCommandContext.setVariable("current", prevCurrent);
    }

    if (qR == null) {
      return Collections.emptyList();
    }
    if (qR instanceof Document) {
      return Collections.singleton(new ResultInternal((Document) qR));
    }
    if (qR instanceof Iterable) {
      final Iterable iterable = (Iterable) qR;
      final List<ResultInternal> result = new ArrayList<>();
      for (final Object o : iterable) {
        if (o instanceof Document) {
          result.add(new ResultInternal((Document) o));
        } else if (o instanceof ResultInternal) {
          result.add((ResultInternal) o);
        } else if (o == null) {
        } else {
          throw new UnsupportedOperationException();
        }
      }
      return result;
    }
    return Collections.emptyList();
  }

}
