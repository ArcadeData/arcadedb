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

  public MatchEdgeTraverser(Result lastUpstreamRecord, EdgeTraversal edge) {
    this.sourceRecord = lastUpstreamRecord;
    this.edge = edge;
    this.item = edge.edge.item;
  }

  public MatchEdgeTraverser(Result lastUpstreamRecord, MatchPathItem item) {
    this.sourceRecord = lastUpstreamRecord;
    this.item = item;
  }

  public boolean hasNext(CommandContext ctx) {
    init(ctx);
    return downstream.hasNext();
  }

  public Result next(CommandContext ctx) {
    init(ctx);
    if (!downstream.hasNext()) {
      throw new IllegalStateException();
    }
    String endPointAlias = getEndpointAlias();
    ResultInternal nextR = downstream.next();
    Document nextElement = nextR.getElement().get();
    Object prevValue = sourceRecord.getProperty(endPointAlias);
    if (prevValue != null && !equals(prevValue, nextElement)) {
      return null;
    }
    ResultInternal result = new ResultInternal();
    for (String prop : sourceRecord.getPropertyNames()) {
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

  protected Object toResult(Document nextElement) {
    ResultInternal result = new ResultInternal();
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

  protected void init(CommandContext ctx) {
    if (downstream == null) {
      Identifiable startingElem = sourceRecord.getElementProperty(getStartingPointAlias());
      if (startingElem instanceof Result) {
        startingElem = ((Result) startingElem).getElement().orElse(null);
      }
      downstream = executeTraversal(ctx, this.item, startingElem, 0, null).iterator();
    }
  }

  protected Iterable<ResultInternal> executeTraversal(CommandContext iCommandContext, MatchPathItem item, Identifiable startingPoint, int depth,
      List<Identifiable> pathToHere) {

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
      String clusterName = targetClusterName(item, iCommandContext);
      if (clusterName != null) {
        clusterId = iCommandContext.getDatabase().getSchema().getBucketByName(clusterName).getId();
      }
      targetRid = targetRid(item, iCommandContext);
    }

    Iterable<ResultInternal> result;

    if (whileCondition == null && maxDepth == null) { // in this case starting point is not returned and only one level depth is
      // evaluated

      Iterable<ResultInternal> queryResult = traversePatternEdge(startingPoint, iCommandContext);
      final WhereClause theFilter = filter;
      final String theClassName = className;
      final Integer theClusterId = clusterId;
      final Rid theTargetRid = targetRid;
      result = () -> {
        Iterator<ResultInternal> iter = queryResult.iterator();

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
              throw new IllegalStateException();
            }
            ResultInternal res = nextElement;
            nextElement = null;
            return res;
          }

          public void fetchNext() {
            Object previousMatch = iCommandContext.getVariable("$currentMatch");
            ResultInternal matched = (ResultInternal) iCommandContext.getVariable("matched");
            if (matched != null) {
              matched.setProperty(getStartingPointAlias(), sourceRecord.getProperty(getStartingPointAlias()));
            }
            while (iter.hasNext()) {
              ResultInternal next = iter.next();
              Document elem = next.toElement();
              iCommandContext.setVariable("$currentMatch", elem);
              if (matchesFilters(iCommandContext, theFilter, elem) && matchesClass(iCommandContext, theClassName, elem) && matchesCluster(iCommandContext,
                  theClusterId, elem) && matchesRid(iCommandContext, theTargetRid, elem)) {
                nextElement = next;
                break;
              }
            }
            iCommandContext.setVariable("$currentMatch", previousMatch);
          }
        };
      };

    } else { // in this case also zero level (starting point) is considered and traversal depth is
      // given by the while condition
      result = new ArrayList<>();
      iCommandContext.setVariable("$depth", depth);
      Object previousMatch = iCommandContext.getVariable("$currentMatch");
      iCommandContext.setVariable("$currentMatch", startingPoint);

      if (matchesFilters(iCommandContext, filter, startingPoint) && matchesClass(iCommandContext, className, startingPoint) && matchesCluster(iCommandContext,
          clusterId, startingPoint) && matchesRid(iCommandContext, targetRid, startingPoint)) {
        ResultInternal rs = new ResultInternal((Document) startingPoint.getRecord());
        // set traversal depth in the metadata
        rs.setMetadata("$depth", depth);
        // set traversal path in the metadata
        rs.setMetadata("$matchPath", pathToHere == null ? Collections.EMPTY_LIST : pathToHere);
        // add the result to the list
        ((List) result).add(rs);
      }

      if ((maxDepth == null || depth < maxDepth) && (whileCondition == null || whileCondition.matchesFilters(startingPoint, iCommandContext))) {

        Iterable<ResultInternal> queryResult = traversePatternEdge(startingPoint, iCommandContext);

        for (ResultInternal origin : queryResult) {
          //          if(origin.equals(startingPoint)){
          //            continue;
          //          }
          // TODO consider break strategies (eg. re-traverse nodes)

          List<Identifiable> newPath = new ArrayList<>();
          if (pathToHere != null) {
            newPath.addAll(pathToHere);
          }

          Document elem = origin.toElement();
          newPath.add(elem.getIdentity());

          Iterable<ResultInternal> subResult = executeTraversal(iCommandContext, item, elem, depth + 1, newPath);
          if (subResult instanceof Collection) {
            ((List) result).addAll((Collection<? extends ResultInternal>) subResult);
          } else {
            for (ResultInternal i : subResult) {
              ((List) result).add(i);
            }
          }
        }
      }
      iCommandContext.setVariable("$currentMatch", previousMatch);
    }
    return result;
  }

  protected WhereClause getTargetFilter(MatchPathItem item) {
    return item.getFilter().getFilter();
  }

  protected String targetClassName(MatchPathItem item, CommandContext iCommandContext) {
    return item.getFilter().getTypeName(iCommandContext);
  }

  protected String targetClusterName(MatchPathItem item, CommandContext iCommandContext) {
    return item.getFilter().getBucketName(iCommandContext);
  }

  protected Rid targetRid(MatchPathItem item, CommandContext iCommandContext) {
    return item.getFilter().getRid(iCommandContext);
  }

  private boolean matchesClass(CommandContext iCommandContext, String className, Identifiable origin) {
    if (className == null) {
      return true;
    }
    Document element = null;
    if (origin instanceof Document) {
      element = (Document) origin;
    } else {
      Object record = origin.getRecord();
      if (record instanceof Document) {
        element = (Document) record;
      }
    }
    if (element != null) {
      Object typez = element.getTypeName();
      if (typez == null) {
        return false;
      }
      return typez.equals(className);
    }
    return false;
  }

  private boolean matchesCluster(CommandContext iCommandContext, Integer bucketId, Identifiable origin) {
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

  private boolean matchesRid(CommandContext iCommandContext, Rid rid, Identifiable origin) {
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

  protected boolean matchesFilters(CommandContext iCommandContext, WhereClause filter, Identifiable origin) {
    return filter == null || filter.matchesFilters(origin, iCommandContext);
  }

  //TODO refactor this method to receive the item.

  protected Iterable<ResultInternal> traversePatternEdge(Identifiable startingPoint, CommandContext iCommandContext) {

    Iterable possibleResults = null;
    if (this.item.getFilter() != null) {
      String alias = getEndpointAlias();
      Object matchedNodes = iCommandContext.getVariable(MatchPrefetchStep.PREFETCHED_MATCH_ALIAS_PREFIX + alias);
      if (matchedNodes != null) {
        if (matchedNodes instanceof Iterable) {
          possibleResults = (Iterable) matchedNodes;
        } else {
          possibleResults = Collections.singleton(matchedNodes);
        }
      }
    }

    Object prevCurrent = iCommandContext.getVariable("$current");
    iCommandContext.setVariable("$current", startingPoint);
    Object qR;
    try {
      qR = this.item.getMethod().execute(startingPoint, possibleResults, iCommandContext);
    } finally {
      iCommandContext.setVariable("$current", prevCurrent);
    }

    if (qR == null) {
      return Collections.EMPTY_LIST;
    }
    if (qR instanceof Document) {
      return Collections.singleton(new ResultInternal((Document) qR));
    }
    if (qR instanceof Iterable) {
      Iterable iterable = (Iterable) qR;
      List<ResultInternal> result = new ArrayList<>();
      for (Object o : iterable) {
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
    return Collections.EMPTY_LIST;
  }

}
