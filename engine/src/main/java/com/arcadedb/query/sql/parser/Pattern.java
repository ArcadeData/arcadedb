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
package com.arcadedb.query.sql.parser;

import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.PatternEdge;
import com.arcadedb.query.sql.executor.PatternNode;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by luigidellaquila on 28/07/15.
 */
public class Pattern {
  public Map<String, PatternNode> aliasToNode = new LinkedHashMap<>();
  public int                      numOfEdges  = 0;

  public void addExpression(MatchExpression expression) {
    PatternNode originNode = getOrCreateNode(expression.origin);

    for (MatchPathItem item : expression.items) {
      String nextAlias = item.filter.getAlias();
      PatternNode nextNode = getOrCreateNode(item.filter);

      numOfEdges += originNode.addEdge(item, nextNode);
      originNode = nextNode;
    }
  }

  private PatternNode getOrCreateNode(MatchFilter origin) {
    PatternNode originNode = get(origin.getAlias());
    if (originNode == null) {
      originNode = new PatternNode();
      originNode.alias = origin.getAlias();
      aliasToNode.put(originNode.alias, originNode);
    }
    if (origin.isOptional()) {
      originNode.optional = true;
    }
    return originNode;
  }

  public PatternNode get(String alias) {
    return aliasToNode.get(alias);
  }

  public int getNumOfEdges() {
    return numOfEdges;
  }

  public void validate() {
    for (PatternNode node : this.aliasToNode.values()) {
      if (node.isOptionalNode()) {
        if (node.out.size() > 0) {
          throw new CommandSQLParsingException(
              "In current MATCH version, optional nodes are allowed only on right terminal nodes, eg. {} --> {optional:true} is allowed, {optional:true} <-- {} is not. ");
        }
        if (node.in.size() == 0) {
          throw new CommandSQLParsingException("In current MATCH version, optional nodes must have at least one incoming pattern edge");
        }
        //        if (node.in.size() != 1) {
        //          throw new OCommandSQLParsingException("In current MATCH version, optional nodes are allowed only as single terminal nodes. ");
        //        }
      }
    }
  }

  /**
   * splits this pattern into multiple
   *
   * @return
   */
  public List<Pattern> getDisjointPatterns() {
    Map<PatternNode, String> reverseMap = new IdentityHashMap<>(this.aliasToNode.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey)));

    List<Pattern> result = new ArrayList<>();
    while (!reverseMap.isEmpty()) {
      Pattern pattern = new Pattern();
      result.add(pattern);
      Map.Entry<PatternNode, String> nextNode = reverseMap.entrySet().iterator().next();
      Set<PatternNode> toVisit = new HashSet<>();
      toVisit.add(nextNode.getKey());
      while (toVisit.size() > 0) {
        PatternNode currentNode = toVisit.iterator().next();
        toVisit.remove(currentNode);
        if (reverseMap.containsKey(currentNode)) {
          pattern.aliasToNode.put(reverseMap.get(currentNode), currentNode);
          reverseMap.remove(currentNode);
          for (PatternEdge x : currentNode.out) {
            toVisit.add(x.in);
          }
          for (PatternEdge x : currentNode.in) {
            toVisit.add(x.out);
          }
        }
      }
      pattern.recalculateNumOfEdges();
    }
    return result;
  }

  private void recalculateNumOfEdges() {
    Map<PatternEdge, PatternEdge> edges = new IdentityHashMap<>();
    for (PatternNode node : this.aliasToNode.values()) {
      for (PatternEdge edge : node.out) {
        edges.put(edge, edge);
      }
      for (PatternEdge edge : node.in) {
        edges.put(edge, edge);
      }
    }
    this.numOfEdges = edges.size();
  }

  public Map<String, PatternNode> getAliasToNode() {
    return aliasToNode;
  }

  public void setAliasToNode(Map<String, PatternNode> aliasToNode) {
    this.aliasToNode = aliasToNode;
  }

  public void setNumOfEdges(int numOfEdges) {
    this.numOfEdges = numOfEdges;
  }
}
