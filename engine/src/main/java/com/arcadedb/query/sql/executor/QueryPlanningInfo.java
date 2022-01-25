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

import com.arcadedb.query.sql.parser.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by luigidellaquila on 19/06/17.
 */
public class QueryPlanningInfo {

  protected Timeout timeout;
  boolean distinct = false;
  boolean expand   = false;

  Projection preAggregateProjection;
  Projection aggregateProjection;
  Projection projection             = null;
  Projection projectionAfterOrderBy = null;

  LetClause globalLetClause  = null;
  boolean   globalLetPresent = false;

  LetClause perRecordLetClause = null;

  /**
   * in a sharded execution plan, this maps the single server to the clusters it will be queried for to execute the query.
   */
  Map<String, Set<String>> serverToClusters;

  Map<String, SelectExecutionPlan> distributedFetchExecutionPlans;

  /**
   * set to true when the distributedFetchExecutionPlans are aggregated in the main execution plan
   */
  public boolean distributedPlanCreated = false;

  FromClause     target;
  WhereClause    whereClause;
  List<AndBlock> flattenedWhereClause;
  GroupBy        groupBy;
  OrderBy        orderBy;
  Unwind         unwind;
  Skip           skip;
  Limit          limit;
  boolean        orderApplied          = false;
  boolean        projectionsCalculated = false;
  AndBlock       ridRangeConditions;

  public QueryPlanningInfo copy() {
    //TODO check what has to be copied and what can be just referenced as it is
    QueryPlanningInfo result = new QueryPlanningInfo();
    result.distinct = this.distinct;
    result.expand = this.expand;
    result.preAggregateProjection = this.preAggregateProjection;
    result.aggregateProjection = this.aggregateProjection;
    result.projection = this.projection;
    result.projectionAfterOrderBy = this.projectionAfterOrderBy;
    result.globalLetClause = this.globalLetClause;
    result.globalLetPresent = this.globalLetPresent;
    result.perRecordLetClause = this.perRecordLetClause;
    result.serverToClusters = this.serverToClusters;
    result.distributedPlanCreated = this.distributedPlanCreated;
    result.target = this.target;
    result.whereClause = this.whereClause;
    result.flattenedWhereClause = this.flattenedWhereClause;
    result.groupBy = this.groupBy;
    result.orderBy = this.orderBy;
    result.unwind = this.unwind;
    result.skip = this.skip;
    result.limit = this.limit;
    result.orderApplied = this.orderApplied;
    result.projectionsCalculated = this.projectionsCalculated;
    result.ridRangeConditions = this.ridRangeConditions;

    return result;
  }
}
