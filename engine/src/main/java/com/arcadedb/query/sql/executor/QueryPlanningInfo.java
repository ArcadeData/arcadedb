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

import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.GroupBy;
import com.arcadedb.query.sql.parser.LetClause;
import com.arcadedb.query.sql.parser.Limit;
import com.arcadedb.query.sql.parser.OrderBy;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.Skip;
import com.arcadedb.query.sql.parser.Timeout;
import com.arcadedb.query.sql.parser.Unwind;
import com.arcadedb.query.sql.parser.WhereClause;

import java.util.*;

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
  Projection projectionAfterUnwind  = null;

  LetClause globalLetClause  = null;
  boolean   globalLetPresent = false;

  LetClause perRecordLetClause = null;

  Set<String> buckets;

  SelectExecutionPlan fetchExecutionPlan;

  /**
   * set to true when the distributedFetchExecutionPlans are aggregated in the main execution plan
   */
  public boolean planCreated = false;

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

  /**
   * Set of property names required by this query (from SELECT, WHERE, GROUP BY, ORDER BY).
   * When null, all properties are needed (e.g., SELECT *).
   * Used for column projection pushdown to avoid deserializing unused properties.
   */
  Set<String> projectedProperties;

  public QueryPlanningInfo copy() {
    //TODO check what has to be copied and what can be just referenced as it is
    final QueryPlanningInfo result = new QueryPlanningInfo();
    result.distinct = this.distinct;
    result.expand = this.expand;
    result.preAggregateProjection = this.preAggregateProjection;
    result.aggregateProjection = this.aggregateProjection;
    result.projection = this.projection;
    result.projectionAfterOrderBy = this.projectionAfterOrderBy;
    result.projectionAfterUnwind = this.projectionAfterUnwind;
    result.globalLetClause = this.globalLetClause;
    result.globalLetPresent = this.globalLetPresent;
    result.perRecordLetClause = this.perRecordLetClause;
    result.buckets = this.buckets;
    result.planCreated = this.planCreated;
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
    result.projectedProperties = this.projectedProperties;

    return result;
  }
}
