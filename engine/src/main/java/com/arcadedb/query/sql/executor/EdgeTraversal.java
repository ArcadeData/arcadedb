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

import com.arcadedb.query.sql.parser.Rid;
import com.arcadedb.query.sql.parser.WhereClause;

/**
 * Created by luigidellaquila on 20/09/16.
 */
public class EdgeTraversal {
  boolean out;
  public final PatternEdge edge;
  private      String      leftClass;
  private String      leftCluster;
  private Rid         leftRid;
  private WhereClause leftFilter;

  public EdgeTraversal(PatternEdge edge, boolean out) {
    this.edge = edge;
    this.out = out;
  }

  public void setLeftClass(String leftClass) {
    this.leftClass = leftClass;
  }

  public void setLeftFilter(WhereClause leftFilter) {
    this.leftFilter = leftFilter;
  }

  public String getLeftClass() {
    return leftClass;
  }
  public String getLeftCluster() {
    return leftCluster;
  }

  public Rid getLeftRid() {
    return leftRid;
  }

  public void setLeftCluster(String leftCluster) {
    this.leftCluster = leftCluster;
  }

  public void setLeftRid(Rid leftRid) {
    this.leftRid = leftRid;
  }

  public WhereClause getLeftFilter() {
    return leftFilter;
  }

  @Override
  public String toString() {
    return edge.toString();
  }
}
