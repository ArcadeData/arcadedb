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

import java.util.ArrayList;
import java.util.List;

/**
 * This class is used by the query planner to split projections in three parts:
 * <ul>
 * <li>pre-aggregate projections</li>
 * <li>aggregate projections</li>
 * <li>post-aggregate projections</li>
 * </ul>
 * <p>
 * An example:
 * {@literal
 *   select max(a + b) + (max(b + c * 2) + 1 + 2) * 3 as foo, max(d) + max(e), f from " + className
 * }
 * will become
 * {@literal
 * <p>
 *   a + b AS _$$$OALIAS$$_1, b + c * 2 AS _$$$OALIAS$$_3, d AS _$$$OALIAS$$_5, e AS _$$$OALIAS$$_7, f
 * <p>
 *   max(_$$$OALIAS$$_1) AS _$$$OALIAS$$_0, max(_$$$OALIAS$$_3) AS _$$$OALIAS$$_2, max(_$$$OALIAS$$_5) AS _$$$OALIAS$$_4, max(_$$$OALIAS$$_7) AS _$$$OALIAS$$_6, f
 * <p>
 *   _$$$OALIAS$$_0 + (_$$$OALIAS$$_2 + 1 + 2) * 3 AS `foo`, _$$$OALIAS$$_4 + _$$$OALIAS$$_6 AS `max(d) + max(e)`, f
 * }
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class AggregateProjectionSplit {

  protected static final String GENERATED_ALIAS_PREFIX = "_$$$OALIAS$$_";
  protected              int    nextAliasId            = 0;

  protected List<ProjectionItem> preAggregate = new ArrayList<>();
  protected List<ProjectionItem> aggregate    = new ArrayList<>();

  public Identifier getNextAlias() {
    Identifier result = new Identifier(GENERATED_ALIAS_PREFIX + (nextAliasId++));
    result.internalAlias = true;
    return result;
  }

  public List<ProjectionItem> getPreAggregate() {
    return preAggregate;
  }

  public void setPreAggregate(List<ProjectionItem> preAggregate) {
    this.preAggregate = preAggregate;
  }

  public List<ProjectionItem> getAggregate() {
    return aggregate;
  }

  public void setAggregate(List<ProjectionItem> aggregate) {
    this.aggregate = aggregate;
  }

  /**
   * clean the content, but NOT the counter!
   */
  public void reset() {
    this.preAggregate.clear();
    this.aggregate.clear();
  }
}
