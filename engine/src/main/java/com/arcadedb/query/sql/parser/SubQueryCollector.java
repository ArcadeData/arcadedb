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

import java.util.HashMap;
import java.util.Map;

/**
 * This class is used by the query planner to extract subqueries and move them to LET clause
 * <br>
 * An example:
 * <br>
 * <br>
 * {@literal
 * select from foo where name in (select name from bar)
 * }
 * <br><br>
 * will become
 * <br><br>
 * {@literal
 * select from foo<br>
 * let _$$$SUBQUERY$$_0 = (select name from bar)<br>
 * where name in _$$$SUBQUERY$$_0
 * }
 * <br>
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class SubQueryCollector {

  protected static final String GENERATED_ALIAS_PREFIX = "_$$$SUBQUERY$$_";
  protected              int    nextAliasId            = 0;

  protected Map<Identifier, Statement> subQueries = new HashMap<>();

  protected Identifier getNextAlias() {
    Identifier result = new Identifier(GENERATED_ALIAS_PREFIX + (nextAliasId++));
    result.internalAlias = true;
    return result;
  }

  /**
   * clean the content, but NOT the counter!
   */
  public void reset() {
    this.subQueries.clear();
  }

  public Identifier addStatement(Identifier alias, Statement stm) {
    subQueries.put(alias, stm);
    return alias;
  }

  public Identifier addStatement(Statement stm) {
    Identifier alias = getNextAlias();
    return addStatement(alias, stm);
  }

  public Map<Identifier, Statement> getSubQueries() {
    return subQueries;
  }
}
