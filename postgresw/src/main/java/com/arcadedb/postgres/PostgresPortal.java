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
package com.arcadedb.postgres;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.parser.Statement;

import java.util.List;
import java.util.Map;

public class PostgresPortal {
  public String                    query;
  public List<Long>                parameterTypes;
  public List<Integer>             parameterFormats;
  public List<Object>              parameterValues;
  public List<Integer>             resultFormats;
  public Statement                 sqlStatement;
  public boolean                   ignoreExecution   = false;
  public List<Result>              cachedResultset;
  public Map<String, PostgresType> columns;
  public boolean                   isExpectingResult = true;
  public boolean                   executed          = false;

  public PostgresPortal(final String query) {
    this.query = query;
    //final String queryUpperCase = query.toUpperCase();
    this.isExpectingResult = true;//queryUpperCase.startsWith("SELECT") || queryUpperCase.startsWith("MATCH");
  }

  @Override
  public String toString() {
    return query;
  }
}
