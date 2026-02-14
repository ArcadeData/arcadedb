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

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;

public class AlterMaterializedViewStatement extends DDLStatement {
  public Identifier name;
  public String refreshMode;
  public int refreshInterval;
  public String refreshUnit;

  public AlterMaterializedViewStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    // TODO: implement alter logic
    throw new UnsupportedOperationException("ALTER MATERIALIZED VIEW not yet implemented");
  }

  @Override
  public String toString() {
    return "ALTER MATERIALIZED VIEW " + name + " REFRESH " + refreshMode;
  }
}
