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
package com.arcadedb.query.sql.function;

import com.arcadedb.query.sql.executor.SQLFunction;

/**
 * Abstract class to extend to build Custom SQL Functions.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public abstract class SQLFunctionAbstract implements SQLFunction {
  protected String name;

  public SQLFunctionAbstract(final String iName) {
    this.name = iName;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return getSyntax();
  }

  @Override
  public void config(final Object[] iConfiguredParameters) {
  }

  @Override
  public boolean aggregateResults() {
    return false;
  }

  @Override
  public Object getResult() {
    return null;
  }
}
