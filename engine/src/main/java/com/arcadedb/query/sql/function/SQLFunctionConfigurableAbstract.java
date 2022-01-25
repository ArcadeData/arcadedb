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

/**
 * Abstract class to extend to build Custom SQL Functions that saves the configured parameters. Extend it and register it with:
 * {@literal OSQLParser.getInstance().registerStatelessFunction()} or
 * {@literal OSQLParser.getInstance().registerStatefullFunction()} to being used by the SQL engine.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 *
 */
public abstract class SQLFunctionConfigurableAbstract extends SQLFunctionAbstract {
  protected Object[] configuredParameters;

  protected SQLFunctionConfigurableAbstract(final String iName, final int iMinParams, final int iMaxParams) {
    super(iName);
  }

  @Override
  public void config(final Object[] iConfiguredParameters) {
    configuredParameters = iConfiguredParameters;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder(name);
    buffer.append('(');
    if (configuredParameters != null) {
      for (int i = 0; i < configuredParameters.length; ++i) {
        if (i > 0)
          buffer.append(',');
        buffer.append(configuredParameters[i]);
      }
    }
    buffer.append(')');
    return buffer.toString();
  }
}
