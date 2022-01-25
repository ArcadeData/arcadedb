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
package com.arcadedb.query.sql.function.coll;

import com.arcadedb.query.sql.function.SQLFunctionConfigurableAbstract;

/**
 * Abstract class for multi-value based function implementations.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public abstract class SQLFunctionMultiValueAbstract<T> extends SQLFunctionConfigurableAbstract {

  protected T context;

  public SQLFunctionMultiValueAbstract(final String iName, final int iMinParams, final int iMaxParams) {
    super(iName, iMinParams, iMaxParams);
  }

  @Override
  public boolean aggregateResults() {
    return configuredParameters.length == 1;
  }

  @Override
  public T getResult() {
    return context;
  }
}
