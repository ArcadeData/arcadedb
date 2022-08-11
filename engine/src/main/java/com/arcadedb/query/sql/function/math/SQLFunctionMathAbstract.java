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
package com.arcadedb.query.sql.function.math;

import com.arcadedb.query.sql.function.SQLFunctionConfigurableAbstract;

import java.math.*;

/**
 * Abstract class for math function.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public abstract class SQLFunctionMathAbstract extends SQLFunctionConfigurableAbstract {

  public SQLFunctionMathAbstract(String iName, int iMinParams, int iMaxParams) {
    super(iName, iMinParams, iMaxParams);
  }

  protected Number getContextValue(Object iContext, final Class<? extends Number> iClass) {
    if (iClass != iContext.getClass()) {
      // CHANGE TYPE
      if (iClass == Long.class)
        iContext = Long.valueOf(((Number) iContext).longValue());
      else if (iClass == Short.class)
        iContext = Short.valueOf(((Number) iContext).shortValue());
      else if (iClass == Float.class)
        iContext = Float.valueOf(((Number) iContext).floatValue());
      else if (iClass == Double.class)
        iContext = Double.valueOf(((Number) iContext).doubleValue());
    }

    return (Number) iContext;
  }

  protected Class<? extends Number> getClassWithMorePrecision(final Class<? extends Number> iClass1, final Class<? extends Number> iClass2) {
    if (iClass1 == iClass2)
      return iClass1;

    if (iClass1 == Integer.class && (iClass2 == Long.class || iClass2 == Float.class || iClass2 == Double.class || iClass2 == BigDecimal.class))
      return iClass2;
    else if (iClass1 == Long.class && (iClass2 == Float.class || iClass2 == Double.class || iClass2 == BigDecimal.class))
      return iClass2;
    else if (iClass1 == Float.class && (iClass2 == Double.class || iClass2 == BigDecimal.class))
      return iClass2;

    return iClass1;
  }

  @Override
  public boolean aggregateResults() {
    return configuredParameters.length == 1;
  }
}
