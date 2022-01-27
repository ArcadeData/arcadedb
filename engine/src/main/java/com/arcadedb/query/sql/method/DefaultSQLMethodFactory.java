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
package com.arcadedb.query.sql.method;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.SQLMethod;
import com.arcadedb.query.sql.function.conversion.SQLMethodAsDate;
import com.arcadedb.query.sql.function.conversion.SQLMethodAsDateTime;
import com.arcadedb.query.sql.function.conversion.SQLMethodAsDecimal;
import com.arcadedb.query.sql.function.conversion.SQLMethodConvert;
import com.arcadedb.query.sql.function.text.*;
import com.arcadedb.query.sql.method.misc.*;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Default method factory.
 *
 * @author Johann Sorel (Geomatys)
 */
public class DefaultSQLMethodFactory implements SQLMethodFactory {
  private final Map<String, Object> methods = new HashMap<>();

  public DefaultSQLMethodFactory() {
    register(SQLMethodAppend.NAME, new SQLMethodAppend());
    register(SQLMethodAsBoolean.NAME, new SQLMethodAsBoolean());
    register(SQLMethodAsDate.NAME, new SQLMethodAsDate());
    register(SQLMethodAsDateTime.NAME, new SQLMethodAsDateTime());
    register(SQLMethodAsDecimal.NAME, new SQLMethodAsDecimal());
    register(SQLMethodAsFloat.NAME, new SQLMethodAsFloat());
    register(SQLMethodAsInteger.NAME, new SQLMethodAsInteger());
    register(SQLMethodAsList.NAME, new SQLMethodAsList());
    register(SQLMethodAsLong.NAME, new SQLMethodAsLong());
    register(SQLMethodAsMap.NAME, new SQLMethodAsMap());
    register(SQLMethodAsSet.NAME, new SQLMethodAsSet());
    register(SQLMethodAsString.NAME, new SQLMethodAsString());
    register(SQLMethodCharAt.NAME, new SQLMethodCharAt());
    register(SQLMethodConvert.NAME, new SQLMethodConvert());
    register(SQLMethodField.NAME, new SQLMethodField());
    register(SQLMethodFormat.NAME, new SQLMethodFormat());
    register(SQLMethodHash.NAME, new SQLMethodHash());
    register(SQLMethodIndexOf.NAME, new SQLMethodIndexOf());
    register(SQLMethodJavaType.NAME, new SQLMethodJavaType());
    register(SQLMethodKeys.NAME, new SQLMethodKeys());
    register(SQLMethodLastIndexOf.NAME, new SQLMethodLastIndexOf());
    register(SQLMethodLeft.NAME, new SQLMethodLeft());
    register(SQLMethodLength.NAME, new SQLMethodLength());
    register(SQLMethodNormalize.NAME, new SQLMethodNormalize());
    register(SQLMethodPrefix.NAME, new SQLMethodPrefix());
    register(SQLMethodRemove.NAME, new SQLMethodRemove());
    register(SQLMethodRemoveAll.NAME, new SQLMethodRemoveAll());
    register(SQLMethodReplace.NAME, new SQLMethodReplace());
    register(SQLMethodRight.NAME, new SQLMethodRight());
    register(SQLMethodSize.NAME, new SQLMethodSize());
    register(SQLMethodSplit.NAME, new SQLMethodSplit());
    register(SQLMethodToLowerCase.NAME, new SQLMethodToLowerCase());
    register(SQLMethodToUpperCase.NAME, new SQLMethodToUpperCase());
    register(SQLMethodTrim.NAME, new SQLMethodTrim());
    register(SQLMethodType.NAME, new SQLMethodType());
    register(SQLMethodSubString.NAME, new SQLMethodSubString());
    register(SQLMethodToJSON.NAME, new SQLMethodToJSON());
  }

  public void register(final String iName, final Object iImplementation) {
    methods.put(iName.toLowerCase(Locale.ENGLISH), iImplementation);
  }

  @Override
  public SQLMethod createMethod(final String name) throws CommandExecutionException {
    final Object m = methods.get(name.toLowerCase());
    final SQLMethod method;

    if (m instanceof Class<?>)
      try {
        method = (SQLMethod) ((Class<?>) m).getConstructor().newInstance();
      } catch (Exception e) {
        throw new CommandExecutionException("Cannot create SQL method: " + m, e);
      }
    else
      method = (SQLMethod) m;

    if (method == null)
      throw new CommandExecutionException("Unknown method name: " + name);

    return method;
  }

}
