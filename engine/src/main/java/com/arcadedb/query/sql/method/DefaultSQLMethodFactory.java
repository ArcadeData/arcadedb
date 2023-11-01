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

// Collections
import com.arcadedb.query.sql.method.collection.SQLMethodField;
import com.arcadedb.query.sql.method.collection.SQLMethodKeys;
import com.arcadedb.query.sql.method.collection.SQLMethodRemove;
import com.arcadedb.query.sql.method.collection.SQLMethodRemoveAll;
import com.arcadedb.query.sql.method.collection.SQLMethodSize;
import com.arcadedb.query.sql.method.collection.SQLMethodTransform;
import com.arcadedb.query.sql.method.collection.SQLMethodValues;

// Conversions
import com.arcadedb.query.sql.method.conversion.SQLMethodAsBoolean;
import com.arcadedb.query.sql.method.conversion.SQLMethodAsByte;
import com.arcadedb.query.sql.method.conversion.SQLMethodAsDate;
import com.arcadedb.query.sql.method.conversion.SQLMethodAsDateTime;
import com.arcadedb.query.sql.method.conversion.SQLMethodAsDecimal;
import com.arcadedb.query.sql.method.conversion.SQLMethodAsDouble;
import com.arcadedb.query.sql.method.conversion.SQLMethodAsFloat;
import com.arcadedb.query.sql.method.conversion.SQLMethodAsInteger;
import com.arcadedb.query.sql.method.conversion.SQLMethodAsList;
import com.arcadedb.query.sql.method.conversion.SQLMethodAsLong;
import com.arcadedb.query.sql.method.conversion.SQLMethodAsMap;
import com.arcadedb.query.sql.method.conversion.SQLMethodAsRID;
import com.arcadedb.query.sql.method.conversion.SQLMethodAsRecord;
import com.arcadedb.query.sql.method.conversion.SQLMethodAsSet;
import com.arcadedb.query.sql.method.conversion.SQLMethodAsShort;
import com.arcadedb.query.sql.method.conversion.SQLMethodAsString;
import com.arcadedb.query.sql.method.conversion.SQLMethodConvert;

// Geometrics
import com.arcadedb.query.sql.method.geo.SQLMethodIntersectsWith;
import com.arcadedb.query.sql.method.geo.SQLMethodIsWithin;

// Misc
import com.arcadedb.query.sql.method.misc.SQLMethodExclude;
import com.arcadedb.query.sql.method.misc.SQLMethodHash;
import com.arcadedb.query.sql.method.misc.SQLMethodIfNull;
import com.arcadedb.query.sql.method.misc.SQLMethodInclude;
import com.arcadedb.query.sql.method.misc.SQLMethodJavaType;
import com.arcadedb.query.sql.method.misc.SQLMethodPrecision;
import com.arcadedb.query.sql.method.misc.SQLMethodToJSON;
import com.arcadedb.query.sql.method.misc.SQLMethodType;

// String
import com.arcadedb.query.sql.method.string.SQLMethodAppend;
import com.arcadedb.query.sql.method.string.SQLMethodCapitalize;
import com.arcadedb.query.sql.method.string.SQLMethodCharAt;
import com.arcadedb.query.sql.method.string.SQLMethodFormat;
import com.arcadedb.query.sql.method.string.SQLMethodIndexOf;
import com.arcadedb.query.sql.method.string.SQLMethodLastIndexOf;
import com.arcadedb.query.sql.method.string.SQLMethodLeft;
import com.arcadedb.query.sql.method.string.SQLMethodLength;
import com.arcadedb.query.sql.method.string.SQLMethodNormalize;
import com.arcadedb.query.sql.method.string.SQLMethodPrefix;
import com.arcadedb.query.sql.method.string.SQLMethodReplace;
import com.arcadedb.query.sql.method.string.SQLMethodRight;
import com.arcadedb.query.sql.method.string.SQLMethodSplit;
import com.arcadedb.query.sql.method.string.SQLMethodSubString;
import com.arcadedb.query.sql.method.string.SQLMethodToLowerCase;
import com.arcadedb.query.sql.method.string.SQLMethodToUpperCase;
import com.arcadedb.query.sql.method.string.SQLMethodTrim;

import java.util.*;

/**
 * Default method factory.
 *
 * @author Johann Sorel (Geomatys)
 */
public class DefaultSQLMethodFactory implements SQLMethodFactory {
  private final Map<String, Object> methods = new HashMap<>();

  public DefaultSQLMethodFactory() {
    // Collections
    register(SQLMethodField.NAME, new SQLMethodField());
    register(SQLMethodKeys.NAME, new SQLMethodKeys());
    register(SQLMethodRemove.NAME, new SQLMethodRemove());
    register(SQLMethodRemoveAll.NAME, new SQLMethodRemoveAll());
    register(SQLMethodSize.NAME, new SQLMethodSize());
    register(SQLMethodTransform.NAME, new SQLMethodTransform());
    register(SQLMethodValues.NAME, new SQLMethodValues());

    // Conversions
    register(SQLMethodAsBoolean.NAME, new SQLMethodAsBoolean());
    register(SQLMethodAsByte.NAME, new SQLMethodAsByte());
    register(SQLMethodAsDate.NAME, new SQLMethodAsDate());
    register(SQLMethodAsDateTime.NAME, new SQLMethodAsDateTime());
    register(SQLMethodAsDecimal.NAME, new SQLMethodAsDecimal());
    register(SQLMethodAsDouble.NAME, new SQLMethodAsDouble());
    register(SQLMethodAsFloat.NAME, new SQLMethodAsFloat());
    register(SQLMethodAsInteger.NAME, new SQLMethodAsInteger());
    register(SQLMethodAsList.NAME, new SQLMethodAsList());
    register(SQLMethodAsLong.NAME, new SQLMethodAsLong());
    register(SQLMethodAsMap.NAME, new SQLMethodAsMap());
    register(SQLMethodAsRID.NAME, new SQLMethodAsRID());
    register(SQLMethodAsRecord.NAME, new SQLMethodAsRecord());
    register(SQLMethodAsSet.NAME, new SQLMethodAsSet());
    register(SQLMethodAsShort.NAME, new SQLMethodAsShort());
    register(SQLMethodAsString.NAME, new SQLMethodAsString());
    register(SQLMethodConvert.NAME, new SQLMethodConvert());

    // Geo
    register(SQLMethodIsWithin.NAME, new SQLMethodIsWithin());
    register(SQLMethodIntersectsWith.NAME, new SQLMethodIntersectsWith());

    // Misc
    register(SQLMethodExclude.NAME, new SQLMethodExclude());
    register(SQLMethodHash.NAME, new SQLMethodHash());
    register(SQLMethodIfNull.NAME, new SQLMethodIfNull());
    register(SQLMethodInclude.NAME, new SQLMethodInclude());
    register(SQLMethodJavaType.NAME, new SQLMethodJavaType());
    register(SQLMethodPrecision.NAME, new SQLMethodPrecision());
    register(SQLMethodToJSON.NAME, new SQLMethodToJSON());
    register(SQLMethodType.NAME, new SQLMethodType());

    // String
    register(SQLMethodAppend.NAME, new SQLMethodAppend());
    register(SQLMethodCapitalize.NAME, new SQLMethodCapitalize());
    register(SQLMethodCharAt.NAME, new SQLMethodCharAt());
    register(SQLMethodFormat.NAME, new SQLMethodFormat());
    register(SQLMethodIndexOf.NAME, new SQLMethodIndexOf());
    register(SQLMethodLastIndexOf.NAME, new SQLMethodLastIndexOf());
    register(SQLMethodLeft.NAME, new SQLMethodLeft());
    register(SQLMethodLength.NAME, new SQLMethodLength());
    register(SQLMethodNormalize.NAME, new SQLMethodNormalize());
    register(SQLMethodPrefix.NAME, new SQLMethodPrefix());
    register(SQLMethodReplace.NAME, new SQLMethodReplace());
    register(SQLMethodRight.NAME, new SQLMethodRight());
    register(SQLMethodSplit.NAME, new SQLMethodSplit());
    register(SQLMethodSubString.NAME, new SQLMethodSubString());
    register(SQLMethodToLowerCase.NAME, new SQLMethodToLowerCase());
    register(SQLMethodToUpperCase.NAME, new SQLMethodToUpperCase());
    register(SQLMethodTrim.NAME, new SQLMethodTrim());
  }

  public void register(final String iName, final Object iImplementation) {
    methods.put(iName.toLowerCase(Locale.ENGLISH), iImplementation);
  }

  @Override
  public SQLMethod createMethod(final String name) throws CommandExecutionException {
    final Object m = methods.get(name.toLowerCase(Locale.ENGLISH));
    final SQLMethod method;

    if (m instanceof Class<?>)
      try {
        method = (SQLMethod) ((Class<?>) m).getConstructor().newInstance();
      } catch (final Exception e) {
        throw new CommandExecutionException("Cannot create SQL method: " + m, e);
      }
    else
      method = (SQLMethod) m;

    if (method == null)
      throw new CommandExecutionException("Unknown method name: " + name);

    return method;
  }

}
