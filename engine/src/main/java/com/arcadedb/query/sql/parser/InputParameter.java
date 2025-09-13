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
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_USERTYPE_VISIBILITY_PUBLIC=true */
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.MultiValue;

import java.math.*;
import java.text.*;
import java.util.*;

public class InputParameter extends SimpleNode {

  protected static final String dateFormatString = "yyyy-MM-dd HH:mm:ss.SSS";

  public InputParameter(final int id) {
    super(id);
  }

  public Object bindFromInputParams(final Map<String, Object> params) {
    return null;
  }

  public Object getValue(final Map<String, Object> params) {
    return null;
  }

  protected Object toParsedTree(final Object value) {
    if (value == null) {
      final Expression result = new Expression(-1);
      result.isNull = true;
      return result;
    }
    if (value instanceof Boolean boolean1) {
      final Expression result = new Expression(-1);
      result.booleanValue = boolean1;
      return result;
    }
    if (value instanceof Integer integer) {
      final PInteger result = new PInteger(-1);
      result.setValue(integer);
      return result;
    }
    if (value instanceof BigDecimal decimal) {
      final Expression result = new Expression(-1);
      final FunctionCall funct = new FunctionCall(-1);
      result.mathExpression = new BaseExpression(-1);
      ((BaseExpression) result.mathExpression).identifier = new BaseIdentifier(-1);
      ((BaseExpression) result.mathExpression).identifier.levelZero = new LevelZeroIdentifier(-1);
      ((BaseExpression) result.mathExpression).identifier.levelZero.functionCall = funct;
      funct.name = new Identifier("decimal");
      final Expression stringExp = new Expression(-1);
      stringExp.mathExpression = new BaseExpression(decimal.toPlainString());
      funct.getParams().add(stringExp);
      return result;
    }

    if (value instanceof Number number) {
      final FloatingPoint result = new FloatingPoint(-1);
      result.sign = number.doubleValue() >= 0 ? 1 : -1;
      result.stringValue = value.toString();
      if (result.stringValue.startsWith("-")) {
        result.stringValue = result.stringValue.substring(1);
      }
      return result;
    }
    if (value instanceof String) {
      return value;
    }
    if (MultiValue.isMultiValue(value) && !(value instanceof byte[]) && !(value instanceof Byte[])) {
      final PCollection coll = new PCollection(-1);
      coll.expressions = new ArrayList<Expression>();
      final Iterator iterator = MultiValue.getMultiValueIterator(value);
      while (iterator.hasNext()) {
        final Object o = iterator.next();
        final Expression exp = new Expression(-1);
        exp.value = toParsedTree(o);
        coll.expressions.add(exp);
      }
      return coll;
    }
    if (value instanceof Map map) {
      final Json json = new Json(-1);
      json.items = new ArrayList<JsonItem>();
      for (final Object entry : map.entrySet()) {
        final JsonItem item = new JsonItem();
        item.leftString = "" + ((Map.Entry) entry).getKey();
        final Expression exp = new Expression(-1);
        exp.value = toParsedTree(((Map.Entry) entry).getValue());
        item.right = exp;
        json.items.add(item);
      }
      return json;
    }
    if (value instanceof Identifiable identifiable) {
      // TODO if invalid build a JSON
      final Rid rid = new Rid(-1);
      final String stringVal = identifiable.getIdentity().toString().substring(1);
      final String[] splitted = stringVal.split(":");
      final PInteger c = new PInteger(-1);
      c.setValue(Integer.parseInt(splitted[0]));
      rid.bucket = c;
      final PInteger p = new PInteger(-1);
      p.setValue(Integer.parseInt(splitted[1]));
      rid.position = p;
      rid.setLegacy(true);
      return rid;
    }
    if (value instanceof Date) {
      final FunctionCall function = new FunctionCall(-1);
      function.name = new Identifier("date");

      final Expression dateExpr = new Expression(-1);
      dateExpr.singleQuotes = true;
      dateExpr.doubleQuotes = false;
      final SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatString);
      dateExpr.value = dateFormat.format(value);
      function.getParams().add(dateExpr);

      final Expression dateFormatExpr = new Expression(-1);
      dateFormatExpr.singleQuotes = true;
      dateFormatExpr.doubleQuotes = false;
      dateFormatExpr.value = dateFormatString;
      function.getParams().add(dateFormatExpr);
      return function;
    }

    if (value.getClass().isEnum())
      return value.toString();

    return this;
  }

  public InputParameter copy() {
    throw new UnsupportedOperationException();
  }
}
/* JavaCC - OriginalChecksum=bb2f3732f5e3be4d954527ee0baa9020 (do not edit this line) */
