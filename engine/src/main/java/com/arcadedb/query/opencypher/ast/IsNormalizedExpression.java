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
package com.arcadedb.query.opencypher.ast;

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.text.Normalizer;

/**
 * GQL / Cypher 25 normalized predicate {@code <string> IS [NOT] [NFC|NFD|NFKC|NFKD] NORMALIZED} (issue #9940): whether the
 * string is already in the given Unicode normal form, NFC when none is written. Null, and anything other than a string,
 * answers null (UNKNOWN), as in Neo4j Cypher 25.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class IsNormalizedExpression implements BooleanExpression {
  private final Expression      valueExpression;
  private final Normalizer.Form form;
  private final boolean         isNot;

  public IsNormalizedExpression(final Expression valueExpression, final Normalizer.Form form, final boolean isNot) {
    this.valueExpression = valueExpression;
    this.form = form;
    this.isNot = isNot;
  }

  @Override
  public boolean evaluate(final Result result, final CommandContext context) {
    return Boolean.TRUE.equals(evaluateTernary(result, context));
  }

  @Override
  public Object evaluateTernary(final Result result, final CommandContext context) {
    final Object value = valueExpression.evaluate(result, context);
    // Only a string has a normal form: anything else, like null, is UNKNOWN, as in Neo4j Cypher 25
    if (!(value instanceof String string))
      return null;
    return isNot != Normalizer.isNormalized(string, form);
  }

  @Override
  public String getText() {
    return valueExpression.getText() + (isNot ? " IS NOT " : " IS ") + form + " NORMALIZED";
  }

  public Expression getValueExpression() {
    return valueExpression;
  }

  public Normalizer.Form getForm() {
    return form;
  }

  public boolean isNot() {
    return isNot;
  }
}
