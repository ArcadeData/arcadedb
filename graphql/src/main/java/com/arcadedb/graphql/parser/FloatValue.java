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
/* ParserGeneratorCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.arcadedb.graphql.parser;

import com.arcadedb.exception.CommandParsingException;

import java.math.BigDecimal;

public class FloatValue extends AbstractValue {
  protected String stringValue;

  public FloatValue(final int id) {
    super(id);
  }

  /**
   * The longest float literal taken, in characters: a {@code double} needs at most 17 significant digits and an exponent of 3,
   * while {@code BigDecimal} parsing is superlinear in the digits, so a document of digits must not buy that CPU (#11451).
   */
  static final int MAX_LENGTH = 128;

  @Override
  public Object getValue() {
    if (stringValue.length() > MAX_LENGTH)
      throw new CommandParsingException(
          "GraphQL float literal of " + stringValue.length() + " characters exceeds the " + MAX_LENGTH + " a Float can take");
    return new BigDecimal(stringValue);
  }

  @Override
  public String toString() {
    return "FloatValue{" + stringValue + '}';
  }
}
/* ParserGeneratorCC - OriginalChecksum=6629a12b125a5d138e3a3f27eae0b061 (do not edit this line) */
