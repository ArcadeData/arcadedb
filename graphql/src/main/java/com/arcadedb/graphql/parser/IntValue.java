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

import java.math.BigInteger;

/**
 * An integer literal. It keeps the digits as written and answers the narrowest Java type that holds them - an {@link Integer},
 * a {@link Long} or a {@link BigInteger} - because the literal is not an {@code Int} until the type of the argument it is written
 * for says so: a {@code Long} or an {@code ID} argument takes values beyond 32 bits, and the 32-bit bound of {@code Int} is
 * checked where the declared type is known. Parsing it as an {@code int} here failed the whole document with a raw
 * {@code NumberFormatException} whatever the argument (issue #11451).
 */
public class IntValue extends AbstractValue {
  protected String image;

  /** Memoised result of {@link #getValue()}: an immutable boxed number, so the unsynchronised publication is a benign race. */
  private transient Object parsed;

  public IntValue(final int id) {
    super(id);
  }

  @Override
  public Object getValue() {
    Object result = parsed;
    if (result == null && image != null)
      parsed = result = parse(image);
    return result;
  }

  /** The literal as written in the document. */
  public String getImage() {
    return image;
  }

  /**
   * The narrowest of {@link Integer}, {@link Long} and {@link BigInteger} that holds {@code literal}, a lexed integer literal
   * (an optional minus sign and decimal digits without leading zeros).
   */
  static Object parse(final String literal) {
    final int digits = literal.length() - (literal.charAt(0) == '-' ? 1 : 0);
    // 18 DIGITS ALWAYS FIT A LONG, 19 MAY NOT: ONLY THE RARE WIDER LITERAL PAYS FOR A BigInteger
    if (digits <= 18) {
      final long value = Long.parseLong(literal);
      if (value == (int) value)
        return (int) value;
      return value;
    }
    final BigInteger value = new BigInteger(literal);
    // bitLength() EXCLUDES THE SIGN BIT, SO A SIGNED 64-BIT VALUE IS EXACTLY ONE OF AT MOST 63 BITS
    if (value.bitLength() < Long.SIZE)
      return value.longValue();
    return value;
  }

  @Override
  public String toString() {
    return "IntValue{" + image + '}';
  }
}
/* ParserGeneratorCC - OriginalChecksum=7cc83806b39c0849299461073146a07c (do not edit this line) */
