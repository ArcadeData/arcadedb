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
package com.arcadedb.query.literal;

import java.util.Arrays;

/**
 * The literal tokens a lexer found in one query text, and the text's SHAPE: the text with every one of those tokens replaced
 * by a marker naming its token type. Two texts with the same shape differ only in the values of their literals, so the parser
 * builds the same tree for both - its decisions read token types, never token text - and a decision taken on that tree about
 * which literal can become a parameter holds for every text of the shape (issue #8307).
 * <p>
 * Positions are character offsets into the text. A literal immediately preceded by a minus token records where that minus
 * starts, because the grammar may fold the sign into the literal and the policy then extracts both together.
 * <p>
 * The shape also records which literals are written identically (same token type, same text): both languages match an
 * expression against another by its text (an ORDER BY or GROUP BY item against a projection), so two identical literals must
 * be either both extracted, as one parameter, or both kept. Recording it in the shape makes that a property of the shape, so
 * one decision still holds for every text of the shape.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class LiteralScan {
  /** Separates a literal marker from the text around it; a text holding this character is never parameterized. */
  public static final char MARKER = '\u0000';

  private final String text;
  private       String shape;
  private       int    count;
  private       int[]  starts      = new int[4];
  private       int[]  ends        = new int[4];
  private       int[]  minusStarts = new int[4];
  private       int[]  tokenTypes  = new int[4];
  private       int[]  duplicateOf = new int[4];

  public LiteralScan(final String text) {
    this.text = text;
  }

  /**
   * Records a literal token. Tokens are recorded in the order they appear in the text.
   *
   * @param tokenType  the lexer token type, part of the shape
   * @param start      offset of the first character of the token
   * @param end        offset after the last character of the token
   * @param minusStart offset of a minus token that immediately precedes the literal, or -1
   */
  public void add(final int tokenType, final int start, final int end, final int minusStart) {
    if (count == starts.length) {
      final int capacity = count * 2;
      starts = Arrays.copyOf(starts, capacity);
      ends = Arrays.copyOf(ends, capacity);
      minusStarts = Arrays.copyOf(minusStarts, capacity);
      tokenTypes = Arrays.copyOf(tokenTypes, capacity);
      duplicateOf = Arrays.copyOf(duplicateOf, capacity);
    }
    starts[count] = start;
    ends[count] = end;
    minusStarts[count] = minusStart;
    tokenTypes[count] = tokenType;
    duplicateOf[count] = -1;
    for (int j = 0; j < count; j++)
      if (duplicateOf[j] < 0 && tokenTypes[j] == tokenType && ends[j] - starts[j] == end - start
          && text.regionMatches(starts[j], text, start, end - start)) {
        duplicateOf[count] = j;
        break;
      }
    ++count;
  }

  public String getText() {
    return text;
  }

  public int size() {
    return count;
  }

  public int getStart(final int index) {
    return starts[index];
  }

  public int getEnd(final int index) {
    return ends[index];
  }

  public int getMinusStart(final int index) {
    return minusStarts[index];
  }

  public int getTokenType(final int index) {
    return tokenTypes[index];
  }

  /** The first literal written identically to this one, or -1 when this one is the first of its text. */
  public int getDuplicateOf(final int index) {
    return duplicateOf[index];
  }

  /** The index of the literal starting at {@code offset}, or -1 when no literal starts there. */
  public int indexOfStart(final int offset) {
    final int found = Arrays.binarySearch(starts, 0, count, offset);
    return found < 0 ? -1 : found;
  }

  /**
   * The text with every literal replaced by {@link #MARKER}, its token type and the position (plus one) of the first literal
   * written identically, or 0. Computed once.
   */
  public String getShape() {
    if (shape == null) {
      final StringBuilder builder = new StringBuilder(text.length());
      int last = 0;
      for (int i = 0; i < count; i++) {
        builder.append(text, last, starts[i]).append(MARKER).append((char) tokenTypes[i]).append((char) (duplicateOf[i] + 1));
        last = ends[i];
      }
      shape = builder.append(text, last, text.length()).toString();
    }
    return shape;
  }

  /**
   * True when {@code text} can be scanned: it holds no {@link #MARKER}, which would make two shapes ambiguous, and no name in
   * the generated parameter namespace, which the extracted parameters would collide with.
   */
  public static boolean isEligible(final String text) {
    if (text.indexOf(MARKER) >= 0)
      return false;
    final String prefix = LiteralParameterizer.PARAMETER_PREFIX;
    for (int i = text.indexOf('_'); i >= 0 && i <= text.length() - prefix.length(); i = text.indexOf('_', i + 1))
      if (text.regionMatches(true, i, prefix, 0, prefix.length()))
        return false;
    return true;
  }
}
