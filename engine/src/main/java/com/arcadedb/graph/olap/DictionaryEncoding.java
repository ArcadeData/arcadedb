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
package com.arcadedb.graph.olap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Dictionary encoding for string columns — maps unique string values to dense int codes.
 * <p>
 * This reduces memory by storing each unique string only once, and using compact
 * {@code int} codes in the column array. For columns with low cardinality
 * (e.g., status fields, categories), this can save 10-100x memory.
 * <p>
 * Encoding is also SIMD-friendly: equality filters on string columns become
 * simple integer comparisons on the code array.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DictionaryEncoding {
  private final Map<String, Integer> stringToCode;
  private final List<String>         codeToString;

  public DictionaryEncoding() {
    this.stringToCode = new HashMap<>();
    this.codeToString = new ArrayList<>();
  }

  /**
   * Encodes a string value, returning its integer code.
   * Assigns a new code if the value hasn't been seen before.
   */
  public int encode(final String value) {
    final Integer existing = stringToCode.get(value);
    if (existing != null)
      return existing;

    final int code = codeToString.size();
    codeToString.add(value);
    stringToCode.put(value, code);
    return code;
  }

  /**
   * Decodes an integer code back to its string value.
   */
  public String decode(final int code) {
    return codeToString.get(code);
  }

  /**
   * Returns the code for a string value, or -1 if not in the dictionary.
   */
  public int getCode(final String value) {
    final Integer code = stringToCode.get(value);
    return code != null ? code : -1;
  }

  /**
   * Returns the number of unique values in the dictionary.
   */
  public int size() {
    return codeToString.size();
  }

  /**
   * Returns the dictionary values as an array (indexed by code).
   */
  public String[] getValues() {
    return codeToString.toArray(new String[0]);
  }

  /**
   * Returns approximate memory usage in bytes.
   */
  public long getMemoryUsageBytes() {
    long bytes = 0;
    for (final String s : codeToString)
      bytes += 40 + (long) s.length() * 2; // String object overhead + char data
    bytes += (long) codeToString.size() * 48; // HashMap entry overhead
    return bytes;
  }
}
