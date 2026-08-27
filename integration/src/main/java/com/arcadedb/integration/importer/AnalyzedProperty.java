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
package com.arcadedb.integration.importer;

import com.arcadedb.schema.Type;

import java.util.HashSet;
import java.util.Set;

public class AnalyzedProperty {
  /** A value longer than this is not worth keeping as a sample, but it is still evidence about the type. */
  private static final int MAX_SAMPLE_LENGTH = 100;

  private final String      name;
  private final long        maxValueSampling;
  private final int         index;
  private final Set<String> contents            = new HashSet<>();
  private       Type        type;
  private       String      lastContent;
  private       boolean     candidateForInteger = true;
  private       boolean     candidateForDecimal = true;
  private       boolean     collectingSamples   = true;

  public AnalyzedProperty(final String name, final Type type, final long maxValueSampling, final int index) {
    this.name = name;
    this.type = type;
    this.maxValueSampling = maxValueSampling;
    this.index = index;
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  public void endParsing() {
    if (lastContent != null)
      if (candidateForInteger)
        type = Type.LONG;
      else if (candidateForDecimal)
        type = Type.DOUBLE;
  }

  public int getIndex() {
    return index;
  }

  /**
   * Sampling and type refutation are two independent concerns and are handled in that order here. Sample collection is
   * a memory bound and legitimately stops - on an oversized value or once enough distinct values have been seen. Type
   * refutation is evidence: skipping it for a value, or stopping it for the rest of the column, is how a text column
   * used to end up declared LONG and blow the import up on the very value the analysis never looked at (issue #6814).
   */
  public void setLastContent(final String lastContent) {
    if (lastContent == null)
      return;

    this.lastContent = lastContent;

    if (!lastContent.isEmpty()) {
      // EVERY VALUE IS PROBED, NO MATTER ITS LENGTH OR HOW MANY CAME BEFORE IT. THE PROBES COST NOTHING ONCE THE
      // CANDIDATE HAS ALREADY BEEN REFUTED, WHICH IS THE COMMON CASE FOR A TEXT COLUMN.
      if (candidateForInteger) {
        try {
          Long.parseLong(lastContent);
        } catch (final NumberFormatException e) {
          candidateForInteger = false;
        }
      }

      if (candidateForDecimal) {
        try {
          Double.parseDouble(lastContent);
        } catch (final NumberFormatException e) {
          candidateForDecimal = false;
        }
      }
    }

    if (!collectingSamples)
      return;

    if (lastContent.length() > MAX_SAMPLE_LENGTH || contents.size() > maxValueSampling) {
      collectingSamples = false;
      contents.clear();
      return;
    }

    contents.add(lastContent);
  }

  public Set<String> getContents() {
    return contents;
  }

  public boolean isCollectingSamples() {
    return collectingSamples;
  }

  @Override
  public String toString() {
    return name;
  }
}
