/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.index.fulltext;

import java.util.Map;

/**
 * Type-wide corpus statistics shared by every bucket while scoring one BM25 query.
 */
record BM25ScoringContext(long totalDocs, double avgDocLength, Map<String, Long> documentFrequencies) {
  BM25ScoringContext {
    if (totalDocs < 1)
      throw new IllegalArgumentException("totalDocs must be at least 1");
    if (!Double.isFinite(avgDocLength) || avgDocLength <= 0)
      throw new IllegalArgumentException("avgDocLength must be finite and positive");
    documentFrequencies = Map.copyOf(documentFrequencies);
  }

  long documentFrequency(final String token) {
    return documentFrequencies.getOrDefault(token, 0L);
  }
}
