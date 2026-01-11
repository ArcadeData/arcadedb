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
package com.arcadedb.opencypher.ast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents a MATCH clause in a Cypher query.
 * Contains graph patterns to match against the database.
 */
public class MatchClause {
  private final String pattern; // Raw pattern string (Phase 1)
  private final boolean optional;
  private final List<PathPattern> pathPatterns; // Parsed path patterns (Phase 2+)

  /**
   * Creates a match clause with raw pattern string (Phase 1).
   */
  public MatchClause(final String pattern, final boolean optional) {
    this.pattern = pattern;
    this.optional = optional;
    this.pathPatterns = new ArrayList<>();
  }

  /**
   * Creates a match clause with parsed path patterns (Phase 2+).
   */
  public MatchClause(final List<PathPattern> pathPatterns, final boolean optional) {
    this.pattern = null;
    this.optional = optional;
    this.pathPatterns = pathPatterns != null ? new ArrayList<>(pathPatterns) : new ArrayList<>();
  }

  /**
   * Returns the raw pattern string (Phase 1).
   *
   * @return raw pattern string
   */
  public String getPattern() {
    return pattern;
  }

  /**
   * Returns true if this is an OPTIONAL MATCH.
   *
   * @return true if optional
   */
  public boolean isOptional() {
    return optional;
  }

  /**
   * Returns the list of path patterns (Phase 2+).
   *
   * @return list of path patterns
   */
  public List<PathPattern> getPathPatterns() {
    return Collections.unmodifiableList(pathPatterns);
  }

  /**
   * Returns true if this match clause has parsed path patterns.
   *
   * @return true if has path patterns
   */
  public boolean hasPathPatterns() {
    return !pathPatterns.isEmpty();
  }

  /**
   * Adds a path pattern to this match clause.
   *
   * @param pathPattern path pattern to add
   */
  public void addPathPattern(final PathPattern pathPattern) {
    this.pathPatterns.add(pathPattern);
  }
}
