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
package com.arcadedb.query.opencypher.traversal;

import java.util.ArrayList;
import java.util.Collection;

/**
 * The path a {@code shortestPath()} / {@code allShortestPaths()} pattern binds: the alternating vertex, edge, vertex ...
 * list it has always been, so every consumer that reads a path as a list keeps working, but of its own class so the type
 * system can tell it from a list that only happens to alternate vertices and edges (issue #9940): it is a PATH, the other
 * one a LIST.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ShortestPathList extends ArrayList<Object> {
  public ShortestPathList(final Collection<?> elements) {
    super(elements);
  }
}
