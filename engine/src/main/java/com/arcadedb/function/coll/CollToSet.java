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
package com.arcadedb.function.coll;

/**
 * coll.toSet(list) - the APOC spelling of {@link CollDistinct}, kept because a migrating query catalogue writes it
 * (issue #6157).
 * <p>
 * It is an alias and nothing more: both names answer {@code [1, 2]} for {@code [1, 1, 2]}, both preserve the order
 * of first occurrence, both recognize duplicates by object equality (so {@code coll.toSet([1, 1.0])} keeps both
 * elements), and both return a LIST - the name is the only difference. Written as a subclass rather than as a
 * second copy of the body so there is one implementation to fix: while there were two, every change to one had to
 * be remembered for the other, and the lazy-range short-circuit of issue #6353 had to be written twice
 * (issue #6403).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CollToSet extends CollDistinct {
  @Override
  protected String getSimpleName() {
    return "toSet";
  }

  @Override
  public String getDescription() {
    return "Returns a unique list from the given list, preserving the order of first occurrence";
  }
}
