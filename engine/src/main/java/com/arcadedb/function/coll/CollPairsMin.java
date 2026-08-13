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

import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.List;

/**
 * coll.pairsMin(list) - Returns the consecutive-element pairs of a list, e.g. {@code coll.pairsMin([1, 2, 3])}
 * returns {@code [[1, 2], [2, 3]]}. Where APOC's {@code apoc.coll.pairs} pads the trailing incomplete pair with
 * {@code null}, this one drops it, so a list of fewer than two elements yields an empty list. Only the dropping
 * variant is implemented here; there is no {@code coll.pairs} in this engine.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CollPairsMin extends AbstractCollFunction {
  @Override
  protected String getSimpleName() {
    return "pairsMin";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public String getDescription() {
    return "Returns the consecutive-element pairs of a list, dropping the trailing incomplete pair";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    final List<Object> list = asList(args[0]);
    if (list == null)
      return null;

    final int pairCount = list.size() - 1;
    if (pairCount <= 0)
      return new ArrayList<>(0);

    final List<Object> result = new ArrayList<>(pairCount);
    for (int i = 0; i < pairCount; i++) {
      final List<Object> pair = new ArrayList<>(2);
      pair.add(list.get(i));
      pair.add(list.get(i + 1));
      result.add(pair);
    }
    return result;
  }
}
