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
package com.arcadedb.query.sql.function.coll;

import com.arcadedb.query.sql.executor.BasicCommandContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionIntersectTest {

  @Test
  public void intersectInline() {
    final SQLFunctionIntersect function = new SQLFunctionIntersect();

    final List<Integer> coll1 = Arrays.asList(1, 1, 2, 3, 4, 5, 5, 6, 7, 9, 0, 1, 1, 1);
    final List<Integer> coll2 = Arrays.asList(1, 3, 0, 8);

    final ArrayList<Object> result = (ArrayList<Object>) function.execute(null, null, null, new Object[] { coll1, coll2 },
        new BasicCommandContext());

    assertThat(new HashSet<>(Arrays.asList(1, 3, 0))).isEqualTo(new HashSet<>(result));
  }

  @Test
  public void intersectNotInline() {
    final SQLFunctionIntersect function = new SQLFunctionIntersect();

    final List<Integer> coll1 = Arrays.asList(1, 1, 2, 3, 4, 5, 5, 6, 7, 9, 0, 1, 1, 1);
    final List<Integer> coll2 = Arrays.asList(1, 3, 0, 8);

    function.execute(null, null, null, new Object[] { coll1 }, new BasicCommandContext());
    function.execute(null, null, null, new Object[] { coll2 }, new BasicCommandContext());

    assertThat(new HashSet<>(Arrays.asList(1, 3, 0))).isEqualTo(function.getResult());
  }
}
