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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.query.sql.executor.ResultInternal;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests verifying copy-on-write reference identity in {@link DeleteStep#markRowDeletedReferences}.
 */
class DeleteStepMarkDeletedReferencesTest {

  @Test
  void testCopyOnWritePreservesContainerIdentityWhenNothingDeleted() {
    final ResultInternal row = new ResultInternal();
    final List<Object> list = new ArrayList<>(List.of("val1", 42L));
    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("key1", "val1");
    map.put("key2", 42L);

    row.setProperty("list", list);
    row.setProperty("map", map);

    final DeleteStep.DeletedSoFar deleted = new DeleteStep.DeletedSoFar();

    DeleteStep.markRowDeletedReferences(row, deleted);

    assertThat((Object) row.getProperty("list")).isSameAs(list);
    assertThat((Object) row.getProperty("map")).isSameAs(map);
  }
}
