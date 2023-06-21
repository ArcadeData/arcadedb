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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CompressedRID2RIDIndexTest extends TestHelper {
  private static final int TOT = 10_000_000;

  @Test
  public void testIndexPutAndGet() throws ClassNotFoundException {
    final CompressedRID2RIDIndex index = new CompressedRID2RIDIndex(database, TOT, TOT);

    for (int i = 0; i < TOT; i++)
      index.put(new RID(database, 3, i), new RID(database, 4, i));

    for (int i = 0; i < TOT; i++)
      Assertions.assertEquals(new RID(database, 4, i), index.get(new RID(database, 3, i)));

    int found = 0;
    for (CompressedRID2RIDIndex.EntryIterator it = index.entryIterator(); it.hasNext(); ) {
      final RID key = it.getKeyRID();
      final RID value = it.getValueRID();

      Assertions.assertEquals(3, key.getBucketId());
      Assertions.assertEquals(4, value.getBucketId());
      Assertions.assertEquals(key.getPosition(), value.getPosition());

      ++found;
      it.moveNext();
    }

    Assertions.assertEquals(TOT, found);
  }
}
