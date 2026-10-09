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
package com.arcadedb.redis;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.RecordNotFoundException;

/**
 * How the Redis surfaces (RESP wire path and {@code redis} query language) name and delete a record by RID, so HDEL answers
 * the same on both (#9056, #9161).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RedisRecords {
  private RedisRecords() {
  }

  /** {@code #<bucket-id>:<position>}, both non-negative: the only shape of key HDEL reads as a record rather than a variable. */
  public static boolean isRid(final String key) {
    final int colon = key.indexOf(':');
    if (colon < 2 || key.charAt(0) != '#' || colon == key.length() - 1)
      return false;
    for (int i = 1; i < key.length(); i++)
      if (i != colon && (key.charAt(i) < '0' || key.charAt(i) > '9'))
        return false;
    return true;
  }

  public static RID parseRid(final String text) {
    try {
      return new RID(text);
    } catch (final RuntimeException e) {
      throw new RedisException("invalid RID '" + text + "', it must be #<bucket-id>:<bucket-position>");
    }
  }

  /** Deletes the record at {@code rid}; false when there is none, so the reply counts only what was really deleted. */
  public static boolean deleteByRid(final Database database, final RID rid) {
    final Record record;
    try {
      record = database.lookupByRID(rid, true);
    } catch (final RecordNotFoundException e) {
      return false;
    }
    record.delete();
    return true;
  }
}
