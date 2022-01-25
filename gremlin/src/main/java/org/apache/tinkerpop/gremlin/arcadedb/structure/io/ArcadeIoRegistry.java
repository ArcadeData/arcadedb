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
package org.apache.tinkerpop.gremlin.arcadedb.structure.io;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;

import java.util.*;

@SuppressWarnings("serial")
public class ArcadeIoRegistry extends AbstractIoRegistry {
  public static final  String           BUCKET_ID       = "bucketId";
  public static final  String           BUCKET_POSITION = "bucketPosition";
  private static final ArcadeIoRegistry INSTANCE        = new ArcadeIoRegistry();

  private final Database database;

  public ArcadeIoRegistry() {
    this(null);
  }

  public ArcadeIoRegistry(final Database database) {
    this.database = database;
  }

  public Database getDatabase() {
    return database;
  }

  public RID newRID(final Object obj) {
    return newRID(this.database, obj);
  }

  public static RID newRID(final Database database, final Object obj) {
    if (obj == null)
      return null;
    if (obj instanceof RID)
      return (RID) obj;
    if (obj instanceof String)
      return new RID(database, (String) obj);

    if (obj instanceof Map) {
      @SuppressWarnings({ "unchecked", "rawtypes" })
      final Map<String, Number> map = (Map) obj;
      return new RID(database, map.get(BUCKET_ID).intValue(), map.get(BUCKET_POSITION).longValue());
    }

    throw new IllegalArgumentException("Unable to convert unknown (" + obj.getClass() + ") type to RID");
  }

  public static boolean isRID(final Object result) {
    if (!(result instanceof Map))
      return false;

    @SuppressWarnings("unchecked")
    final Map<String, Number> map = (Map<String, Number>) result;
    return map.containsKey(BUCKET_ID) && map.containsKey(BUCKET_POSITION);
  }

  public static ArcadeIoRegistry instance() {
    return INSTANCE;
  }
}
