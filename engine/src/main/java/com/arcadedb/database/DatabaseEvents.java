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
 */
package com.arcadedb.database;

import com.arcadedb.event.DatabaseEventAfterCreateListener;
import com.arcadedb.event.DatabaseEventAfterDeleteListener;
import com.arcadedb.event.DatabaseEventAfterUpdateListener;
import com.arcadedb.event.DatabaseEventBeforeCreateListener;
import com.arcadedb.event.DatabaseEventBeforeDeleteListener;
import com.arcadedb.event.DatabaseEventBeforeUpdateListener;

public interface DatabaseEvents {
  DatabaseEvents registerListener(final DatabaseEventBeforeCreateListener listener);

  DatabaseEvents registerListener(final DatabaseEventBeforeUpdateListener listener);

  DatabaseEvents registerListener(final DatabaseEventBeforeDeleteListener listener);

  DatabaseEvents registerListener(final DatabaseEventAfterCreateListener listener);

  DatabaseEvents registerListener(final DatabaseEventAfterUpdateListener listener);

  DatabaseEvents registerListener(final DatabaseEventAfterDeleteListener listener);

  DatabaseEvents unregisterListener(final DatabaseEventBeforeCreateListener listener);

  DatabaseEvents unregisterListener(final DatabaseEventBeforeUpdateListener listener);

  DatabaseEvents unregisterListener(final DatabaseEventBeforeDeleteListener listener);

  DatabaseEvents unregisterListener(final DatabaseEventAfterCreateListener listener);

  DatabaseEvents unregisterListener(final DatabaseEventAfterUpdateListener listener);

  DatabaseEvents unregisterListener(final DatabaseEventAfterDeleteListener listener);
}
