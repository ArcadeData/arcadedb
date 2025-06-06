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
package com.arcadedb.database;

import com.arcadedb.engine.WALFile;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

@ExcludeFromJacocoGeneratedReport
public interface Transaction {
  void begin(Database.TRANSACTION_ISOLATION_LEVEL isolationLevel);

  Binary commit();

  void rollback();

  boolean isActive();

  void setUseWAL(boolean useWAL);

  void setWALFlush(WALFile.FlushType flush);

  boolean isAsyncFlush();

  void setAsyncFlush(boolean value);
}
