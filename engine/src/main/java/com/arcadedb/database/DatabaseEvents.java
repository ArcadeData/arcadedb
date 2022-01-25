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

import com.arcadedb.event.*;

public interface DatabaseEvents {
  DatabaseEvents registerListener(final BeforeRecordCreateListener listener);

  DatabaseEvents registerListener(final BeforeRecordUpdateListener listener);

  DatabaseEvents registerListener(final BeforeRecordDeleteListener listener);

  DatabaseEvents registerListener(final AfterRecordCreateListener listener);

  DatabaseEvents registerListener(final AfterRecordUpdateListener listener);

  DatabaseEvents registerListener(final AfterRecordDeleteListener listener);

  DatabaseEvents unregisterListener(final BeforeRecordCreateListener listener);

  DatabaseEvents unregisterListener(final BeforeRecordUpdateListener listener);

  DatabaseEvents unregisterListener(final BeforeRecordDeleteListener listener);

  DatabaseEvents unregisterListener(final AfterRecordCreateListener listener);

  DatabaseEvents unregisterListener(final AfterRecordUpdateListener listener);

  DatabaseEvents unregisterListener(final AfterRecordDeleteListener listener);
}
