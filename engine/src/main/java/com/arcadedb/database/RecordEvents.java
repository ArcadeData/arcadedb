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

import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.event.BeforeRecordReadListener;
import com.arcadedb.event.BeforeRecordUpdateListener;

public interface RecordEvents {
  RecordEvents registerListener(BeforeRecordCreateListener listener);

  RecordEvents registerListener(BeforeRecordReadListener listener);

  RecordEvents registerListener(BeforeRecordUpdateListener listener);

  RecordEvents registerListener(BeforeRecordDeleteListener listener);

  RecordEvents registerListener(AfterRecordCreateListener listener);

  RecordEvents registerListener(AfterRecordReadListener listener);

  RecordEvents registerListener(AfterRecordUpdateListener listener);

  RecordEvents registerListener(AfterRecordDeleteListener listener);

  RecordEvents unregisterListener(BeforeRecordCreateListener listener);

  RecordEvents unregisterListener(BeforeRecordReadListener listener);

  RecordEvents unregisterListener(BeforeRecordUpdateListener listener);

  RecordEvents unregisterListener(BeforeRecordDeleteListener listener);

  RecordEvents unregisterListener(AfterRecordCreateListener listener);

  RecordEvents unregisterListener(AfterRecordReadListener listener);

  RecordEvents unregisterListener(AfterRecordUpdateListener listener);

  RecordEvents unregisterListener(AfterRecordDeleteListener listener);
}
