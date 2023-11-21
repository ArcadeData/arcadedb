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
package com.arcadedb.remote;

import com.arcadedb.database.Binary;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ErrorRecordCallback;
import com.arcadedb.engine.RawRecordCallback;

import java.util.*;

/**
 * Document type used by {@link RemoteDatabase} class. The metadata are cached from the server until the schema is changed or
 * {@link RemoteSchema#reload()} is called.
 * <p>
 * This class is not thread safe. For multi-thread usage create one instance of RemoteDatabase per thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */

public class RemoteBucket implements Bucket {
  private final String name;

  public RemoteBucket(final String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public RID createRecord(Record record, boolean discardRecordAfter) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateRecord(Record record, boolean discardRecordAfter) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Binary getRecord(RID rid) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean existsRecord(RID rid) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteRecord(RID rid) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void scan(RawRecordCallback callback, ErrorRecordCallback errorRecordCallback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<Record> iterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long count() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getFileId() {
    throw new UnsupportedOperationException();
  }
}
