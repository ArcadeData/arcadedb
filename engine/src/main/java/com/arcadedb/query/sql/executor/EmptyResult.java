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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;

import java.util.*;

/**
 * Empty Result.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class EmptyResult implements Result {
  @Override
  public <T> T getProperty(String name) {
    return null;
  }

  @Override
  public <T> T getProperty(String name, Object defaultValue) {
    return (T) defaultValue;
  }

  @Override
  public Record getElementProperty(String name) {
    return null;
  }

  @Override
  public Set<String> getPropertyNames() {
    return null;
  }

  @Override
  public Optional<RID> getIdentity() {
    return Optional.empty();
  }

  @Override
  public boolean isElement() {
    return false;
  }

  @Override
  public Optional<Document> getElement() {
    return Optional.empty();
  }

  @Override
  public Document toElement() {
    return null;
  }

  @Override
  public Optional<Record> getRecord() {
    return Optional.empty();
  }

  @Override
  public boolean isProjection() {
    return false;
  }

  @Override
  public Object getMetadata(String key) {
    return null;
  }

  @Override
  public Set<String> getMetadataKeys() {
    return null;
  }

  @Override
  public Database getDatabase() {
    return null;
  }

  @Override
  public boolean hasProperty(String varName) {
    return false;
  }

  @Override
  public Map<String, Object> toMap() {
    return null;
  }
}
