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
package com.arcadedb.engine;

import com.arcadedb.database.DatabaseInternal;

/**
 * Basic abstract File Component.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class Component {
  protected final DatabaseInternal database;
  protected final String           componentName;
  protected final int              fileId;
  protected final int              version;
  protected final String           filePath;

  protected Component(final DatabaseInternal database, final String componentName, final int id, final int version, final String filePath) {
    if (id < 0)
      throw new IllegalArgumentException("Invalid file id " + id);
    if (componentName == null || componentName.isEmpty())
      throw new IllegalArgumentException("Invalid file name " + componentName);

    this.database = database;
    this.componentName = componentName;
    this.fileId = id;
    this.version = version;
    this.filePath = filePath;
  }

  public abstract void close();

  public Object getMainComponent() {
    return this;
  }

  public void onAfterLoad() {
    // NO ACTIONS
  }

  public void onAfterCommit() {
    // NO ACTIONS
  }

  public void onAfterSchemaLoad() {
    // NO ACTIONS
  }

  public String getName() {
    return componentName;
  }

  public int getFileId() {
    return fileId;
  }

  public int getVersion() {
    return version;
  }

  public DatabaseInternal getDatabase() {
    return database;
  }
}
