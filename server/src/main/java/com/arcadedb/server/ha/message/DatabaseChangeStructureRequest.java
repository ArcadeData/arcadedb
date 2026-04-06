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
package com.arcadedb.server.ha.message;

import java.util.Map;

/**
 * Holds schema/file structure change information for replication via Ratis.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DatabaseChangeStructureRequest {
  private final String               databaseName;
  private final String               schemaJson;
  private final Map<Integer, String> filesToAdd;
  private final Map<Integer, String> filesToRemove;

  public DatabaseChangeStructureRequest(final String databaseName, final String schemaJson, final Map<Integer, String> filesToAdd,
      final Map<Integer, String> filesToRemove) {
    this.databaseName = databaseName;
    this.schemaJson = schemaJson;
    this.filesToAdd = filesToAdd;
    this.filesToRemove = filesToRemove;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getSchemaJson() {
    return schemaJson;
  }

  public Map<Integer, String> getFilesToAdd() {
    return filesToAdd;
  }

  public Map<Integer, String> getFilesToRemove() {
    return filesToRemove;
  }

  @Override
  public String toString() {
    return "dbchangestructure add=" + filesToAdd + " remove=" + filesToRemove;
  }
}
