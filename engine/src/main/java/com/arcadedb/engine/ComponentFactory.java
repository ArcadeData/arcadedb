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

import java.io.*;
import java.util.*;

public class ComponentFactory {
  private final Map<String, PaginatedComponentFactoryHandler> map = new HashMap<>();
  private final DatabaseInternal                              database;

  public interface PaginatedComponentFactoryHandler {
    Component createOnLoad(final DatabaseInternal database, final String name, final String filePath, final int id, final ComponentFile.MODE mode,
        final int pageSize, final int version) throws IOException;
  }

  public ComponentFactory(final DatabaseInternal database) {
    this.database = database;
  }

  public void registerComponent(final String fileExt, final PaginatedComponentFactoryHandler handler) {
    map.put(fileExt, handler);
  }

  public Component createComponent(final ComponentFile file, final ComponentFile.MODE mode) throws IOException {
    final String fileName = file.getComponentName();
    final int fileId = file.getFileId();
    final String fileExt = file.getFileExtension();
    final int pageSize = file instanceof PaginatedComponentFile ? ((PaginatedComponentFile) file).getPageSize() : 0;
    final int version = file.getVersion();

    final PaginatedComponentFactoryHandler handler = map.get(fileExt);
    if (handler != null)
      return handler.createOnLoad(database, fileName, file.getFilePath(), fileId, mode, pageSize, version);

    return null;
  }
}
