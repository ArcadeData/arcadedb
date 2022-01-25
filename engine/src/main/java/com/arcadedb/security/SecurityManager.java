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
package com.arcadedb.security;

import com.arcadedb.database.DatabaseInternal;

/**
 * Base security at server level. In the core package the default implementation throws unsupported exceptions, but if ArcadeDB is running on a server it uses the server security.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 **/
public interface SecurityManager {
  String ANY = "*";

  /**
   * Notifies the update of the schema.
   */
  void updateSchema(DatabaseInternal database);
}
