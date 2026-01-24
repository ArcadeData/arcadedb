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
package com.arcadedb.schema.trigger;

import com.arcadedb.database.Database;
import com.arcadedb.database.Record;

/**
 * Interface for user-implemented Java triggers.
 * <p>
 * Implementations of this interface can be registered as triggers using the SQL syntax:
 * <pre>
 * CREATE TRIGGER trigger_name BEFORE CREATE ON TYPE MyType
 * EXECUTE JAVA 'com.example.MyTriggerClass'
 * </pre>
 * <p>
 * The class must:
 * <ul>
 *   <li>Implement this interface</li>
 *   <li>Have a public no-argument constructor</li>
 *   <li>Be available on the classpath at runtime</li>
 * </ul>
 * <p>
 * Example implementation:
 * <pre>
 * public class MyValidationTrigger implements JavaTrigger {
 *   &#64;Override
 *   public boolean execute(Database database, Record record, Record oldRecord) {
 *     // Validate the record
 *     if (record.asDocument().get("email") == null) {
 *       throw new IllegalArgumentException("Email is required");
 *     }
 *     return true; // Continue operation
 *   }
 * }
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface JavaTrigger {

  /**
   * Execute the trigger logic.
   * <p>
   * This method is called when the trigger fires. It receives the database instance,
   * the current record being operated on, and (for UPDATE operations) the original record.
   *
   * @param database  The database instance where the operation is occurring
   * @param record    The current record being created/read/updated/deleted
   * @param oldRecord The original record before modification (only for UPDATE events, null otherwise)
   * @return true to continue the operation, false to abort it (for BEFORE triggers only)
   * @throws Exception if the operation should be aborted with an error
   */
  boolean execute(Database database, Record record, Record oldRecord) throws Exception;
}
