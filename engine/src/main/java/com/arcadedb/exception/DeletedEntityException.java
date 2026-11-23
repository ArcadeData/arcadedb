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
package com.arcadedb.exception;

/**
 * A statement that reads, writes or links an entity it deleted itself earlier on ({@code MATCH (n) DELETE n RETURN n.x}): openCypher's
 * {@code DeletedEntityAccess}. The caller's mistake, which no retry fixes: classified {@link ErrorCategory#NOT_FOUND}, the entity
 * the statement names being gone, as Neo4j answers it ({@code Neo.ClientError.Statement.EntityNotFound}); HTTP answers it 404.
 * As the plain {@link CommandExecutionException} it was raised as, no rule classified it and every protocol answered it as a server
 * fault (issue #11974).
 * <p>
 * Not a {@link RecordNotFoundException}: the engine catches that one around record loads to skip a dangling pointer, and a
 * deleted entity read in the same place must still fail the statement.
 */
public class DeletedEntityException extends CommandExecutionException {
  public DeletedEntityException(final String message) {
    super(message);
  }
}
