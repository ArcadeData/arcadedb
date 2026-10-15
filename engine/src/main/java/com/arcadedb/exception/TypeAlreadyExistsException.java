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
 * A type was asked to be created and one of that name is already there. A {@link SchemaException} of its own so that a statement
 * that says {@code IF NOT EXISTS} can tell "someone else created it between my check and my create" from every other schema failure
 * (issue #10067): the check and the create are two steps, and the only thing that can make them disagree is a concurrent creation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TypeAlreadyExistsException extends SchemaException {
  public TypeAlreadyExistsException(final String typeName) {
    super("Cannot create type '" + typeName + "' because already exists");
  }
}
