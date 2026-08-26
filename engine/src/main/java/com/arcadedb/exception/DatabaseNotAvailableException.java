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
 * No open database handle exists for the requested name and the caller forbade loading one
 * (`allowLoad=false`) - either the database was never opened, or it was closed/dropped after the
 * caller last resolved it. Distinct from the generic {@link DatabaseOperationException} it extends so
 * callers that need to answer this specific condition (e.g. an HTTP 404) do not have to match on every
 * other unrelated failure that type also carries. See issue #6778.
 */
public class DatabaseNotAvailableException extends DatabaseOperationException {
  public DatabaseNotAvailableException(final String s) {
    super(s);
  }
}
