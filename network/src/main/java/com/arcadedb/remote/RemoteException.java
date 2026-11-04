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

import com.arcadedb.exception.ArcadeDBException;

/**
 * A failure of a remote operation the driver has no more specific type for. An {@link ArcadeDBException}, like every
 * exception the engine raises (issue #11045): a {@code catch (ArcadeDBException)} written against the database interfaces
 * catches the same failures whether the database is embedded or remote, including one whose type the server named but this
 * client cannot rebuild (a class of a newer server, or of a plugin).
 */
public class RemoteException extends ArcadeDBException {
  public RemoteException(final String message) {
    super(message);
  }

  public RemoteException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
