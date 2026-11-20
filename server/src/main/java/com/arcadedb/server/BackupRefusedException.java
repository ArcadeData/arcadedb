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
package com.arcadedb.server;

/**
 * A backup command ({@code list backups}, {@code delete backup}, {@code restore backup}, {@code restore database}) refused because of
 * what the caller asked: a file name the command does not accept, an archive that is not there, a target database that is taken
 * without {@code overwrite}, an archive that cannot be read as a backup (issue #12406).
 * <p>
 * An {@link IllegalArgumentException}, which these refusals were raised as, so every caller that catches it keeps catching them. As
 * that plain type the HTTP layer answered all of them with the same text, {@code Cannot execute command}, and the operator could not
 * tell which one it was when the server conceals the detail (production mode). The message is the label of the answer, so the
 * reason survives there too.
 */
public class BackupRefusedException extends IllegalArgumentException {
  public BackupRefusedException(final String message) {
    super(message);
  }

  public BackupRefusedException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
