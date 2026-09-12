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
package com.arcadedb.server.security;

import com.arcadedb.server.security.SecurityDocumentVersions.Document;

/**
 * A replicated security document entry was built from a version of the document that is no longer current
 * (issue #7509): somebody changed the same document on another node between this submitter's read and the
 * apply.
 * <p>
 * Nothing is installed. The point is that the outcome is REPORTED: before this existed the stale document was
 * installed on every node, so the concurrent change was reverted and neither request failed.
 * <p>
 * A {@link ServerSecurityException} so the HTTP layer already classifies it as a security failure rather than
 * as an internal error; the message names the document and both versions, because what the caller has to do
 * about it is re-read and reissue.
 */
public class SecurityDocumentConflictException extends ServerSecurityException {
  private final Document document;
  private final long     expectedVersion;
  private final long     currentVersion;

  public SecurityDocumentConflictException(final Document document, final long expectedVersion, final long currentVersion) {
    super("The replicated " + document.getDocumentFileName() + " document changed on another node while this change "
        + "was being submitted (it was built from version " + expectedVersion + ", the cluster is now at version "
        + currentVersion + "), so it was refused rather than applied over that change. Re-read the document and "
        + "reissue the change");
    this.document = document;
    this.expectedVersion = expectedVersion;
    this.currentVersion = currentVersion;
  }

  public Document getDocument() {
    return document;
  }

  public long getExpectedVersion() {
    return expectedVersion;
  }

  public long getCurrentVersion() {
    return currentVersion;
  }
}
