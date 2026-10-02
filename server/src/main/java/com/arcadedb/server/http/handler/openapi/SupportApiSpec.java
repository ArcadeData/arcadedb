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
package com.arcadedb.server.http.handler.openapi;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;

import java.util.List;

/**
 * Documents the support endpoints of the Studio "Support" tab. Every one is for the server administrator (root); the server
 * holds the Client key of the ArcadeData customer portal and proxies the portal, so the key is never part of any request
 * or response, and a refusal of the portal is never answered with 401 or 403 (Studio reads a 401 as an expired session).
 */
public class SupportApiSpec implements OpenApiContributor {
  private static final String TAG = "Support";
  private static final String ERRORS = " Errors carry a code in 'error' (invalid_key, client_mismatch, scope_denied, "
      + "support_not_active, not_found, too_large, rate_limited, bad_request, portal_unreachable, portal_error, not_registered, "
      + "preview_not_found, bundle_too_large, preview_busy, support_stopped) and a clear message in 'message'.";

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem("/api/v1/server/support", createStatusPath());
    openAPI.getPaths().addPathItem("/api/v1/server/support/register", createRegisterPath());
    openAPI.getPaths().addPathItem("/api/v1/server/support/installation", createInstallationPath());
    openAPI.getPaths().addPathItem("/api/v1/server/support/connect", createConnectPath());
    openAPI.getPaths().addPathItem("/api/v1/server/support/preview", createPreviewPath());
    openAPI.getPaths().addPathItem("/api/v1/server/support/bundle", createBundlePath());
    openAPI.getPaths().addPathItem("/api/v1/server/support/issues", createIssuesPath());
    openAPI.getPaths().addPathItem("/api/v1/server/support/issues/{number}", createIssuePath());
    openAPI.getPaths().addPathItem("/api/v1/server/support/issues/{number}/comments", createCommentsPath());
    openAPI.getPaths().addPathItem("/api/v1/server/support/screenshots", createStageScreenshotPath());
    openAPI.getPaths().addPathItem("/api/v1/server/support/screenshots/{id}", createDiscardScreenshotPath());
    openAPI.getPaths().addPathItem("/api/v1/server/support/issues/{number}/requests/{requestId}/response", createAnswerRequestPath());
    openAPI.getPaths().addPathItem("/api/v1/server/support/issues/{number}/responses", createAnswerRequestsPath());
    openAPI.getPaths().addPathItem("/api/v1/server/support/issues/{number}/attachments", createAttachmentsPath());

    // Every operation is authenticated: the 401 of an unauthenticated request is declared once, here
    openAPI.getPaths().forEach((path, item) -> {
      if (path.startsWith("/api/v1/server/support"))
        item.readOperations().forEach(operation -> {
          if (operation.getResponses().get("401") == null)
            operation.getResponses().addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
        });
    });

    openAPI.getComponents().addSchemas("SupportStatus", createStatusSchema());
    openAPI.getComponents().addSchemas("SupportRegisterRequest", createRegisterRequestSchema());
    openAPI.getComponents().addSchemas("SupportPreviewRequest", createPreviewRequestSchema());
    openAPI.getComponents().addSchemas("SupportPreview", createPreviewSchema());
    openAPI.getComponents().addSchemas("SupportBundleRequest", createBundleRequestSchema());
    openAPI.getComponents().addSchemas("SupportCreateIssueRequest", createIssueRequestSchema());
    openAPI.getComponents().addSchemas("SupportCommentRequest", createCommentRequestSchema());
    openAPI.getComponents().addSchemas("SupportScreenshotRequest", createScreenshotSchema());
    openAPI.getComponents().addSchemas("SupportAnswerRequest", createAnswerSchema(false));
    openAPI.getComponents().addSchemas("SupportAnswersRequest", createAnswersSchema());
    openAPI.getComponents().addSchemas("SupportSetOpenRequest", createSetOpenRequestSchema());
    openAPI.getComponents().addSchemas("SupportAttachRequest", createAttachRequestSchema());
  }

  private PathItem createStatusPath() {
    final Operation get = SpecBuilders.operation("getSupportStatus", TAG, "Read the support registration of this server",
        "Whether the server is registered with the ArcadeData customer portal, and the workspace, plan and first-response "
            + "times the portal reports. The Client key is never returned, only its last four characters ('keyHint'). "
            + "Restricted to the root user." + ERRORS);
    get.addParametersItem(SpecBuilders.queryParam("refresh", "true to ask the portal again instead of using the answer cached "
        + "for one minute", false));
    get.setResponses(SpecBuilders.standardResponses("200", SpecBuilders.jsonResponse("Support status", "SupportStatus"), "403", "500"));
    final PathItem item = new PathItem();
    item.setGet(get);
    return item;
  }

  private PathItem createRegisterPath() {
    final Operation post = SpecBuilders.operation("registerSupport", TAG, "Register this server with the support portal",
        "Verifies the Client ID and key with the portal ('whoami') and stores them in support.json of the server configuration "
            + "directory (owner-only permissions). Restricted to the root user." + ERRORS);
    post.setRequestBody(SpecBuilders.jsonBody("Client ID and key", "SupportRegisterRequest", true));
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", SpecBuilders.jsonResponse("Registered: the same document as GET /server/support", "SupportStatus"));
    responses.addApiResponse("400", SpecBuilders.errorResponse("The portal refused the Client ID or key, or a value is not valid"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden: only the root user may register"));
    responses.addApiResponse("409", SpecBuilders.errorResponse("The registration comes from the settings, or the configuration "
        + "directory is not writable"));
    responses.addApiResponse("503", SpecBuilders.errorResponse("The portal cannot be reached"));
    post.setResponses(responses);

    final Operation delete = SpecBuilders.operation("unregisterSupport", TAG, "Remove the support registration",
        "Deletes support.json. A registration configured through the settings arcadedb.support.clientId and "
            + "arcadedb.support.clientKey cannot be removed here. Restricted to the root user.");
    delete.setResponses(SpecBuilders.standardResponses("204", SpecBuilders.emptyResponse("Unregistered"), "403", "409"));

    final PathItem item = new PathItem();
    item.setPost(post);
    item.setDelete(delete);
    return item;
  }

  private PathItem createInstallationPath() {
    final Operation post = SpecBuilders.operation("registerSupportInstallation", TAG, "Register this server as an installation in the portal",
        "Sends the redacted diagnostics of this server to the portal, which creates the installation in the workspace of the "
            + "Client key, or completes the blank fields of the one it already has. Answers {status: created|updated|unchanged, "
            + "installationId, name, filled, differs}. Restricted to the root user." + ERRORS);
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", SpecBuilders.emptyResponse("What the portal did with the installation"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden: only the root user may register"));
    responses.addApiResponse("409", SpecBuilders.errorResponse("The server is not registered, or the portal refused the identity "
        + "it reported"));
    responses.addApiResponse("503", SpecBuilders.errorResponse("The portal cannot be reached"));
    post.setResponses(responses);
    final PathItem item = new PathItem();
    item.setPost(post);
    return item;
  }

  private PathItem createConnectPath() {
    final Operation post = SpecBuilders.operation("startSupportConnect", TAG, "Start connecting this server to the portal",
        "Asks the portal for a code ('device authorization'): answers {userCode, verifyUrl, expiresIn}. Studio shows the code and "
            + "opens verifyUrl in a new tab; once a workspace owner or admin approves it there, the server receives the workspace "
            + "key (never shown to the browser), stores it as a registration and registers itself as an installation. One "
            + "connection waits at a time. Restricted to the root user (HTTP Basic). Without Studio, from a shell or the console "
            + "('connect portal'): `curl -s -u root:PASSWORD -X POST -H 'Content-Type: application/json' -d '{\"label\":\"prod-1\"}' "
            + "http://localhost:2480/api/v1/server/support/connect` answers {\"userCode\":\"WDJB-MJHT\",\"verifyUrl\":"
            + "\"https://portal.arcadedb.com/#/connect?code=WDJB-MJHT\",\"expiresIn\":600}; open verifyUrl in any browser, check "
            + "that the code matches and approve; then `curl -s -u root:PASSWORD http://localhost:2480/api/v1/server/support/connect` "
            + "until status is no longer 'pending' (every 2 seconds is plenty); `curl -s -u root:PASSWORD -X DELETE "
            + "http://localhost:2480/api/v1/server/support/connect` stops waiting (204). The optional body field 'label' (up to 60 "
            + "characters) names the key in the portal." + ERRORS);
    final ApiResponses created = new ApiResponses();
    created.addApiResponse("200", SpecBuilders.emptyResponse("The code to show: {userCode, verifyUrl, expiresIn}"));
    created.addApiResponse("403", SpecBuilders.errorResponse("Forbidden: only the root user may connect"));
    created.addApiResponse("404", SpecBuilders.errorResponse("The portal cannot connect servers from Studio yet"));
    created.addApiResponse("409", SpecBuilders.errorResponse("A connection is already waiting (connect_in_progress), the registration "
        + "comes from the settings, or the configuration directory is not writable"));
    created.addApiResponse("429", SpecBuilders.errorResponse("The portal refuses too many attempts from this server"));
    created.addApiResponse("503", SpecBuilders.errorResponse("The portal cannot be reached"));
    post.setResponses(created);

    final Operation get = SpecBuilders.operation("getSupportConnect", TAG, "State of the connection to the portal",
        "{status: none|pending|connected|expired|denied|error|cancelled}; pending carries userCode, verifyUrl and expiresOn; "
            + "connected carries workspaceName and registration (the outcome of registering the installation); error carries "
            + "{error, message}. Restricted to the root user.");
    get.setResponses(SpecBuilders.standardResponses("200", SpecBuilders.emptyResponse("The state of the last connection"), "403"));

    final Operation delete = SpecBuilders.operation("cancelSupportConnect", TAG, "Stop waiting for the approval",
        "Ends the wait. A key that was already received stays registered. Restricted to the root user.");
    delete.setResponses(SpecBuilders.standardResponses("204", SpecBuilders.emptyResponse("Stopped"), "403"));

    final PathItem item = new PathItem();
    item.setPost(post);
    item.setGet(get);
    item.setDelete(delete);
    return item;
  }

  private PathItem createPreviewPath() {
    final Operation post = SpecBuilders.operation("previewSupportBundle", TAG, "Build and preview the redacted support bundle",
        "Collects the logs of a time window, the diagnostics snapshot and optionally a thread dump into a temporary directory, "
            + "with secrets redacted BEFORE anything is written, and describes them: files, sizes, line counts and redaction "
            + "counts per file, warnings. The preview lives 15 minutes; POST /server/support/issues sends exactly these files and "
            + "POST /server/support/bundle downloads them. Log lines carry no time zone: they are written in the time zone of "
            + "the server JVM, reported in 'logTimeZone', and the window is converted to it. An empty window is reported in "
            + "'warnings', not as an error; a window over 100 MB zipped is refused with 413 bundle_too_large. Works without "
            + "registration. Restricted to the root user." + ERRORS);
    post.setRequestBody(SpecBuilders.jsonBody("What to include", "SupportPreviewRequest", true));
    post.setResponses(SpecBuilders.standardResponses("200", SpecBuilders.jsonResponse("The preview", "SupportPreview"), "400", "403",
        "413", "500"));
    final PathItem item = new PathItem();
    item.setPost(post);
    return item;
  }

  private PathItem createBundlePath() {
    final Operation post = SpecBuilders.operation("downloadSupportBundle", TAG, "Download the redacted support bundle",
        "Streams the files of a preview as one zip (logs under logs/, diagnostics.json, summary.json, threads.txt): for the "
            + "public GitHub path and offline sharing. Nothing is uploaded anywhere. Restricted to the root user.");
    post.setRequestBody(SpecBuilders.jsonBody("The preview to download", "SupportBundleRequest", true));
    final ApiResponse zip = new ApiResponse();
    zip.setDescription("The zip file");
    zip.setContent(new Content().addMediaType("application/zip", new MediaType().schema(new Schema<>().type("string").format("binary"))));
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", zip);
    responses.addApiResponse("400", SpecBuilders.errorResponse("Bad request"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("The preview does not exist or has expired"));
    post.setResponses(responses);
    final PathItem item = new PathItem();
    item.setPost(post);
    return item;
  }

  private PathItem createIssuesPath() {
    final Operation get = SpecBuilders.operation("listSupportIssues", TAG, "List the support issues of the workspace",
        "Proxy of the portal list (only public timeline data). The body is the portal's, unchanged, with the Client key "
            + "scrubbed. Restricted to the root user." + ERRORS);
    get.addParametersItem(SpecBuilders.queryParam("status", "open, closed or all (default open)", false));
    get.setResponses(portalResponses("200", SpecBuilders.jsonResponse("The issues, as the portal answers", null)));

    final Operation post = SpecBuilders.operation("createSupportIssue", TAG, "Open a support issue",
        "Opens an issue in the portal, attaching the files of the preview 'previewId' (when given) exactly as previewed. "
            + "Needs a registration whose plan is active (402 support_not_active otherwise). Restricted to the root user." + ERRORS);
    post.setRequestBody(SpecBuilders.jsonBody("The issue", "SupportCreateIssueRequest", true));
    post.setResponses(portalResponses("201", SpecBuilders.jsonResponse("The number and the portal link of the issue", null)));

    final PathItem item = new PathItem();
    item.setGet(get);
    item.setPost(post);
    return item;
  }

  private PathItem createIssuePath() {
    final Operation get = SpecBuilders.operation("getSupportIssue", TAG, "Read a support issue",
        "Proxy of the portal issue with its public timeline. Restricted to the root user." + ERRORS);
    get.addParametersItem(SpecBuilders.pathParam("number", "Issue number"));
    get.setResponses(portalResponses("200", SpecBuilders.jsonResponse("The issue, as the portal answers", null)));

    final Operation put = SpecBuilders.operation("setSupportIssueOpen", TAG, "Close or reopen a support issue",
        "Restricted to the root user." + ERRORS);
    put.addParametersItem(SpecBuilders.pathParam("number", "Issue number"));
    put.setRequestBody(SpecBuilders.jsonBody("Open or closed", "SupportSetOpenRequest", true));
    put.setResponses(portalResponses("204", SpecBuilders.emptyResponse("Done")));

    final PathItem item = new PathItem();
    item.setGet(get);
    item.setPut(put);
    return item;
  }

  private PathItem createCommentsPath() {
    final Operation post = SpecBuilders.operation("commentSupportIssue", TAG, "Reply on a support issue",
        "Restricted to the root user." + ERRORS);
    post.addParametersItem(SpecBuilders.pathParam("number", "Issue number"));
    post.setRequestBody(SpecBuilders.jsonBody("The reply", "SupportCommentRequest", true));
    post.setResponses(portalResponses("201", SpecBuilders.jsonResponse("The timeline entry, as the portal answers", null)));
    final PathItem item = new PathItem();
    item.setPost(post);
    return item;
  }

  private PathItem createStageScreenshotPath() {
    final Operation post = SpecBuilders.operation("stageSupportScreenshot", TAG, "Hold a screenshot until it is sent",
        "A user pastes, drops or picks a picture of what they see (a query result, an error). It is held in memory for 15 minutes, "
            + "checked by its first bytes (PNG, JPEG, GIF or WebP; never SVG), at most 5 MB, and answered by id so the issue, the reply "
            + "or the files can refer to it in `screenshots`. Nothing is sent to ArcadeData until then. Restricted to the root user."
            + ERRORS);
    post.setRequestBody(SpecBuilders.jsonBody("The picture", "SupportScreenshotRequest", true));
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("201", SpecBuilders.jsonResponse("{id, type, size}", null));
    responses.addApiResponse("400", SpecBuilders.errorResponse("Not a PNG, JPEG, GIF or WebP image, or too many are waiting"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden: only the root user"));
    responses.addApiResponse("413", SpecBuilders.errorResponse("The picture is larger than 5 MB"));
    post.setResponses(responses);
    final PathItem item = new PathItem();
    item.setPost(post);
    return item;
  }

  private PathItem createDiscardScreenshotPath() {
    final Operation delete = SpecBuilders.operation("discardSupportScreenshot", TAG, "Remove a held screenshot",
        "The user removed it before sending. Unknown ids are not an error. Restricted to the root user.");
    delete.addParametersItem(SpecBuilders.pathParam("id", "Screenshot id"));
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("204", new ApiResponse().description("Removed"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden: only the root user"));
    delete.setResponses(responses);
    final PathItem item = new PathItem();
    item.setDelete(delete);
    return item;
  }

  private PathItem createAnswerRequestPath() {
    final Operation post = SpecBuilders.operation("answerSupportRequest", TAG, "Answer a support request",
        "Staff can ask for the result of a read-only query. The browser runs it through the ordinary query endpoint, then sends the "
            + "result (or a decline, or the failure) here and the server forwards it to the portal, which turns it into a client "
            + "comment. Restricted to the root user." + ERRORS);
    post.addParametersItem(SpecBuilders.pathParam("number", "Issue number"));
    post.addParametersItem(SpecBuilders.pathParam("requestId", "Request id (rq_ and 8 hex digits)"));
    post.setRequestBody(SpecBuilders.jsonBody("The answer", "SupportAnswerRequest", true));
    post.setResponses(portalResponses("201", SpecBuilders.jsonResponse("The client comment, as the portal answers", null)));
    post.getResponses().addApiResponse("409", SpecBuilders.errorResponse("The request was already answered"));
    final PathItem item = new PathItem();
    item.setPost(post);
    return item;
  }

  private PathItem createAnswerRequestsPath() {
    final Operation post = SpecBuilders.operation("answerSupportRequests", TAG, "Answer several support requests at once",
        "As answering one, for \"Run all\": the portal writes ONE comment. Restricted to the root user." + ERRORS);
    post.addParametersItem(SpecBuilders.pathParam("number", "Issue number"));
    post.setRequestBody(SpecBuilders.jsonBody("The answers", "SupportAnswersRequest", true));
    post.setResponses(portalResponses("201", SpecBuilders.jsonResponse("The client comment, as the portal answers", null)));
    post.getResponses().addApiResponse("409", SpecBuilders.errorResponse("A request was already answered"));
    final PathItem item = new PathItem();
    item.setPost(post);
    return item;
  }

  private PathItem createAttachmentsPath() {
    final Operation post = SpecBuilders.operation("attachToSupportIssue", TAG, "Send more files to a support issue",
        "Sends the files of a preview to an existing issue. Restricted to the root user." + ERRORS);
    post.addParametersItem(SpecBuilders.pathParam("number", "Issue number"));
    post.setRequestBody(SpecBuilders.jsonBody("The preview to send", "SupportAttachRequest", true));
    post.setResponses(portalResponses("200", SpecBuilders.jsonResponse("The attachments, as the portal answers", null)));
    final PathItem item = new PathItem();
    item.setPost(post);
    return item;
  }

  private static ApiResponses portalResponses(final String success, final ApiResponse ok) {
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse(success, ok);
    responses.addApiResponse("400", SpecBuilders.errorResponse("Bad request"));
    responses.addApiResponse("402", SpecBuilders.errorResponse("The support plan is not active"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden: only the root user"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("The issue or the preview was not found"));
    responses.addApiResponse("409", SpecBuilders.errorResponse("The server is not registered"));
    responses.addApiResponse("413", SpecBuilders.errorResponse("The upload is too large"));
    responses.addApiResponse("429", SpecBuilders.errorResponse("Rate limited by the portal; see Retry-After"));
    responses.addApiResponse("502", SpecBuilders.errorResponse("The portal refused the key of this server (invalid_key, "
        + "client_mismatch, scope_denied) or answered with an error"));
    responses.addApiResponse("503", SpecBuilders.errorResponse("The portal cannot be reached"));
    return responses;
  }

  private Schema<?> createStatusSchema() {
    final Schema<Object> schema = SpecBuilders.object("The registration of this server with the support portal");
    schema.addProperty("registered", SpecBuilders.bool("Whether a Client ID and key are configured"));
    schema.addProperty("clientId", SpecBuilders.string("The Client ID (workspace id)"));
    schema.addProperty("keyHint", SpecBuilders.string("The last four characters of the key, prefixed by an ellipsis: all that is ever shown"));
    schema.addProperty("workspaceName", SpecBuilders.string("Name of the workspace, from the portal"));
    schema.addProperty("plan", SpecBuilders.freeFormObject("{entitled, label, units, endsOn} as the portal reports it"));
    schema.addProperty("sla", SpecBuilders.freeFormObject("First-response times {S1, S2, S3, S4, coverage}, or null"));
    schema.addProperty("portalUrl", SpecBuilders.string("The portal URL in use"));
    schema.addProperty("canWriteConfig", SpecBuilders.bool("Whether the configuration directory accepts the registration file"));
    schema.addProperty("instanceId", SpecBuilders.string("The instance id of this server"));
    schema.addProperty("fromSettings", SpecBuilders.bool("The registration comes from the settings, not from support.json"));
    schema.addProperty("portalError", SpecBuilders.freeFormObject("Present when the portal did not answer: {error, message}"));
    schema.addProperty("logTimeZone", SpecBuilders.freeFormObject("The time zone the log timestamps are written in: {id, name, offset, note}"));
    schema.setRequired(List.of("registered", "portalUrl", "canWriteConfig"));
    return schema;
  }

  private Schema<?> createRegisterRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("Credentials of the customer portal");
    schema.addProperty("clientId", SpecBuilders.string("The Client ID (workspace id) of the portal"));
    schema.addProperty("key", SpecBuilders.string("The Client key ('wsk_...') created in the portal. Never returned by any API"));
    schema.addProperty("verifyOnly", SpecBuilders.bool("true to check the credentials with the portal without storing them: "
        + "answers {verified, workspaceName, plan, sla, keyLabel, scopes}"));
    schema.setRequired(List.of("clientId", "key"));
    return schema;
  }

  private Schema<?> createPreviewRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("What to put in the bundle");
    schema.addProperty("includeLogs", SpecBuilders.bool("Include the logs of the window (default false)"));
    schema.addProperty("window", SpecBuilders.freeFormObject("{preset: 10m|30m|1h|12h|24h|1w} or {from, to} in ISO-8601. A value "
        + "without an offset is read in the time zone of the log"));
    schema.addProperty("includeDiagnostics", SpecBuilders.bool("Include diagnostics.json (default true)"));
    schema.addProperty("includeThreads", SpecBuilders.bool("Include a thread dump, threads.txt (default false)"));
    return schema;
  }

  private Schema<?> createPreviewSchema() {
    final Schema<Object> schema = SpecBuilders.object("The redacted files, ready to send or download");
    schema.addProperty("previewId", SpecBuilders.string("Identifier of the preview, valid 15 minutes"));
    schema.addProperty("expiresAt", SpecBuilders.string("When the preview is deleted"));
    schema.addProperty("files", SpecBuilders.arrayOf(SpecBuilders.freeFormObject("{name, sizeBytes, lines, redactions, entries?}"),
        "One entry per file"));
    schema.addProperty("warnings", SpecBuilders.arrayOf(SpecBuilders.string("A warning"), "Warnings, e.g. an empty window"));
    schema.addProperty("window", SpecBuilders.freeFormObject("{from, to} as instants, when logs were requested"));
    schema.addProperty("logTimeZone", SpecBuilders.freeFormObject("{id, name, offset, note}"));
    schema.addProperty("githubSummary", SpecBuilders.string("Markdown of the environment and log summary for a public GitHub issue, without logs"));
    schema.setRequired(List.of("previewId", "files", "warnings"));
    return schema;
  }

  private Schema<?> createBundleRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("The preview to download");
    schema.addProperty("previewId", SpecBuilders.string("Identifier returned by the preview"));
    schema.setRequired(List.of("previewId"));
    return schema;
  }

  private Schema<?> createIssueRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("A support issue");
    schema.addProperty("previewId", SpecBuilders.string("Preview whose files are attached, optional"));
    schema.addProperty("title", SpecBuilders.string("1 to 200 characters"));
    schema.addProperty("body", SpecBuilders.string("At most 20000 characters"));
    schema.addProperty("severity", SpecBuilders.string("S1, S2, S3 or S4"));
    schema.addProperty("kind", SpecBuilders.string("bug, question, performance or other, optional"));
    schema.setRequired(List.of("title", "severity"));
    return schema;
  }

  private Schema<?> createCommentRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("A reply");
    schema.addProperty("body", SpecBuilders.string("At most 20000 characters"));
    schema.addProperty("screenshots", SpecBuilders.object("Ids of held screenshots (at most 5) the reply shows: they are attached to the issue first"));
    schema.setRequired(List.of("body"));
    return schema;
  }

  private Schema<?> createScreenshotSchema() {
    final Schema<Object> schema = SpecBuilders.object("A screenshot");
    schema.addProperty("data", SpecBuilders.string("The picture, base64 encoded (at most 5 MB decoded)"));
    schema.setRequired(List.of("data"));
    return schema;
  }

  private Schema<?> createAnswerSchema(final boolean withRequestId) {
    final Schema<Object> schema = SpecBuilders.object("The answer to a support request");
    if (withRequestId)
      schema.addProperty("requestId", SpecBuilders.string("The request being answered (rq_ and 8 hex digits)"));
    schema.addProperty("outcome", SpecBuilders.string("answered, declined or failed"));
    schema.addProperty("result", SpecBuilders.object("answered only: {columns: [{name, type}], rows: [[...]], truncated, masked: "
        + "{cells: [[row, column]], columns: [name], mode: redact|hash}}. Masked values are replaced before they are sent; "
        + "either result or text, not both"));
    schema.addProperty("text", SpecBuilders.string("answered only: pasted text instead of a result, at most 20000 characters"));
    schema.addProperty("reason", SpecBuilders.string("declined, or failed: why, at most 500 characters"));
    schema.addProperty("durationMs", SpecBuilders.integer("How long the query ran"));
    schema.setRequired(withRequestId ? List.of("requestId", "outcome") : List.of("outcome"));
    return schema;
  }

  private Schema<?> createAnswersSchema() {
    final Schema<Object> schema = SpecBuilders.object("Several answers, written as one comment");
    schema.addProperty("responses", SpecBuilders.object("A list of 1 to 20 answers, each as SupportAnswerRequest with its requestId"));
    schema.setRequired(List.of("responses"));
    return schema;
  }

  private Schema<?> createSetOpenRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("Close or reopen");
    schema.addProperty("open", SpecBuilders.bool("true to reopen, false to close"));
    schema.setRequired(List.of("open"));
    return schema;
  }

  private Schema<?> createAttachRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("The preview to send to the issue");
    schema.addProperty("previewId", SpecBuilders.string("Identifier returned by the preview"));
    schema.setRequired(List.of("previewId"));
    return schema;
  }
}
