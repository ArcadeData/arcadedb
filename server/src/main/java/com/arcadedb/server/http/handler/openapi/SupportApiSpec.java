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
    openAPI.getPaths().addPathItem("/api/v1/server/support/preview", createPreviewPath());
    openAPI.getPaths().addPathItem("/api/v1/server/support/bundle", createBundlePath());
    openAPI.getPaths().addPathItem("/api/v1/server/support/issues", createIssuesPath());
    openAPI.getPaths().addPathItem("/api/v1/server/support/issues/{number}", createIssuePath());
    openAPI.getPaths().addPathItem("/api/v1/server/support/issues/{number}/comments", createCommentsPath());
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
    schema.setRequired(List.of("body"));
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
