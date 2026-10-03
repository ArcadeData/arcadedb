/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

// ===== AI Assistant Module =====

var aiCurrentChatId = null;
var aiMessages = [];
var aiChatList = [];
var aiConfigured = false;
var aiSending = false;
var aiCurrentXhr = null;
var aiCommandBlockCounter = 0;
var aiMode = globalStorageLoad("ai-mode", "auto");

// Protocol version this Studio bundle speaks to the AI server. Sent on every
// /api/v1/ai/chat request so the server can manage breaking changes without
// forcing all browsers and servers to upgrade in lock-step. Keep in sync with
// AiProtocol.CURRENT_VERSION on the Java side.
var AI_PROTOCOL_VERSION = 1;

function initAi() {
  // Check if AI is configured
  jQuery.ajax({
    type: "GET",
    url: "api/v1/ai/config",
    beforeSend: function(xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    }
  })
  .done(function(data) {
    aiConfigured = data.configured === true;
    aiPortalInfo = data.portal || null;
    if (aiConfigured) {
      // Surface an obvious warning if our bundle's protocol version isn't in the
      // server's supported list. We still try to send the request; the server
      // returns a clear protocol_unsupported error if it really can't speak v1.
      if (Array.isArray(data.supportedProtocolVersions)
          && data.supportedProtocolVersions.indexOf(AI_PROTOCOL_VERSION) === -1) {
        globalNotify("AI Assistant",
          "This Studio bundle uses protocol v" + AI_PROTOCOL_VERSION
            + " but the server supports " + data.supportedProtocolVersions.join(", ")
            + ". Reload Studio or update the server.",
          "warning");
      }
      $("#aiInactivePanel").hide();
      $("#aiActivePanel").show();
      if (typeof supportFitAi === "function") supportFitAi();
      aiShowUsage(data.source === "portal" ? aiPortalInfo : null);
      aiHideNotice();
      initSearchableDbSelect("aiDbSelectContainer");
      aiApplyMode();
      aiLoadChatList();
    } else {
      aiShowInactive();
    }
  })
  .fail(function() {
    aiPortalInfo = null;
    aiShowInactive();
  });
}

// ===== The portal: connect, plan, allowance =====

/** What the server said about the customer portal (connected, enabled, tier, spent, budget, percent, upgradeUrl...), or null. */
var aiPortalInfo = null;

/** Only an http(s) address goes into a link: the URL comes from the server, but a link is a place to be careful. */
function aiSafeUrl(url) {
  return typeof url === "string" && /^https?:\/\//i.test(url) ? url : null;
}

/** The inactive page: connect this server to the portal, or say why the plan cannot use the assistant. */
function aiShowInactive() {
  $("#aiInactivePanel").show();
  $("#aiActivePanel").hide();

  var box = $("#aiPortalState").empty();
  var card = $('<div style="padding: 1.5rem; border: 1px solid var(--border-main); border-radius: 10px; background: var(--bg-card);"></div>');
  var info = aiPortalInfo;

  if (!info || !info.connected) {
    card.append($('<h6 style="color: var(--text-primary); margin-bottom: 0.75rem;"></h6>').text("Connect to the ArcadeDB customer portal"));
    card.append($('<p style="color: var(--text-muted); font-size: 0.85rem; margin-bottom: 1rem;"></p>')
      .text("The AI Assistant is part of your ArcadeDB plan. Connect this server to your workspace in the customer portal and it is enabled."));
    card.append($('<button class="btn" style="background: var(--color-brand); color: white; border: none;"></button>')
      .text("Connect to ArcadeDB Portal").on("click", function () {
        if (typeof showSupportView === "function") showSupportView("overview");
      }));
  } else if (info.code) {
    // Connected, but the portal could not tell the plan (a rejected key, no network): say so and let the user retry
    card.append($('<h6 style="color: var(--text-primary); margin-bottom: 0.75rem;"></h6>').text("The portal could not be asked"));
    card.append($('<p style="color: var(--text-muted); font-size: 0.85rem; margin-bottom: 1rem;"></p>').text(info.message || info.code));
    card.append($('<button class="btn btn-sm btn-outline-secondary"></button>').text("Check again").on("click", function () { initAi(); }));
  } else {
    card.append($('<h6 style="color: var(--text-primary); margin-bottom: 0.75rem;"></h6>').text("Your plan does not include the AI Assistant"));
    card.append($('<p style="color: var(--text-muted); font-size: 0.85rem; margin-bottom: 1rem;"></p>')
      .text("This server is connected to your workspace, but its plan does not include the AI Assistant, or the plan is not active. Review your plan in the customer portal."));
    var link = aiSafeUrl(info.upgradeUrl);
    if (link)
      card.append($('<a class="btn me-2" target="_blank" rel="noopener noreferrer" style="background: var(--color-brand); color: white; border: none;"></a>')
        .attr("href", link).text("Review your plan"));
    card.append($('<button class="btn btn-sm btn-outline-secondary"></button>').text("Check again").on("click", function () { initAi(); }));
  }
  box.append(card);
}

/** "$3.41 of $20.00 AI use this month" next to the mode switch, when the answers come from the portal. Billed dollars. */
function aiShowUsage(info) {
  var el = $("#aiUsage");
  if (!info || typeof info.budget !== "number" || typeof info.spent !== "number") {
    el.hide();
    return;
  }
  var money = function (n) { return "$" + n.toFixed(2); };
  var percent = typeof info.percent === "number" ? info.percent : (info.budget > 0 ? Math.floor(info.spent / info.budget * 100) : 0);
  var text = money(info.spent) + " of " + money(info.budget) + " AI use this month";
  if (percent >= 100)
    text += " - used up";
  else if (percent >= 80)
    text += " - almost used up";
  el.text(text).attr("title", info.resetsOn ? "Starts over on " + info.resetsOn : "")
    .css("color", percent >= 100 ? "#dc3545" : percent >= 80 ? "#fd7e14" : "").show();
}

function aiHideNotice() {
  $("#aiNotice").hide().empty();
}

/** A refusal worth reading, with the link that fixes it (the plan page of the portal) when the server sent one. */
function aiShowNotice(message, upgrade) {
  var el = $("#aiNotice").empty().append($("<span></span>").text(message));
  var link = upgrade && aiPortalInfo ? aiSafeUrl(aiPortalInfo.upgradeUrl) : null;
  if (link)
    el.append(" ").append($('<a target="_blank" rel="noopener noreferrer"></a>').attr("href", link).text("Review your plan"));
  el.show();
}

/** Codes that mean "this server cannot use the assistant any more": back to the inactive page, with the reason. */
function aiIsAccessError(code) {
  return code === "ai.not_entitled" || code === "invalid_key" || code === "client_mismatch" || code === "scope_denied"
    || code === "support_not_active";
}

/**
 * The failure of an answer, from the HTTP error body or the 'error' event of the stream. Returns true when it was handled
 * (the caller shows nothing else).
 */
function aiHandleFailure(code, message, upgrade) {
  if (code === "ai.allowance_exhausted") {
    aiShowNotice(message || "You have used the AI allowance of your plan for this month.", true);
    initAi();
    return true;
  }
  if (aiIsAccessError(code)) {
    aiConfigured = false;
    initAi();
    globalNotify("AI Assistant", message || "The AI Assistant is not available for this server.", "warning");
    return true;
  }
  if (code === "ai.busy") {
    aiShowNotice(message || "The AI Assistant is busy. Try again in a moment.", false);
    return true;
  }
  return false;
}

// ===== Mode Toggle =====

function aiSetMode(mode) {
  aiMode = mode;
  globalStorageSave("ai-mode", mode);
  aiApplyMode();
}

function aiApplyMode() {
  $("#aiModeToggle button").each(function() {
    var btn = $(this);
    if (btn.data("mode") === aiMode) {
      btn.css({ "background": "var(--color-brand)", "color": "white", "border-color": "var(--color-brand)" });
    } else {
      btn.css({ "background": "transparent", "color": "var(--text-muted)", "border-color": "var(--border-main)" });
    }
  });
}

// ===== Activation =====

function aiActivate() {
  var key = $("#aiSubscriptionKey").val().trim();
  if (!key) {
    $("#aiActivateError").text("Please enter a subscription key.").show();
    return;
  }

  $("#aiActivateError").hide();
  $("#aiActivateSuccess").hide();

  // Show disclaimer before activating
  var modal = document.getElementById("aiDisclaimerModal");
  if (!modal) {
    var div = document.createElement("div");
    div.innerHTML =
      '<div class="modal fade" id="aiDisclaimerModal" tabindex="-1" data-bs-backdrop="static" data-bs-keyboard="false">' +
      '<div class="modal-dialog modal-lg modal-dialog-scrollable"><div class="modal-content" style="background: var(--bg-card); color: var(--text-primary);">' +
      '<div class="modal-header"><h5 class="modal-title">AI Assistant - Terms of Use</h5></div>' +
      '<div class="modal-body" style="font-size: 0.9rem; line-height: 1.6;">' +
      '<p>By activating and using the ArcadeDB AI Assistant (the "<b>Service</b>"), you acknowledge and agree to the following terms:</p>' +
      '<p><b>1. Data Transmission.</b> To provide AI-powered assistance, certain information from your database — including but not limited to ' +
      'schema definitions, metadata, query structures, and data excerpts — may be transmitted to third-party Large Language Model (LLM) ' +
      'providers for processing. You understand and consent to such transmission as a necessary condition of using the Service.</p>' +
      '<p><b>2. No Training on Your Data.</b> Your data is <b>never</b> used for training any AI or machine learning model. ' +
      'Your data is <b>never</b> stored by the LLM provider beyond the duration of the request processing.</p>' +
      '<p><b>3. Use of Test Data Recommended.</b> For maximum privacy protection, Arcade Data Ltd strongly recommends ' +
      'using the AI Assistant exclusively with non-production, test, or anonymized datasets. You are solely responsible for ' +
      'determining the suitability of any data you expose to the Service.</p>' +
      '<p><b>4. Limitation of Liability.</b> TO THE FULLEST EXTENT PERMITTED BY APPLICABLE LAW, ARCADE DATA LTD AND ITS AFFILIATES, ' +
      'OFFICERS, DIRECTORS, EMPLOYEES, AND AGENTS SHALL NOT BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, CONSEQUENTIAL, ' +
      'OR EXEMPLARY DAMAGES ARISING FROM OR RELATED TO: (A) ANY UNAUTHORIZED ACCESS TO, DISCLOSURE OF, OR LOSS OF YOUR DATABASE DATA ' +
      'OR PERSONAL INFORMATION TRANSMITTED THROUGH THE SERVICE; (B) ANY BREACH OF CONFIDENTIALITY OCCURRING DURING DATA TRANSMISSION ' +
      'TO OR PROCESSING BY THIRD-PARTY LLM PROVIDERS; OR (C) ANY RELIANCE ON AI-GENERATED RESPONSES, QUERIES, OR RECOMMENDATIONS.</p>' +
      '<p><b>5. User Responsibility.</b> You are solely responsible for ensuring compliance with all applicable data protection ' +
      'regulations (including but not limited to GDPR, CCPA, and equivalent legislation) with respect to any data you submit to the Service. ' +
      'You represent and warrant that you have all necessary rights and authorizations to transmit such data.</p>' +
      '<p><b>6. Acceptance.</b> By clicking "<b>I Agree</b>" below, you confirm that you have read, understood, and agree to be bound by these terms.</p>' +
      '</div>' +
      '<div class="modal-footer">' +
      '<button type="button" class="btn btn-secondary" data-bs-dismiss="modal" id="aiDisclaimerDecline">Decline</button>' +
      '<button type="button" class="btn btn-primary" id="aiDisclaimerAgree" style="background: var(--color-brand); border-color: var(--color-brand);">I Agree</button>' +
      '</div></div></div></div>';
    document.body.appendChild(div.firstChild);
    modal = document.getElementById("aiDisclaimerModal");
  }

  var bsModal = bootstrap.Modal.getOrCreateInstance(modal);
  bsModal.show();

  // Remove previous listeners by cloning the button
  var agreeBtn = document.getElementById("aiDisclaimerAgree");
  var newAgreeBtn = agreeBtn.cloneNode(true);
  agreeBtn.parentNode.replaceChild(newAgreeBtn, agreeBtn);

  newAgreeBtn.addEventListener("click", function() {
    bsModal.hide();

    var btn = $("#aiActivateBtn");
    btn.prop("disabled", true).html('<i class="fa fa-spinner fa-spin me-1"></i>Activating...');

    jQuery.ajax({
      type: "POST",
      url: "api/v1/ai/activate",
      data: JSON.stringify({ subscriptionKey: key }),
      contentType: "application/json",
      beforeSend: function(xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
      timeout: 30000
    })
    .done(function() {
      btn.prop("disabled", false).html("Activate");
      $("#aiActivateSuccess").text("Subscription activated successfully!").show();
      setTimeout(function() { initAi(); }, 1000);
    })
    .fail(function(jqXHR) {
      btn.prop("disabled", false).html("Activate");
      var errorMsg = "Activation failed. Please check your key and try again.";
      try {
        if (jqXHR.responseText) {
          var errData = JSON.parse(jqXHR.responseText);
          if (errData.error) errorMsg = errData.error;
        }
      } catch (e) { /* ignore */ }
      $("#aiActivateError").text(errorMsg).show();
    });
  });
}

// ===== Chat History =====

function aiLoadChatList() {
  jQuery.ajax({
    type: "GET",
    url: "api/v1/ai/chats",
    beforeSend: function(xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    }
  })
  .done(function(data) {
    aiChatList = data.chats || [];
    aiRenderChatList();
  })
  .fail(function() {
    aiChatList = [];
    aiRenderChatList();
  });
}

function aiRenderChatList() {
  var container = $("#aiChatList");
  container.empty();

  if (aiChatList.length === 0) {
    container.append('<div style="color: var(--text-muted); font-size: 0.85rem; padding: 8px; text-align: center;">No conversations yet</div>');
    return;
  }

  // Group chats by date
  var groups = aiGroupChatsByDate(aiChatList);
  var groupLabels = ["Today", "Yesterday", "This Week", "Older"];

  for (var i = 0; i < groupLabels.length; i++) {
    var label = groupLabels[i];
    var chats = groups[label];
    if (!chats || chats.length === 0) continue;

    container.append('<div style="color: var(--text-muted); font-size: 0.75rem; font-weight: 600; padding: 8px 8px 4px 8px; text-transform: uppercase;">' + escapeHtml(label) + '</div>');

    for (var j = 0; j < chats.length; j++) {
      var chat = chats[j];
      var isActive = chat.id === aiCurrentChatId;
      var activeClass = isActive ? 'background: var(--bg-hover); font-weight: 500;' : '';
      var item = '<div class="ai-chat-item d-flex align-items-center" style="padding: 6px 8px; border-radius: 6px; cursor: pointer; margin-bottom: 2px; color: var(--text-primary); font-size: 0.85rem; ' + activeClass + '" ' +
        'onclick="aiLoadChat(\'' + escapeHtml(chat.id) + '\')" ' +
        'onmouseover="this.style.background=\'var(--bg-hover)\'" ' +
        'onmouseout="this.style.background=\'' + (isActive ? 'var(--bg-hover)' : '') + '\'">' +
        '<span class="text-truncate" style="flex: 1;">' + escapeHtml(chat.title || "Untitled") + '</span>' +
        '<i class="fa fa-trash-can ms-1" style="font-size: 0.7rem; color: var(--text-muted); opacity: 0; cursor: pointer;" ' +
        'onclick="event.stopPropagation(); aiDeleteChat(\'' + escapeHtml(chat.id) + '\')" ' +
        'onmouseover="this.parentElement.querySelector(\'.fa-trash-can\').style.opacity=1" ' +
        '></i>' +
        '</div>';
      container.append(item);
    }
  }

  // Show delete icon on hover of parent
  container.find('.ai-chat-item').on('mouseenter', function() {
    $(this).find('.fa-trash-can').css('opacity', '0.6');
  }).on('mouseleave', function() {
    $(this).find('.fa-trash-can').css('opacity', '0');
  });
}

function aiGroupChatsByDate(chats) {
  var groups = { "Today": [], "Yesterday": [], "This Week": [], "Older": [] };
  var now = new Date();
  var today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  var yesterday = new Date(today.getTime() - 86400000);
  var weekAgo = new Date(today.getTime() - 7 * 86400000);

  for (var i = 0; i < chats.length; i++) {
    var chat = chats[i];
    var chatDate = new Date(chat.updated || chat.created);
    if (chatDate >= today)
      groups["Today"].push(chat);
    else if (chatDate >= yesterday)
      groups["Yesterday"].push(chat);
    else if (chatDate >= weekAgo)
      groups["This Week"].push(chat);
    else
      groups["Older"].push(chat);
  }
  return groups;
}

// ===== Chat Operations =====

function aiNewChat() {
  aiCurrentChatId = null;
  aiMessages = [];
  aiRenderMessages();
  aiRenderChatList();
  $("#aiInput").val("").focus();
}

function aiLoadChat(chatId) {
  jQuery.ajax({
    type: "GET",
    url: "api/v1/ai/chats/" + encodeURIComponent(chatId),
    beforeSend: function(xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    }
  })
  .done(function(data) {
    aiCurrentChatId = data.id;
    aiMessages = data.messages || [];
    aiRenderMessages();
    aiRenderChatList();
    // Set database if chat has one
    if (data.database)
      selectDbInWidget(data.database, "aiDbSelectContainer");
  })
  .fail(function(jqXHR) {
    globalNotify("Error", "Failed to load chat", "danger");
  });
}

function aiDeleteChat(chatId) {
  globalConfirm("Delete Chat", "Are you sure you want to delete this conversation?", "warning", function() {
    jQuery.ajax({
      type: "DELETE",
      url: "api/v1/ai/chats/" + encodeURIComponent(chatId),
      beforeSend: function(xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      }
    })
    .done(function() {
      if (aiCurrentChatId === chatId)
        aiNewChat();
      aiLoadChatList();
    })
    .fail(function() {
      globalNotify("Error", "Failed to delete chat", "danger");
    });
  });
}

function aiDeleteMessage(msgIndex) {
  if (msgIndex < 0 || msgIndex >= aiMessages.length) return;

  // If deleting a user message, also delete the assistant response that follows it
  if (aiMessages[msgIndex].role === "user" && msgIndex + 1 < aiMessages.length && aiMessages[msgIndex + 1].role === "assistant")
    aiMessages.splice(msgIndex, 2);
  else
    aiMessages.splice(msgIndex, 1);

  aiRenderMessages();

  // Persist the updated chat
  if (aiCurrentChatId)
    aiSaveCurrentChat();
}

function aiSaveCurrentChat() {
  if (!aiCurrentChatId) return;
  jQuery.ajax({
    type: "PUT",
    url: "api/v1/ai/chats/" + encodeURIComponent(aiCurrentChatId),
    data: JSON.stringify({ messages: aiMessages }),
    contentType: "application/json",
    beforeSend: function(xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    }
  });
}

// ===== Sending Messages =====

function aiHandleInputKeydown(event) {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    aiSendMessage();
  }
}

function aiSendMessage() {
  var input = $("#aiInput");
  var message = input.val().trim();
  if (!message || aiSending) return;

  var db = aiGetCurrentDatabase();
  if (!db) {
    globalNotify("Warning", "Please select a database first", "warning");
    return;
  }

  // Add user message to display
  aiMessages.push({ role: "user", content: message, timestamp: new Date().toISOString() });
  aiRenderMessages();
  input.val("");

  // Show thinking indicator
  aiSetSending(true);

  if (aiMode === "auto") {
    aiSendMessageStreaming(db, message);
  } else {
    aiSendMessageLegacy(db, message);
  }
}

function aiSendMessageStreaming(db, message) {
  var controller = new AbortController();
  aiCurrentXhr = { abort: function() { controller.abort(); } };

  // Show live tool call area below thinking indicator
  var liveId = "aiLiveTools_" + Date.now();
  var thinkingEl = document.getElementById("aiThinking");
  if (thinkingEl) {
    var liveDiv = document.createElement("div");
    liveDiv.id = liveId;
    liveDiv.style.cssText = "font-size: 0.8rem; color: var(--text-muted); margin-left: 40px; margin-bottom: 8px;";
    thinkingEl.parentNode.insertBefore(liveDiv, thinkingEl.nextSibling);
  }

  var toolCalls = [];

  fetch("api/v1/ai/chat/stream", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": globalCredentials
    },
    body: JSON.stringify({ database: db, message: message, chatId: aiCurrentChatId, protocolVersion: AI_PROTOCOL_VERSION }),
    signal: controller.signal
  })
  .then(function(response) {
    if (response.status === 401) {
      if (typeof handleSessionExpired === "function") handleSessionExpired();
      var err = new Error("Session expired");
      err.status = 401;
      throw err;
    }
    if (!response.ok) {
      return response.text().then(function(text) {
        var err = new Error("HTTP " + response.status);
        err.responseText = text;
        err.status = response.status;
        throw err;
      });
    }

    var contentType = response.headers.get("content-type") || "";
    if (contentType.indexOf("text/event-stream") === -1) {
      // Non-streaming response (fallback)
      return response.json().then(function(data) {
        aiHandleResponse(data);
      });
    }

    // SSE streaming
    var reader = response.body.getReader();
    var decoder = new TextDecoder();
    var buffer = "";
    var gotDone = false;

    function read() {
      return reader.read().then(function(result) {
        if (result.done) {
          // Stream ended — if we never got a done event, clean up
          if (!gotDone) {
            aiCurrentXhr = null;
            aiSetSending(false);
            if (toolCalls.length === 0)
              globalNotify("Error", "Connection to AI service was interrupted", "danger");
          }
          return;
        }

        buffer += decoder.decode(result.value, { stream: true });
        var lines = buffer.split("\n");
        buffer = lines.pop(); // Keep incomplete line in buffer

        for (var i = 0; i < lines.length; i++) {
          var line = lines[i];
          if (line.indexOf("data: ") !== 0) continue;
          try {
            var event = JSON.parse(line.substring(6));
            if (event.type === "tool_start") {
              toolCalls.push(event);
              aiUpdateLiveTools(liveId, toolCalls);
            } else if (event.type === "tool_end") {
              // Update last matching tool call with error status
              for (var j = toolCalls.length - 1; j >= 0; j--) {
                if (toolCalls[j].tool === event.tool && !toolCalls[j]._done) {
                  toolCalls[j].error = event.error;
                  toolCalls[j]._done = true;
                  break;
                }
              }
              aiUpdateLiveTools(liveId, toolCalls);
            } else if (event.type === "delta") {
              aiAppendLiveText(liveId, event.text);
            } else if (event.type === "reset") {
              aiResetLiveText(liveId);
            } else if (event.type === "done") {
              gotDone = true;
              if (event.usage && aiPortalInfo && typeof event.usage.budget === "number")
                aiShowUsage({ spent: event.usage.spent, budget: event.usage.budget, percent: event.usage.percent,
                  resetsOn: event.usage.resetsOn || aiPortalInfo.resetsOn });
              // Inject accumulated tool calls into the done data
              event.toolCalls = toolCalls.length > 0 ? toolCalls : undefined;
              aiHandleResponse(event);
            } else if (event.type === "error") {
              // The server cut the stream short after it started (issue #8642) and says why: no 'done' follows.
              gotDone = true;
              aiCurrentXhr = null;
              aiSetSending(false);
              if (!aiHandleFailure(event.code, event.error, event.upgrade === true))
                globalNotify("Error", event.error || "Connection to AI service was interrupted", "danger");
            }
          } catch (e) { /* ignore malformed events */ }
        }

        return read();
      });
    }

    return read();
  })
  .catch(function(err) {
    aiCurrentXhr = null;
    aiSetSending(false);

    if (err.name === "AbortError") return;

    var errorMsg = "Failed to get a response from the AI assistant.";
    var errorCode = "";
    try {
      if (err.responseText) {
        var errData = JSON.parse(err.responseText);
        if (errData.detail) errorMsg = errData.detail;
        else if (errData.error) errorMsg = errData.error;
        if (errData.code) errorCode = errData.code;
      }
    } catch (e) { /* ignore */ }

    if (aiIsTokenError(errorCode)) {
      aiConfigured = false;
      $("#aiActivePanel").hide();
      $("#aiInactivePanel").show();
      globalNotify("Subscription", errorMsg, "warning");
    } else if (!aiHandleFailure(errorCode, errorMsg, false))
      globalNotify("Error", errorMsg, "danger");
  });
}

function aiIsTokenError(code) {
  return code === "token_invalid" || code === "token_expired" || code === "token_disabled";
}

function aiSendMessageLegacy(db, message) {
  aiCurrentXhr = jQuery.ajax({
    type: "POST",
    url: "api/v1/ai/chat",
    data: JSON.stringify({ database: db, message: message, chatId: aiCurrentChatId, protocolVersion: AI_PROTOCOL_VERSION }),
    contentType: "application/json",
    beforeSend: function(xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    },
    timeout: 120000
  })
  .done(function(data) {
    aiHandleResponse(data);
  })
  .fail(function(jqXHR, textStatus) {
    aiCurrentXhr = null;
    aiSetSending(false);

    // User cancelled the request
    if (textStatus === "abort")
      return;

    var errorMsg = "Failed to get a response from the AI assistant.";
    var errorCode = "";
    try {
      var errData = JSON.parse(jqXHR.responseText);
      if (errData.detail) errorMsg = errData.detail;
      else if (errData.error) errorMsg = errData.error;
      if (errData.code) errorCode = errData.code;
    } catch (e) { /* ignore parse errors */ }

    // If token is invalid or expired, reset to inactive state
    if (aiIsTokenError(errorCode)) {
      aiConfigured = false;
      $("#aiActivePanel").hide();
      $("#aiInactivePanel").show();
      globalNotify("Subscription", errorMsg, "warning");
    } else if (!aiHandleFailure(errorCode, errorMsg, false))
      globalNotify("Error", errorMsg, "danger");
  });
}

function aiHandleResponse(data) {
  aiCurrentXhr = null;
  aiSetSending(false);

  // Update chat ID if new chat was created
  if (data.chatId)
    aiCurrentChatId = data.chatId;

  // Add assistant message
  var assistantMsg = { role: "assistant", content: data.response, timestamp: new Date().toISOString() };
  if (data.commands && data.commands.length > 0)
    assistantMsg.commands = data.commands;
  var charts = aiChartsClean(data.charts);
  if (charts.length > 0)
    assistantMsg.charts = charts;
  if (data.toolCalls && data.toolCalls.length > 0)
    assistantMsg.toolCalls = data.toolCalls;
  aiMessages.push(assistantMsg);
  aiRenderMessages();

  // Refresh chat list
  aiLoadChatList();
}

/** The reply as it is written (plain text: the formatted answer replaces it when 'done' arrives). */
function aiAppendLiveText(liveId, text) {
  var el = document.getElementById(liveId + "_text");
  if (!el) {
    var host = document.getElementById(liveId);
    if (!host) return;
    el = document.createElement("div");
    el.id = liveId + "_text";
    el.style.cssText = "white-space: pre-wrap; margin: 0 0 8px 40px; font-size: 0.9rem; color: var(--text-secondary);";
    // A sibling, not a child: the tool list above is redrawn by replacing the content of its own element
    host.parentNode.insertBefore(el, host.nextSibling);
  }
  el.textContent += text || "";
}

function aiResetLiveText(liveId) {
  var el = document.getElementById(liveId + "_text");
  if (el) el.textContent = "";
}

function aiUpdateLiveTools(liveId, toolCalls) {
  var el = document.getElementById(liveId);
  if (!el) return;

  var html = "";
  for (var i = 0; i < toolCalls.length; i++) {
    var tc = toolCalls[i];
    var icon, label;
    if (tc.tool === "query_database") {
      icon = "fa-database";
      label = '<code style="background: var(--bg-reference); padding: 1px 4px; border-radius: 3px; font-size: 0.8em; color: var(--text-muted);">' +
        escapeHtml(tc.args.command) + '</code>';
    } else if (tc.tool === "get_type") {
      icon = "fa-sitemap";
      label = "Looked at type " + escapeHtml(tc.args && tc.args.name ? tc.args.name : "");
    } else if (tc.tool === "get_schema") {
      icon = "fa-sitemap";
      label = "Fetching schema";
    } else if (tc.tool === "get_server_info") {
      icon = "fa-server";
      label = "Fetching server info";
    } else {
      icon = "fa-wrench";
      label = escapeHtml(tc.tool);
    }
    var statusIcon = tc._done
      ? (tc.error
        ? '<i class="fa fa-circle-exclamation ms-1" style="color: #dc3545; font-size: 0.7rem;" title="' + escapeHtml(tc.error) + '"></i>'
        : '<i class="fa fa-check ms-1" style="color: #28a745; font-size: 0.7rem;"></i>')
      : '<i class="fa fa-spinner fa-spin ms-1" style="font-size: 0.7rem;"></i>';
    html += '<div style="padding: 2px 0;"><i class="fa ' + icon + ' me-1" style="width: 14px; text-align: center;"></i>' + label + statusIcon + '</div>';
    if (tc._done && tc.error) {
      console.warn("AI tool '" + tc.tool + "' failed:", tc.error, tc);
      html += '<div style="padding: 2px 0 6px 20px; color: #dc3545; font-size: 0.75rem; white-space: pre-wrap; word-break: break-word;">' +
        escapeHtml(tc.error) + '</div>';
    }
  }
  el.innerHTML = html;

  aiScrollToBottom();
}

function aiStopResponse() {
  if (aiCurrentXhr) {
    aiCurrentXhr.abort();
    aiCurrentXhr = null;
  }
  aiSetSending(false);
}

function aiSetSending(sending) {
  aiSending = sending;
  var btn = $("#aiSendBtn");
  if (sending) {
    btn.attr("onclick", "aiStopResponse()")
      .css("background", "#dc3545")
      .html('<i class="fa fa-stop me-1"></i>Stop');
    // Add thinking indicator to messages
    var thinkingHtml = '<div id="aiThinking" class="d-flex mb-3">' +
      '<div style="width: 32px; height: 32px; border-radius: 50%; background: var(--color-brand); display: flex; align-items: center; justify-content: center; flex-shrink: 0;">' +
      '<i class="fa fa-robot" style="color: white; font-size: 0.85rem;"></i></div>' +
      '<div class="ms-2 px-3 py-2" style="background: var(--bg-card); border: 1px solid var(--border-main); border-radius: 12px; color: var(--text-muted);">' +
      '<i class="fa fa-spinner fa-spin me-1"></i> Thinking...</div></div>';
    $("#aiMessages").append(thinkingHtml);
    aiScrollToBottom();
  } else {
    btn.attr("onclick", "aiSendMessage()")
      .css("background", "var(--color-brand)")
      .html("Send");
    $("#aiThinking").remove();
  }
}

// ===== Rendering Messages =====

function aiRenderMessages() {
  var container = $("#aiMessages");
  aiDestroyCharts();
  container.empty();
  aiCommandBlockCounter = 0;

  if (aiMessages.length === 0) {
    container.append($("#aiWelcome").length ? '' : '');
    // Show welcome message
    container.append(
      '<div id="aiWelcome" class="text-center" style="margin-top: 80px;">' +
      '<i class="fa fa-robot" style="font-size: 2.5rem; color: var(--color-brand); opacity: 0.6;"></i>' +
      '<h5 style="color: var(--text-primary); margin-top: 12px;">How can I help you?</h5>' +
      '<p style="color: var(--text-muted); font-size: 0.9rem;">Ask me about your database schema, query optimization, data modeling, or synthetic data generation.</p></div>'
    );
    return;
  }

  for (var i = 0; i < aiMessages.length; i++) {
    var msg = aiMessages[i];
    if (msg.role === "user")
      container.append(aiRenderUserMessage(msg, i));
    else if (msg.role === "assistant")
      container.append(aiRenderAssistantMessage(msg, i));
  }

  aiDrawCharts();
  aiScrollToBottom();
}

function aiRenderUserMessage(msg, msgIndex) {
  return '<div class="mb-3">' +
    '<div class="d-flex justify-content-end">' +
    '<div class="px-3 py-2" style="background: var(--color-brand); color: white; border-radius: 12px; max-width: 70%; white-space: pre-wrap; word-break: break-word;">' +
    escapeHtml(msg.content) + '</div>' +
    '<div class="ms-2" style="width: 32px; height: 32px; border-radius: 50%; background: var(--bg-sidebar); display: flex; align-items: center; justify-content: center; flex-shrink: 0;">' +
    '<i class="fa fa-user" style="color: var(--text-muted); font-size: 0.85rem;"></i></div></div>' +
    '<div class="d-flex justify-content-end me-5 mt-1">' +
    '<button class="btn btn-link btn-sm p-0" style="color: var(--text-muted); font-size: 0.7rem;" ' +
    'onclick="aiDeleteMessage(' + msgIndex + ')" title="Delete message"><i class="fa fa-trash-can"></i></button></div></div>';
}

function aiRenderAssistantMessage(msg, msgIndex) {
  var contentHtml = aiRenderMarkdown(msg.content);
  var msgBlockStart = aiCommandBlockCounter;

  var html = '<div class="mb-3">' +
    '<div class="d-flex align-items-start">' +
    '<div style="width: 32px; height: 32px; border-radius: 50%; background: var(--color-brand); display: flex; align-items: center; justify-content: center; flex-shrink: 0;">' +
    '<i class="fa fa-robot" style="color: white; font-size: 0.85rem;"></i></div>' +
    '<div class="ms-2" style="max-width: 80%; min-width: 0;">';

  // Render tool calls log (Auto Run mode transparency)
  if (msg.toolCalls && msg.toolCalls.length > 0)
    html += aiRenderToolCallLog(msg.toolCalls);

  html += '<div class="ai-message-content" style="color: var(--text-primary); line-height: 1.6;">' + contentHtml + '</div>';

  // Charts the assistant asked for: Studio runs their queries (read-only) and draws them once the page is built
  if (msg.charts && msg.charts.length > 0)
    for (var c = 0; c < msg.charts.length; c++)
      html += aiRenderChartShell(msg.charts[c]);

  // Render command blocks if present
  if (msg.commands && msg.commands.length > 0) {
    for (var j = 0; j < msg.commands.length; j++)
      html += aiRenderCommandBlock(msg.commands[j], j, msgIndex);

    // "Execute All" button when multiple commands
    if (msg.commands.length > 1)
      html += '<div class="mt-2"><button class="btn btn-sm" style="background: var(--color-brand); color: white; border: none;" ' +
        'onclick="aiExecuteAll(this, ' + msgBlockStart + ', ' + aiCommandBlockCounter + ')">' +
        '<i class="fa fa-forward me-1"></i>Execute All</button></div>';
  } else {
    // No command blocks: show standalone delete button
    html += '<div class="mt-1 ms-1"><button class="btn btn-link btn-sm p-0" style="color: var(--text-muted); font-size: 0.7rem;" ' +
      'onclick="aiDeleteMessage(' + msgIndex + ')" title="Delete message"><i class="fa fa-trash-can"></i></button></div>';
  }

  html += '</div></div></div>';
  return html;
}

function aiRenderToolCallLog(toolCalls) {
  var html = '<div style="margin-bottom: 8px; font-size: 0.8rem; color: var(--text-muted);">';
  for (var i = 0; i < toolCalls.length; i++) {
    var tc = toolCalls[i];
    var icon, label;
    if (tc.tool === "query_database") {
      icon = "fa-database";
      label = '<code style="background: var(--bg-reference); padding: 1px 4px; border-radius: 3px; font-size: 0.8em; color: var(--text-muted);">' +
        escapeHtml(tc.args.command) + '</code>';
    } else if (tc.tool === "get_schema") {
      icon = "fa-sitemap";
      label = "Fetched schema";
    } else if (tc.tool === "get_server_info") {
      icon = "fa-server";
      label = "Fetched server info";
    } else {
      icon = "fa-wrench";
      label = escapeHtml(tc.tool);
    }
    var statusIcon = tc.error
      ? '<i class="fa fa-circle-exclamation ms-1" style="color: #dc3545; font-size: 0.7rem;" title="' + escapeHtml(tc.error) + '"></i>'
      : '<i class="fa fa-check ms-1" style="color: #28a745; font-size: 0.7rem;"></i>';
    html += '<div style="padding: 2px 0;"><i class="fa ' + icon + ' me-1" style="width: 14px; text-align: center;"></i>' + label + statusIcon + '</div>';
    if (tc.error)
      html += '<div style="padding: 2px 0 6px 20px; color: #dc3545; font-size: 0.75rem; white-space: pre-wrap; word-break: break-word;">' +
        escapeHtml(tc.error) + '</div>';
  }
  html += '</div>';
  return html;
}

function aiRenderCommandBlock(cmd, index, msgIndex) {
  var blockId = "aiCmd_" + (aiCommandBlockCounter++);
  var lang = escapeHtml((cmd.language || "sql").toUpperCase());
  var purpose = cmd.purpose ? '<div style="font-size: 0.85rem; color: var(--text-muted); margin-bottom: 4px;">' + escapeHtml(cmd.purpose) + '</div>' : '';

  return '<div class="ai-command-block" style="margin-top: 8px; border: 1px solid var(--border-main); border-radius: 8px; overflow: hidden; background: var(--bg-card);">' +
    '<div style="padding: 8px 12px; background: var(--bg-sidebar); border-bottom: 1px solid var(--border-main);">' +
    purpose +
    '<span class="badge" style="background: var(--color-brand); color: white; font-size: 0.7rem;">' + lang + '</span></div>' +
    '<pre id="' + blockId + '" style="margin: 0; padding: 12px; background: var(--bg-code); color: var(--text-code); font-size: 0.85rem; overflow-x: auto; white-space: pre-wrap; word-break: break-word;" ' +
    'data-language="' + escapeHtml(cmd.language || "sql") + '" data-command="' + escapeHtml(cmd.command) + '">' +
    escapeHtml(cmd.command) + '</pre>' +
    '<div id="' + blockId + '_result" style="display: none; padding: 8px 12px; border-top: 1px solid var(--border-main); font-size: 0.85rem;"></div>' +
    '<div style="padding: 6px 8px; border-top: 1px solid var(--border-main); display: flex; align-items: center; justify-content: space-between;">' +
    '<div style="display: flex; align-items: center; gap: 8px;">' +
    '<button class="btn btn-link btn-sm p-0" style="color: var(--text-muted); font-size: 0.75rem;" onclick="aiCopyCode(this, \'' + blockId + '\')" title="Copy to clipboard">' +
    '<i class="fa fa-copy"></i></button>' +
    '<button class="btn btn-link btn-sm p-0" style="color: var(--text-muted); font-size: 0.75rem;" onclick="aiDeleteMessage(' + msgIndex + ')" title="Delete message">' +
    '<i class="fa fa-trash-can"></i></button></div>' +
    '<div style="display: flex; align-items: center; gap: 8px;">' +
    '<button class="btn btn-sm" style="background: transparent; color: var(--text-muted); border: 1px solid var(--border-main); font-size: 0.8rem;" onclick="aiOpenInQuery(\'' + blockId + '\')">' +
    '<i class="fa fa-terminal me-1"></i>Open in Query</button>' +
    '<button id="' + blockId + '_toggle" class="btn btn-sm" style="display: none; background: transparent; color: var(--text-muted); border: 1px solid var(--border-main); font-size: 0.8rem;" onclick="aiToggleResults(\'' + blockId + '\')"></button>' +
    '<button class="btn btn-sm" style="background: var(--color-brand); color: white; border: none; font-size: 0.8rem;" onclick="aiExecuteCommand(this, \'' + blockId + '\')">' +
    '<i class="fa fa-play me-1"></i>Execute</button></div></div>' +
    '</div>';
}

// ===== Copy Code =====

function aiCopyCode(button, blockId) {
  var pre = document.getElementById(blockId);
  if (!pre) return;

  var text = pre.getAttribute("data-command") || pre.textContent;
  navigator.clipboard.writeText(text).then(function() {
    var icon = button.querySelector("i");
    if (icon) {
      icon.className = "fa fa-check";
      setTimeout(function() { icon.className = "fa fa-copy"; }, 1500);
    }
  });
}

// ===== Open in Query Panel =====

function aiOpenInQuery(blockId) {
  var pre = document.getElementById(blockId);
  if (!pre) return;

  aiOpenQueryPanel(pre.getAttribute("data-language") || "sql", pre.getAttribute("data-command"));
}

/** Shows a command in the Query panel's editor (does not run it). */
function aiOpenQueryPanel(language, command) {
  // Switch to Query tab
  var queryTab = document.getElementById("tab-query-sel");
  if (queryTab) queryTab.click();

  // Set language and command in Query panel
  setTimeout(function() {
    if (typeof editor !== "undefined" && editor) {
      setEditorLanguage(language);
      editor.setValue(command);
    } else
      $("#inputLanguage").val(language);
  }, 100);
}

// ===== Command Execution =====

function aiExecuteCommand(button, blockId) {
  var pre = document.getElementById(blockId);
  if (!pre) return;

  var command = pre.getAttribute("data-command");
  var language = pre.getAttribute("data-language") || "sql";

  // Auto-switch to sqlscript for multi-statement SQL blocks
  if (language === "sql" && aiIsMultiStatementSql(command))
    language = "sqlscript";

  var db = aiGetCurrentDatabase();

  if (!db) {
    globalNotify("Warning", "Please select a database first", "warning");
    return;
  }

  var btn = $(button);
  btn.prop("disabled", true).html('<i class="fa fa-spinner fa-spin me-1"></i>Running...');

  var resultDiv = $("#" + blockId + "_result");

  jQuery.ajax({
    type: "POST",
    url: "api/v1/command/" + encodeDatabaseName(db),
    data: JSON.stringify({ language: language, command: command }),
    contentType: "application/json",
    beforeSend: function(xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    }
  })
  .done(function(data) {
    btn.prop("disabled", false).html('<i class="fa fa-play me-1"></i>Execute');
    // A result with rows stays (collapsible); a bare "Success" (a write) fades after 8 seconds
    if (!aiShowCommandResult(blockId, data))
      setTimeout(function() { resultDiv.fadeOut(300); }, 8000);
  })
  .fail(function(jqXHR) {
    btn.prop("disabled", false).html('<i class="fa fa-play me-1"></i>Execute');
    var errorMsg = "Command failed";
    try {
      var errData = JSON.parse(jqXHR.responseText);
      if (errData.detail) errorMsg = errData.detail;
      else if (errData.error) errorMsg = errData.error;
    } catch (e) { /* ignore */ }
    resultDiv.show().html('<i class="fa fa-circle-exclamation me-1" style="color: #dc3545;"></i> <span style="color: #dc3545;">' + escapeHtml(errorMsg) + '</span>');
  });
}

// ===== Command results (the Execute buttons) =====

/** A result of up to this many rows starts expanded under its command; a bigger one starts collapsed behind the button. */
var AI_RESULT_EXPANDED_ROWS = 10;

/**
 * Shows what a command returned under its card: the status line and, when it returned records, a compact table in a panel
 * the toggle button opens and closes. @return true when a table was added (such a result stays; a bare "Success" fades).
 */
function aiShowCommandResult(blockId, data) {
  var records = data && data.result;
  var count = Array.isArray(records) ? records.length : 0;
  var table = aiResultTable(records);
  var html = '<i class="fa fa-check-circle me-1" style="color: #28a745;"></i> <span style="color: var(--text-primary);">Success' +
    (count > 0 ? ' (' + count + ' results)' : '') + '</span>';
  if (table) {
    html += '<div id="' + blockId + '_table" style="display: ' + (table.total <= AI_RESULT_EXPANDED_ROWS ? "block" : "none") + ';">' +
      aiResultTableHtml(table, escapeHtml);
    if (table.truncated)
      html += '<div class="mt-1" style="font-size: 0.78rem; color: var(--text-muted);">' + (table.total - table.rows.length) +
        ' more rows: <a href="#" onclick="aiOpenInQuery(\'' + blockId + '\'); return false;">open in Query</a></div>';
    html += '</div>';
  }
  $("#" + blockId + "_result").show().html(html);
  var toggle = $("#" + blockId + "_toggle");
  if (table) {
    toggle.data("count", table.total).show();
    aiSyncToggle(blockId);
  } else
    toggle.hide();
  return !!table;
}

function aiToggleResults(blockId) {
  $("#" + blockId + "_table").toggle();
  aiSyncToggle(blockId);
}

function aiSyncToggle(blockId) {
  var open = $("#" + blockId + "_table").css("display") !== "none";
  $("#" + blockId + "_toggle").html('<i class="fa fa-chevron-' + (open ? "up" : "down") + ' me-1"></i>' +
    (open ? "Hide results" : "Show results (" + $("#" + blockId + "_toggle").data("count") + ")"));
}

// ===== Charts the assistant asks for =====
// The model sends {type, title, language, query, x, y[]}; Studio runs the query itself through api/v1/query, which the engine
// refuses when it would write, and draws the rows with ApexCharts (pure parts in studio-ai-chart.js). The rows never go
// back to the model. Everything that came from the model or the database is escaped or handed over as text.

var aiChartSpecs = {};
var aiChartInstances = {};
var aiChartCounter = 0;

function aiDestroyCharts() {
  for (var id in aiChartInstances) {
    try {
      aiChartInstances[id].destroy();
    } catch (e) { /* the element is already gone */ }
  }
  aiChartInstances = {};
  aiChartSpecs = {};
}

function aiRenderChartShell(spec) {
  var id = "aiChart_" + (aiChartCounter++);
  aiChartSpecs[id] = spec;
  return '<div class="ai-chart" id="' + id + '" data-chart-id="' + id + '" style="margin-top: 10px; border: 1px solid var(--border-main); border-radius: 8px; overflow: hidden; background: var(--bg-card);">' +
    '<div style="padding: 8px 12px; background: var(--bg-sidebar); border-bottom: 1px solid var(--border-main); display: flex; align-items: center; justify-content: space-between; gap: 8px;">' +
    '<span style="font-weight: 600; font-size: 0.9rem;"><i class="fa fa-chart-simple me-1"></i>' + escapeHtml(spec.title || "Chart") + '</span>' +
    '<button class="btn btn-sm" style="background: transparent; color: var(--text-muted); border: 1px solid var(--border-main); font-size: 0.78rem;" onclick="aiChartOpenInQuery(\'' + id + '\')">' +
    '<i class="fa fa-terminal me-1"></i>Open in Query</button></div>' +
    '<div id="' + id + '_body" style="padding: 8px 12px;"><div class="text-muted" style="font-size: 0.85rem;"><i class="fa fa-spinner fa-spin me-1"></i>Running the query...</div></div>' +
    '</div>';
}

function aiChartOpenInQuery(id) {
  var spec = aiChartSpecs[id];
  if (spec) aiOpenQueryPanel(spec.language, spec.query);
}

function aiChartNote(id, icon, color, text) {
  $("#" + id + "_body").html('<div style="font-size: 0.85rem; color: ' + color + ';"><i class="fa ' + icon + ' me-1"></i>' + escapeHtml(text) + '</div>');
}

/** Runs and draws every chart on the page that has not been drawn yet. */
function aiDrawCharts() {
  $("#aiMessages .ai-chart").each(function () {
    var id = $(this).attr("data-chart-id");
    if (aiChartSpecs[id] && !$(this).attr("data-drawn")) {
      $(this).attr("data-drawn", "1");
      aiDrawChart(id);
    }
  });
}

function aiDrawChart(id) {
  var spec = aiChartSpecs[id];
  var db = aiGetCurrentDatabase();
  if (!db) {
    aiChartNote(id, "fa-circle-info", "var(--text-muted)", "Select a database to draw this chart.");
    return;
  }
  if (typeof ApexCharts === "undefined") {
    aiChartNote(id, "fa-circle-exclamation", "#dc3545", "The charting library is not loaded.");
    return;
  }
  // api/v1/query is the read-only door: a query that would write is refused by the engine, never executed
  jQuery.ajax({
    type: "POST",
    url: "api/v1/query/" + encodeDatabaseName(db),
    data: JSON.stringify({ language: spec.language, command: spec.query, limit: 200 }),
    contentType: "application/json",
    beforeSend: function (xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    }
  })
  .done(function (data) {
    if (aiChartSpecs[id] !== spec || !document.getElementById(id + "_body")) return; // redrawn or deleted meanwhile
    var model = aiChartModel(data && data.result, spec);
    if (model.categories.length === 0) {
      aiChartNote(id, "fa-circle-info", "var(--text-muted)", "The query returned no rows with a number in " + spec.y.join(", ") + ", so there is nothing to chart.");
      return;
    }
    var body = $("#" + id + "_body");
    body.empty().append('<div id="' + id + '_plot"></div>');
    var dark = document.documentElement.getAttribute("data-theme") === "dark";
    var chart = new ApexCharts(document.getElementById(id + "_plot"), aiChartOptions(spec, model, dark));
    aiChartInstances[id] = chart;
    chart.render();
    if (model.dropped > 0)
      body.append('<div style="font-size: 0.75rem; color: var(--text-muted);">' + model.dropped + ' row(s) without a usable number were left out.</div>');
  })
  .fail(function (jqXHR) {
    if (aiChartSpecs[id] !== spec) return;
    var message = "The query failed";
    try {
      var err = JSON.parse(jqXHR.responseText);
      if (err.detail) message = err.detail;
      else if (err.error) message = err.error;
    } catch (e) { /* keep the generic message */ }
    aiChartNote(id, "fa-circle-exclamation", "#dc3545", message);
  });
}

// ===== Execute All =====

function aiExecuteAll(button, startId, endId) {
  var commands = [];
  for (var i = startId; i < endId; i++) {
    var blockId = "aiCmd_" + i;
    var pre = document.getElementById(blockId);
    if (pre) {
      var execBtn = $(pre).closest('.ai-command-block').find("button[onclick*='aiExecuteCommand']");
      commands.push({ blockId: blockId, pre: pre, btn: execBtn });
    }
  }
  if (commands.length === 0) return;

  var allBtn = $(button);
  allBtn.prop("disabled", true).html('<i class="fa fa-spinner fa-spin me-1"></i>Running all...');
  aiRunSequential(commands, 0, allBtn);
}

function aiRunSequential(commands, index, allBtn) {
  if (index >= commands.length) {
    allBtn.prop("disabled", false).html('<i class="fa fa-forward me-1"></i>Execute All');
    return;
  }

  var item = commands[index];
  var command = item.pre.getAttribute("data-command");
  var language = item.pre.getAttribute("data-language") || "sql";

  if (language === "sql" && aiIsMultiStatementSql(command))
    language = "sqlscript";

  var db = aiGetCurrentDatabase();
  if (!db) {
    globalNotify("Warning", "Please select a database first", "warning");
    allBtn.prop("disabled", false).html('<i class="fa fa-forward me-1"></i>Execute All');
    return;
  }

  item.btn.prop("disabled", true).html('<i class="fa fa-spinner fa-spin me-1"></i>Running...');
  var resultDiv = $("#" + item.blockId + "_result");

  jQuery.ajax({
    type: "POST",
    url: "api/v1/command/" + encodeDatabaseName(db),
    data: JSON.stringify({ language: language, command: command }),
    contentType: "application/json",
    beforeSend: function(xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    }
  })
  .done(function(data) {
    item.btn.prop("disabled", false).html('<i class="fa fa-play me-1"></i>Execute');
    aiShowCommandResult(item.blockId, data);
    aiRunSequential(commands, index + 1, allBtn);
  })
  .fail(function(jqXHR) {
    item.btn.prop("disabled", false).html('<i class="fa fa-play me-1"></i>Execute');
    var errorMsg = "Command failed";
    try {
      var errData = JSON.parse(jqXHR.responseText);
      if (errData.detail) errorMsg = errData.detail;
      else if (errData.error) errorMsg = errData.error;
    } catch (e) { /* ignore */ }
    resultDiv.show().html('<i class="fa fa-circle-exclamation me-1" style="color: #dc3545;"></i> <span style="color: #dc3545;">' + escapeHtml(errorMsg) + '</span>');
    // Stop on error
    allBtn.prop("disabled", false).html('<i class="fa fa-forward me-1"></i>Execute All');
  });
}

// ===== Markdown Rendering =====

var aiMarkdownBlockId = 0;

function aiRenderMarkdown(text) {
  if (!text) return "";

  // Use marked.js if available, otherwise basic rendering
  if (typeof marked !== "undefined") {
    try {
      var renderer = new marked.Renderer();
      renderer.code = function(obj) {
        var code = (typeof obj === "object") ? obj.text : obj;
        var lang = (typeof obj === "object") ? (obj.lang || "") : "";
        var id = "aiMdCode_" + (aiMarkdownBlockId++);
        var langBadge = lang ? '<span class="badge" style="background: var(--color-brand); color: white; font-size: 0.65rem;">' + escapeHtml(lang.toUpperCase()) + '</span>' : '';
        return '<div style="position: relative; margin: 8px 0; border: 1px solid var(--border-main); border-radius: 6px; overflow: hidden;">' +
          (langBadge ? '<div style="padding: 4px 8px; background: var(--bg-sidebar); border-bottom: 1px solid var(--border-main);">' + langBadge + '</div>' : '') +
          '<pre id="' + id + '" style="margin: 0; padding: 12px; background: var(--bg-code); color: var(--text-code); font-size: 0.85rem; overflow-x: auto; white-space: pre-wrap; word-break: break-word;">' +
          escapeHtml(code) + '</pre>' +
          '<div style="padding: 4px 8px; border-top: 1px solid var(--border-main); background: var(--bg-sidebar);">' +
          '<button class="btn btn-link btn-sm p-0" style="color: var(--text-muted); font-size: 0.75rem;" onclick="aiCopyCode(this, \'' + id + '\')" title="Copy to clipboard">' +
          '<i class="fa fa-copy"></i></button></div></div>';
      };
      return marked.parse(text, { renderer: renderer });
    } catch (e) {
      // Fallback to basic rendering
    }
  }
  return aiBasicMarkdown(text);
}

function aiBasicMarkdown(text) {
  // Basic markdown rendering without external library
  var html = escapeHtml(text);

  // Code blocks with language: ```lang\ncode\n```
  html = html.replace(/```(\w*)\n([\s\S]*?)```/g, function(match, lang, code) {
    var id = "aiMdCode_" + (aiMarkdownBlockId++);
    var langBadge = lang ? '<span class="badge" style="background: var(--color-brand); color: white; font-size: 0.65rem;">' + lang.toUpperCase() + '</span>' : '';
    return '<div style="position: relative; margin: 8px 0; border: 1px solid var(--border-main); border-radius: 6px; overflow: hidden;">' +
      (langBadge ? '<div style="padding: 4px 8px; background: var(--bg-sidebar); border-bottom: 1px solid var(--border-main);">' + langBadge + '</div>' : '') +
      '<pre id="' + id + '" style="margin: 0; padding: 12px; background: var(--bg-code); color: var(--text-code); font-size: 0.85rem; overflow-x: auto; white-space: pre-wrap; word-break: break-word;">' +
      code.trim() + '</pre>' +
      '<div style="padding: 4px 8px; border-top: 1px solid var(--border-main); background: var(--bg-sidebar);">' +
      '<button class="btn btn-link btn-sm p-0" style="color: var(--text-muted); font-size: 0.75rem;" onclick="aiCopyCode(this, \'' + id + '\')" title="Copy to clipboard">' +
      '<i class="fa fa-copy"></i></button></div></div>';
  });

  // Inline code
  html = html.replace(/`([^`]+)`/g, '<code style="background: var(--bg-reference); padding: 2px 4px; border-radius: 3px; font-size: 0.9em;">$1</code>');

  // Bold
  html = html.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');

  // Italic
  html = html.replace(/\*([^*]+)\*/g, '<em>$1</em>');

  // Line breaks
  html = html.replace(/\n/g, '<br>');

  return html;
}

// ===== SQL Helpers =====

function aiIsMultiStatementSql(command) {
  // Strip comments, strings, and whitespace to count real semicolons
  var stripped = command.replace(/'[^']*'/g, "").replace(/--[^\n]*/g, "").replace(/\/\*[\s\S]*?\*\//g, "").trim();
  // Remove trailing semicolon, then check if there are still semicolons left
  stripped = stripped.replace(/;\s*$/, "");
  return stripped.indexOf(";") >= 0;
}

// ===== Utilities =====

function aiScrollToBottom() {
  var container = document.getElementById("aiMessages");
  if (container)
    container.scrollTop = container.scrollHeight;
}

function aiGetCurrentDatabase() {
  return getCurrentDatabase();
}
