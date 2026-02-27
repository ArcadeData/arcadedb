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
    if (aiConfigured) {
      $("#aiInactivePanel").hide();
      $("#aiActivePanel").show();
      initSearchableDbSelect("aiDbSelectContainer");
      aiLoadChatList();
    } else {
      $("#aiInactivePanel").show();
      $("#aiActivePanel").hide();
    }
  })
  .fail(function() {
    $("#aiInactivePanel").show();
    $("#aiActivePanel").hide();
  });
}

// ===== Activation =====

function aiActivate() {
  var key = $("#aiSubscriptionKey").val().trim();
  if (!key) {
    $("#aiActivateError").text("Please enter a subscription key.").show();
    return;
  }

  var btn = $("#aiActivateBtn");
  btn.prop("disabled", true).html('<i class="fa fa-spinner fa-spin me-1"></i>Activating...');
  $("#aiActivateError").hide();
  $("#aiActivateSuccess").hide();

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
    // Reload the AI panel after a brief delay
    setTimeout(function() { initAi(); }, 1000);
  })
  .fail(function(jqXHR) {
    btn.prop("disabled", false).html("Activate");
    var errorMsg = "Activation failed. Please check your key and try again.";
    try {
      var errData = JSON.parse(jqXHR.responseText);
      if (errData.error) errorMsg = errData.error;
    } catch (e) { /* ignore */ }
    $("#aiActivateError").text(errorMsg).show();
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

  aiCurrentXhr = jQuery.ajax({
    type: "POST",
    url: "api/v1/ai/chat",
    data: JSON.stringify({ database: db, message: message, chatId: aiCurrentChatId }),
    contentType: "application/json",
    beforeSend: function(xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    },
    timeout: 120000
  })
  .done(function(data) {
    aiCurrentXhr = null;
    aiSetSending(false);

    // Update chat ID if new chat was created
    if (data.chatId)
      aiCurrentChatId = data.chatId;

    // Add assistant message
    var assistantMsg = { role: "assistant", content: data.response, timestamp: new Date().toISOString() };
    if (data.commands && data.commands.length > 0)
      assistantMsg.commands = data.commands;
    aiMessages.push(assistantMsg);
    aiRenderMessages();

    // Refresh chat list
    aiLoadChatList();
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
    if (errorCode === "token_invalid" || errorCode === "token_expired" || errorCode === "token_disabled") {
      aiConfigured = false;
      $("#aiActivePanel").hide();
      $("#aiInactivePanel").show();
      globalNotify("Subscription", errorMsg, "warning");
    } else
      globalNotify("Error", errorMsg, "danger");
  });
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
    '<div class="ms-2" style="max-width: 80%; min-width: 0;">' +
    '<div class="ai-message-content" style="color: var(--text-primary); line-height: 1.6;">' + contentHtml + '</div>';

  // Render command blocks if present
  if (msg.commands && msg.commands.length > 0) {
    for (var j = 0; j < msg.commands.length; j++)
      html += aiRenderCommandBlock(msg.commands[j], j);

    // "Execute All" button when multiple commands
    if (msg.commands.length > 1)
      html += '<div class="mt-2"><button class="btn btn-sm" style="background: var(--color-brand); color: white; border: none;" ' +
        'onclick="aiExecuteAll(this, ' + msgBlockStart + ', ' + aiCommandBlockCounter + ')">' +
        '<i class="fa fa-forward me-1"></i>Execute All</button></div>';
  }

  // Action bar with delete button
  html += '<div class="mt-1 ms-1"><button class="btn btn-link btn-sm p-0" style="color: var(--text-muted); font-size: 0.7rem;" ' +
    'onclick="aiDeleteMessage(' + msgIndex + ')" title="Delete message"><i class="fa fa-trash-can"></i></button></div>';

  html += '</div></div></div>';
  return html;
}

function aiRenderCommandBlock(cmd, index) {
  var blockId = "aiCmd_" + (aiCommandBlockCounter++);
  var lang = escapeHtml((cmd.language || "sql").toUpperCase());
  var purpose = cmd.purpose ? '<div style="font-size: 0.85rem; color: var(--text-muted); margin-bottom: 4px;">' + escapeHtml(cmd.purpose) + '</div>' : '';

  return '<div class="ai-command-block" style="margin-top: 8px; border: 1px solid var(--border-main); border-radius: 8px; overflow: hidden; background: var(--bg-card);">' +
    '<div style="padding: 8px 12px; background: var(--bg-sidebar); border-bottom: 1px solid var(--border-main); display: flex; align-items: center; justify-content: space-between;">' +
    '<div>' + purpose +
    '<span class="badge" style="background: var(--color-brand); color: white; font-size: 0.7rem;">' + lang + '</span></div>' +
    '<button class="btn btn-sm" style="background: var(--color-brand); color: white; border: none; font-size: 0.8rem;" onclick="aiExecuteCommand(this, \'' + blockId + '\')">' +
    '<i class="fa fa-play me-1"></i>Execute</button></div>' +
    '<pre id="' + blockId + '" style="margin: 0; padding: 12px; background: var(--bg-code); color: var(--text-code); font-size: 0.85rem; overflow-x: auto; white-space: pre-wrap; word-break: break-word;" ' +
    'data-language="' + escapeHtml(cmd.language || "sql") + '" data-command="' + escapeHtml(cmd.command) + '">' +
    escapeHtml(cmd.command) + '</pre>' +
    '<div id="' + blockId + '_result" style="display: none; padding: 8px 12px; border-top: 1px solid var(--border-main); font-size: 0.85rem;"></div>' +
    '</div>';
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
    url: "api/v1/command/" + encodeURIComponent(db),
    data: JSON.stringify({ language: language, command: command }),
    contentType: "application/json",
    beforeSend: function(xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    }
  })
  .done(function(data) {
    btn.prop("disabled", false).html('<i class="fa fa-play me-1"></i>Execute');
    var resultCount = data.result ? data.result.length : 0;
    resultDiv.show().html('<i class="fa fa-check-circle me-1" style="color: #28a745;"></i> <span style="color: var(--text-primary);">Success' +
      (resultCount > 0 ? ' (' + resultCount + ' results)' : '') + '</span>');

    // Auto-hide after 8 seconds
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
    url: "api/v1/command/" + encodeURIComponent(db),
    data: JSON.stringify({ language: language, command: command }),
    contentType: "application/json",
    beforeSend: function(xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    }
  })
  .done(function(data) {
    item.btn.prop("disabled", false).html('<i class="fa fa-play me-1"></i>Execute');
    var resultCount = data.result ? data.result.length : 0;
    resultDiv.show().html('<i class="fa fa-check-circle me-1" style="color: #28a745;"></i> <span style="color: var(--text-primary);">Success' +
      (resultCount > 0 ? ' (' + resultCount + ' results)' : '') + '</span>');
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

function aiRenderMarkdown(text) {
  if (!text) return "";

  // Use marked.js if available, otherwise basic rendering
  if (typeof marked !== "undefined") {
    try {
      return marked.parse(text);
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
    return '<pre style="background: var(--bg-code); color: var(--text-code); padding: 12px; border-radius: 6px; overflow-x: auto; margin: 8px 0;"><code>' + code.trim() + '</code></pre>';
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
