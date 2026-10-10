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

/*
 * Markdown-lite for every piece of untrusted markdown Studio shows: the comments of a support issue (Support tab), the answers
 * of the AI Assistant and the AI analysis of the profiler. The comment that Studio posts for the answer to a support request is
 * "**label**" and a pipe table, support staff write light markdown too, and a model writes headings, lists and tables; shown as
 * plain text all of it is unreadable. None of it is trusted: an AI answer quotes type names and record values from the database
 * and is delivered by a third-party portal (issue #9626), so it must never reach the page through a renderer that keeps raw
 * HTML, as `marked` does.
 *
 * The rule that makes it safe: EVERY character of the input is escaped, and only then are the few elements below written, from
 * fixed templates. Supported: paragraphs and line breaks, # headings, > block quotes, --- rules, **bold**, *italic*, `code`,
 * fenced code blocks, pipe tables, bullet and numbered lists (one level) and [text](url) links whose url is http or https.
 * Nothing else: no raw HTML, no <img> (an image is a plain link), no other URL scheme, no autolinks. Work is bounded for hostile
 * input (see the SUPPORT_MD_* limits, which a caller can raise through the options): a longer body is cut, a longer paragraph
 * is shown as plain text, a bigger table is cut.
 */

var SUPPORT_MD_MAX_CHARS = 20000; // the platform caps a comment at this size too
var SUPPORT_MD_MAX_INLINE = 4000; // a longer paragraph, cell or list item is shown as plain text: bounds the inline matching
var SUPPORT_MD_MAX_ROWS = 200;
var SUPPORT_MD_MAX_COLUMNS = 20;

function supportMdEsc(value) {
  return String(value).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;");
}

/** Bold, italic, code and links of ONE line or paragraph; the result is safe HTML. */
function supportMdInline(text) {
  if (text.length > SUPPORT_MD_MAX_INLINE) return supportMdEsc(text);
  // Code spans and links are taken out first (as placeholders made of a character the input cannot contain), so that no
  // emphasis is applied inside them and the url of a link can never be altered by it.
  var held = [];
  var hold = function (html) {
    held.push(html);
    return "\u0000" + (held.length - 1) + "\u0000";
  };
  var out = supportMdEsc(text);
  out = out.replace(/`([^`\n]+)`/g, function (m, code) {
    return hold("<code>" + code + "</code>");
  });
  // An image (![alt](url)) is shown as a plain link: nothing is loaded from a url on its own
  out = out.replace(/!?\[([^\]\n]+)\]\((https?:\/\/[^\s)]+)\)/gi, function (m, label, url) {
    // Both are already escaped: a quote in the url is &quot;, so it cannot leave the attribute
    return hold('<a href="' + url + '" target="_blank" rel="noopener noreferrer">' + label + "</a>");
  });
  out = out.replace(/\*\*([^\n]+?)\*\*/g, "<strong>$1</strong>");
  out = out.replace(/\*([^\s*][^*\n]*?)\*/g, "<em>$1</em>");
  return out.replace(/\u0000(\d+)\u0000/g, function (m, i) {
    return held[Number(i)];
  });
}

function supportMdIsFence(line) {
  return /^\s*```/.test(line);
}

/** The language named on an opening fence ("```sql"), or "". Only a plain word: it is a label, never markup. */
function supportMdFenceLanguage(line) {
  var m = /^\s*```\s*([A-Za-z0-9_+#.-]{1,32})\s*$/.exec(line);
  return m ? m[1] : "";
}

// Each pattern is anchored at the start and has no nested quantifier over the rest of the line, so it is linear; the closing
// hashes of a heading are stripped by hand (a regex for them backtracks quadratically on a long run of spaces)
var SUPPORT_MD_HEADING = /^\s{0,3}(#{1,6})\s+(.*)$/;
var SUPPORT_MD_RULE = /^\s{0,3}([-*_])(\s*\1){2,}\s*$/;
var SUPPORT_MD_QUOTE = /^\s{0,3}>\s?(.*)$/;

/** The text of a heading without its optional closing hashes ("## Title ##" -> "Title"). */
function supportMdHeadingText(text) {
  var end = text.length;
  while (end > 0 && /\s/.test(text.charAt(end - 1))) end--;
  var hashes = end;
  while (hashes > 0 && text.charAt(hashes - 1) === "#") hashes--;
  if (hashes < end && (hashes === 0 || /\s/.test(text.charAt(hashes - 1)))) end = hashes;
  return text.substring(0, end).trim();
}

/** A heading, rule or quote is at most a paragraph long: a longer line is a paragraph, shown as plain text. */
function supportMdIsBlock(pattern, line) {
  return line.length <= SUPPORT_MD_MAX_INLINE && pattern.test(line);
}

var SUPPORT_MD_SEPARATOR = /^\s*\|?\s*:?-+:?\s*(\|\s*:?-+:?\s*)*\|?\s*$/;
var SUPPORT_MD_BULLET = /^\s*[-*+]\s+(.*)$/;
var SUPPORT_MD_NUMBER = /^\s*\d{1,9}[.)]\s+(.*)$/;

function supportMdIsTableStart(lines, i) {
  return i + 1 < lines.length && lines[i].indexOf("|") >= 0 && lines[i + 1].indexOf("|") >= 0 && SUPPORT_MD_SEPARATOR.test(lines[i + 1]);
}

/** The cells of one table line: the outer pipes are optional and \| is a pipe inside a cell. */
function supportMdCells(line) {
  var cells = [];
  var current = "";
  var s = line.trim();
  if (s.charAt(0) === "|") s = s.substring(1);
  if (s.length && s.charAt(s.length - 1) === "|" && s.charAt(s.length - 2) !== "\\") s = s.substring(0, s.length - 1);
  for (var k = 0; k < s.length; k++) {
    var c = s.charAt(k);
    if (c === "\\" && s.charAt(k + 1) === "|") {
      current += "|";
      k++;
    } else if (c === "|") {
      cells.push(current.trim());
      current = "";
    } else current += c;
    if (cells.length >= SUPPORT_MD_MAX_COLUMNS) return cells;
  }
  cells.push(current.trim());
  return cells.slice(0, SUPPORT_MD_MAX_COLUMNS);
}

function supportMdTable(header, rows) {
  var html = '<div class="table-responsive"><table class="table table-sm table-bordered support-md-table"><thead><tr>';
  for (var h = 0; h < header.length; h++) html += "<th>" + supportMdInline(header[h]) + "</th>";
  html += "</tr></thead><tbody>";
  for (var r = 0; r < rows.length; r++) {
    html += "<tr>";
    for (var c = 0; c < header.length; c++) html += "<td>" + supportMdInline(rows[r][c] === undefined ? "" : rows[r][c]) + "</td>";
    html += "</tr>";
  }
  return html + "</tbody></table></div>";
}

/**
 * The HTML of a markdown text: safe to put in the page as it is. The options are all optional:
 * - maxChars, maxRows: raise the SUPPORT_MD_MAX_CHARS / SUPPORT_MD_MAX_ROWS bounds (an AI answer is longer than a comment);
 * - codeBlock(code, language): draws a fenced block instead of <pre><code>. It receives the RAW code and language and must
 *   escape them itself.
 */
function supportMarkdownHtml(text, options) {
  if (text == null) return "";
  var opts = options || {};
  var maxChars = opts.maxChars > 0 ? opts.maxChars : SUPPORT_MD_MAX_CHARS;
  var maxRows = opts.maxRows > 0 ? opts.maxRows : SUPPORT_MD_MAX_ROWS;
  var source = String(text).replace(/\u0000/g, "").replace(/\r\n?/g, "\n");
  if (source.length > maxChars) source = source.substring(0, maxChars);
  var lines = source.split("\n");
  var html = "";
  var paragraph = [];
  var flush = function () {
    if (paragraph.length) {
      html += "<p>" + paragraph.map(supportMdInline).join("<br>") + "</p>";
      paragraph = [];
    }
  };

  var i = 0;
  while (i < lines.length) {
    var line = lines[i];
    if (!line.trim()) {
      flush();
      i++;
    } else if (supportMdIsFence(line)) {
      flush();
      var language = supportMdFenceLanguage(line);
      var code = [];
      i++;
      while (i < lines.length && !supportMdIsFence(lines[i])) code.push(lines[i++]);
      i++; // the closing fence, or the end of the text for one that never closes
      html += typeof opts.codeBlock === "function" ? opts.codeBlock(code.join("\n"), language) : "<pre><code>" + supportMdEsc(code.join("\n")) + "</code></pre>";
    } else if (supportMdIsBlock(SUPPORT_MD_HEADING, line)) {
      flush();
      var heading = SUPPORT_MD_HEADING.exec(line);
      var level = heading[1].length;
      html += "<h" + level + ">" + supportMdInline(supportMdHeadingText(heading[2])) + "</h" + level + ">";
      i++;
    } else if (supportMdIsBlock(SUPPORT_MD_RULE, line)) {
      flush();
      html += "<hr>";
      i++;
    } else if (supportMdIsBlock(SUPPORT_MD_QUOTE, line)) {
      flush();
      var quoted = [];
      while (i < lines.length && supportMdIsBlock(SUPPORT_MD_QUOTE, lines[i])) {
        if (quoted.length < maxRows) quoted.push(SUPPORT_MD_QUOTE.exec(lines[i])[1]);
        i++;
      }
      // One level, as a paragraph: a nested '>' stays as text
      html += "<blockquote><p>" + quoted.map(supportMdInline).join("<br>") + "</p></blockquote>";
    } else if (supportMdIsTableStart(lines, i)) {
      flush();
      var header = supportMdCells(line);
      var rows = [];
      i += 2;
      while (i < lines.length && lines[i].trim() && lines[i].indexOf("|") >= 0) {
        if (rows.length < maxRows) rows.push(supportMdCells(lines[i]));
        i++;
      }
      html += supportMdTable(header, rows);
    } else if (SUPPORT_MD_BULLET.test(line) || SUPPORT_MD_NUMBER.test(line)) {
      flush();
      var ordered = !SUPPORT_MD_BULLET.test(line);
      var pattern = ordered ? SUPPORT_MD_NUMBER : SUPPORT_MD_BULLET;
      var items = "";
      var count = 0;
      while (i < lines.length && pattern.test(lines[i])) {
        if (count++ < maxRows) items += "<li>" + supportMdInline(pattern.exec(lines[i])[1]) + "</li>";
        i++;
      }
      html += (ordered ? "<ol>" : "<ul>") + items + (ordered ? "</ol>" : "</ul>");
    } else {
      paragraph.push(line);
      i++;
    }
  }
  flush();
  return html;
}
