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

// The ZIP writer behind the result table's "Excel" button. An .xlsx file is a ZIP of XML parts, and the DataTables
// Buttons extension builds those parts itself: it only needs a library to package them. That library was JSZip, which
// pulled five unmaintained packages (setimmediate, lie, immediate and readable-stream's helpers) into the Studio jar.
// StudioZip implements the part of the JSZip API that Buttons calls - new, folder(), file(name, string) and
// generateAsync() - and nothing else. Entries are deflated with the browser's own CompressionStream and stored as they
// are where it is missing or a part would not shrink. No ZIP64: a browser-side table export never nears 4 GB.
// Tested in studio/test/excel-export-zip.test.js.

const STUDIO_ZIP_CRC_TABLE = (function () {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    table[n] = c >>> 0;
  }
  return table;
})();

function studioZipCrc32(bytes) {
  let crc = 0xffffffff;
  for (let i = 0; i < bytes.length; i++) crc = STUDIO_ZIP_CRC_TABLE[(crc ^ bytes[i]) & 0xff] ^ (crc >>> 8);
  return (crc ^ 0xffffffff) >>> 0;
}

// Raw DEFLATE (no zlib header), as the ZIP format wants it. Null when the browser cannot compress.
async function studioZipDeflateRaw(bytes) {
  if (typeof CompressionStream === "undefined") return null;
  try {
    const stream = new Blob([bytes]).stream().pipeThrough(new CompressionStream("deflate-raw"));
    return new Uint8Array(await new Response(stream).arrayBuffer());
  } catch (e) {
    return null;
  }
}

// entries: [{ name: Uint8Array (UTF-8), data: Uint8Array, crc, deflated: Uint8Array or null }], in archive order
function studioZipBuild(entries, date) {
  const time = (date.getHours() << 11) | (date.getMinutes() << 5) | (date.getSeconds() >> 1);
  const day = ((Math.max(date.getFullYear(), 1980) - 1980) << 9) | ((date.getMonth() + 1) << 5) | date.getDate();

  let size = 22;
  for (const e of entries) size += 30 + 46 + 2 * e.name.length + (e.deflated || e.data).length;
  if (size > 0xffffffff || entries.length > 0xffff) throw new Error("Export too large for a ZIP archive");

  const out = new Uint8Array(size);
  const view = new DataView(out.buffer);
  const offsets = new Array(entries.length);
  let p = 0;

  // writes the fields local and central headers share, from "version needed" to "extra field length"
  const common = (e, at) => {
    const body = e.deflated || e.data;
    view.setUint16(at, 20, true); // version needed: 2.0
    view.setUint16(at + 2, 0x0800, true); // names are UTF-8
    view.setUint16(at + 4, e.deflated ? 8 : 0, true);
    view.setUint16(at + 6, time, true);
    view.setUint16(at + 8, day, true);
    view.setUint32(at + 10, e.crc, true);
    view.setUint32(at + 14, body.length, true);
    view.setUint32(at + 18, e.data.length, true);
    view.setUint16(at + 22, e.name.length, true);
    view.setUint16(at + 24, 0, true);
  };

  for (let i = 0; i < entries.length; i++) {
    const e = entries[i];
    offsets[i] = p;
    view.setUint32(p, 0x04034b50, true);
    common(e, p + 4);
    out.set(e.name, p + 30);
    p += 30 + e.name.length;
    const body = e.deflated || e.data;
    out.set(body, p);
    p += body.length;
  }

  const cdStart = p;
  for (let i = 0; i < entries.length; i++) {
    const e = entries[i];
    view.setUint32(p, 0x02014b50, true);
    view.setUint16(p + 4, 20, true); // version made by
    common(e, p + 6);
    // comment length, disk number, internal and external attributes stay 0
    view.setUint32(p + 42, offsets[i], true);
    out.set(e.name, p + 46);
    p += 46 + e.name.length;
  }

  view.setUint32(p, 0x06054b50, true);
  view.setUint16(p + 8, entries.length, true);
  view.setUint16(p + 10, entries.length, true);
  view.setUint32(p + 12, p - cdStart, true);
  view.setUint32(p + 16, cdStart, true);
  return out;
}

class StudioZip {
  constructor(prefix, files) {
    this._prefix = prefix || "";
    this._files = files || [];
  }

  folder(name) {
    return new StudioZip(this._prefix + name + "/", this._files);
  }

  file(name, content) {
    this._files.push({ name: this._prefix + name, content: content });
    return this;
  }

  async generateAsync(options) {
    const opts = options || {};
    const encoder = new TextEncoder();
    const entries = [];
    for (const f of this._files) {
      const data = typeof f.content === "string" ? encoder.encode(f.content) : new Uint8Array(f.content);
      let deflated = opts.compression === "DEFLATE" ? await studioZipDeflateRaw(data) : null;
      if (deflated && deflated.length >= data.length) deflated = null;
      entries.push({ name: encoder.encode(f.name), data: data, crc: studioZipCrc32(data), deflated: deflated });
    }
    const bytes = studioZipBuild(entries, new Date());
    if (opts.type === "uint8array") return bytes;
    return new Blob([bytes], { type: opts.mimeType || "application/zip" });
  }
}

if (typeof DataTable !== "undefined" && DataTable.Buttons) DataTable.Buttons.jszip(StudioZip);
