#!/usr/bin/env python3
#
# Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
# SPDX-License-Identifier: Apache-2.0
#
# Usage: otlp_sink.py <out-dir>
#
# A stand-in OTLP collector for the native-image smoke test (issue #9425), stdlib only, so the export path of the
# arcadedb-metrics and arcadedb-tracing plugins actually runs - serialization, the HTTP client, the gRPC framing -
# instead of failing to connect. The smoke test used to assert only that both plugins STARTED, which leaves whatever
# reflection or resource metadata the exporters need unproven inside the native image.
#
# Two listeners on 127.0.0.1, each on a port the OS picks:
#   - metrics: OTLP/HTTP protobuf (what Micrometer's OTLP registry speaks), a plain HTTP/1.1 server answering 200;
#   - traces: OTLP/gRPC (what OtlpGrpcSpanExporter speaks), a minimal cleartext HTTP/2 (h2c prior knowledge) server
#     that completes the handshake and answers every call with an empty message and grpc-status 0.
#
# Writes into <out-dir>:
#   metrics.port / traces.port         the bound ports, written (atomically) once the listener accepts connections;
#   metrics.requests / traces.requests one line per request received ("POST /v1/metrics 1234" / "grpc 1234");
#   metrics.body / traces.body         the raw request payloads appended, so a caller can grep for what it expects
#                                      (protobuf carries strings as plain UTF-8, e.g. the "service.name" key).
# Runs until killed.

import os
import socket
import struct
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

OUT = sys.argv[1]
LOCK = threading.Lock()

H2_PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
DATA, HEADERS, SETTINGS, PING, GOAWAY = 0x0, 0x1, 0x4, 0x6, 0x7
END_STREAM, ACK, END_HEADERS, PADDED = 0x1, 0x1, 0x4, 0x8

# HPACK: ":status: 200" is static-table index 8; "content-type" is index 31 used as a literal-without-indexing name
# (4-bit prefix: 0x0f then 31 - 15); "grpc-status" is not in the static table, so it is sent as a new literal name
RESPONSE_HEADERS = b"\x88" + b"\x0f\x10" + bytes([16]) + b"application/grpc"
RESPONSE_TRAILERS = b"\x00" + bytes([11]) + b"grpc-status" + bytes([1]) + b"0"
EMPTY_GRPC_MESSAGE = b"\x00\x00\x00\x00\x00"


def record(kind, line, body):
  with LOCK:
    with open(os.path.join(OUT, kind + ".body"), "ab") as f:
      f.write(body)
    with open(os.path.join(OUT, kind + ".requests"), "a") as f:
      f.write(line + "\n")


def publish_port(kind, port):
  tmp = os.path.join(OUT, kind + ".port.tmp")
  with open(tmp, "w") as f:
    f.write(str(port))
  os.replace(tmp, os.path.join(OUT, kind + ".port"))


class MetricsHandler(BaseHTTPRequestHandler):
  def do_POST(self):
    body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
    record("metrics", "POST %s %d" % (self.path, len(body)), body)
    self.send_response(200)
    self.send_header("Content-Type", "application/x-protobuf")
    self.send_header("Content-Length", "0")
    self.end_headers()

  def log_message(self, fmt, *args):
    pass


def frame(frame_type, flags, stream_id, payload=b""):
  return struct.pack(">I", len(payload))[1:] + bytes([frame_type, flags]) + struct.pack(">I", stream_id) + payload


def read_exactly(conn, n):
  buf = b""
  while len(buf) < n:
    chunk = conn.recv(n - len(buf))
    if not chunk:
      raise EOFError()
    buf += chunk
  return buf


def serve_h2(conn):
  try:
    if read_exactly(conn, len(H2_PREFACE)) != H2_PREFACE:
      return
    conn.sendall(frame(SETTINGS, 0, 0))
    bodies = {}
    while True:
      header = read_exactly(conn, 9)
      length = struct.unpack(">I", b"\x00" + header[:3])[0]
      frame_type, flags = header[3], header[4]
      stream_id = struct.unpack(">I", header[5:9])[0] & 0x7FFFFFFF
      payload = read_exactly(conn, length) if length else b""
      if frame_type == SETTINGS and not flags & ACK:
        conn.sendall(frame(SETTINGS, ACK, 0))
      elif frame_type == PING and not flags & ACK:
        conn.sendall(frame(PING, ACK, 0, payload))
      elif frame_type == GOAWAY:
        return
      elif frame_type == DATA:
        if flags & PADDED:
          payload = payload[1:len(payload) - payload[0]]
        bodies[stream_id] = bodies.get(stream_id, b"") + payload
        if flags & END_STREAM:
          body = bodies.pop(stream_id)
          record("traces", "grpc %d" % len(body), body)
          conn.sendall(frame(HEADERS, END_HEADERS, stream_id, RESPONSE_HEADERS)
                       + frame(DATA, 0, stream_id, EMPTY_GRPC_MESSAGE)
                       + frame(HEADERS, END_HEADERS | END_STREAM, stream_id, RESPONSE_TRAILERS))
  except (EOFError, OSError):
    pass
  finally:
    conn.close()


def serve_traces(listener):
  while True:
    conn, _ = listener.accept()
    threading.Thread(target=serve_h2, args=(conn,), daemon=True).start()


def main():
  metrics = ThreadingHTTPServer(("127.0.0.1", 0), MetricsHandler)
  traces = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  traces.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  traces.bind(("127.0.0.1", 0))
  traces.listen(16)

  threading.Thread(target=serve_traces, args=(traces,), daemon=True).start()
  publish_port("traces", traces.getsockname()[1])
  publish_port("metrics", metrics.server_address[1])
  metrics.serve_forever()


if __name__ == "__main__":
  main()
