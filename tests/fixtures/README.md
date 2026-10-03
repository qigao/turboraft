# TLS test fixtures

These certificates and unencrypted private keys exist only for loopback tests.
They are signed by the test CA in this directory and must never be used by a
deployed node. `node-2.mesh` is the server certificate DNS identity; node 1 uses
the client certificate for mutual TLS authentication.

The primary CA and node-1/node-2 leaf fixtures use a ten-year validity window
inside 2026-2036 so both OpenSSL-style and GmSSL X.509 validators exercise the
same intended certificate policy. Avoid century-long test certificates because
stricter TLS backends may reject that validity encoding before TurboRaft's peer
identity policy runs.
