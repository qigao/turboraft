# TLS test fixtures

These certificates and unencrypted private keys exist only for loopback tests.
They are signed by the test CA in this directory and must never be used by a
deployed node. `node-2.mesh` is the server certificate DNS identity; node 1 uses
the client certificate for mutual TLS authentication.
