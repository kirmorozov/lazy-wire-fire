# Test-only keys

Throwaway keys for the unit tests (the RS256 assertion of `vars.oauth=jwt_bearer`,
`src/oauth.rs`). Generated once with OpenSSL for this repository; they guard nothing and
are not a credential of any system — never use one of them outside the tests.

- `rsa-test-key-pkcs8.pem` / `rsa-test-key-pkcs1.pem` — one RSA-2048 key, both encodings
  (`BEGIN PRIVATE KEY` and `BEGIN RSA PRIVATE KEY`): the loader must read either.
- `rsa-test-key-b-pkcs8.pem` — a second RSA-2048 key: new bytes behind a key reference.
- `ec-test-key-pkcs8.pem` — an EC key in PKCS#8: it parses as a key but cannot sign RS256,
  so it is refused at use naming the cause.
