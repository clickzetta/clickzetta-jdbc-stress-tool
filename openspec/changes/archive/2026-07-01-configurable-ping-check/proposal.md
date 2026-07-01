## Why

Before every test the tool runs `ping -c 10 <host>` as a host-reachability pre-check. That ping is already non-fatal — its failures are caught and printed, never gating the run (the real gate is the JDBC connect + connection-pool warm-up). In environments where ICMP is blocked by network policy, every packet returns "Destination Port Unreachable", so the check costs ~10 packets of wall-clock time on every run with zero functional value. There is currently no way to disable or shorten it.

## What Changes

- Introduce a configurable integer `ping` (default `10`) that controls the ICMP packet count used by the pre-test host-reachability check — it maps directly to `ping -c N`.
- `ping = 0` disables the check entirely: the tool does not shell out to `ping` at all. (`0` is the literal "send zero packets", not a magic off-switch.)
- Make `ping` settable via both the config-file property `ping` and a CLI arg `--ping` (CLI overrides the file), consistent with how `thread`/`repeat`/`prefix`/`failure` already work.
- Reject negative values as a configuration error during validation.
- Document the property in `config.ini.template`.

**Streamlit UI (added beyond v1):** a `ping` `number_input` widget in the Streamlit runner's "Advance parameters" section, wired to `--ping` (default `10`, `0` disables). Originally deferred as out-of-scope for v1; added afterward on request.

## Capabilities

### New Capabilities
- `pre-test-validation`: the validation phase that runs before the stress test (host-reachability check + JDBC connect / connection-pool warm-up). This change specifies the configurable, optional host-reachability check.

### Modified Capabilities
<!-- None — no existing specs in openspec/specs/. -->

## Impact

- **Code:** `Main.java` (the ping block in `validate()` + CLI `Options`/arg parsing) and `Config.java` (new `ping` field, `loadFromFile` parsing, `validate()` guard + echo).
- **Config/docs:** `streamlit/config.ini.template` (document the `ping` property).
- **JDBC driver compatibility:** none.
- **Performance:** net positive — removes the per-run packet cost in ICMP-restricted environments; negligible change elsewhere (default preserves current behavior).
- **Behavior compatibility:** backward compatible — default `ping = 10` reproduces today's behavior exactly.
