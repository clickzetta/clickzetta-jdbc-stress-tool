## Context

`Main.validate()` runs two things before every stress test:

1. A host-reachability pre-check that shells out `ping -c 10 <host>`, streams its output, and swallows any `Throwable` (print-only).
2. The real gate: a JDBC connect + connection-pool warm-up that returns success/failure and drives `System.exit`.

The ping is therefore purely diagnostic and already non-fatal. In ICMP-restricted environments it returns "Destination Port Unreachable" on every packet yet still consumes ~10 packets of wall-clock per run, with no way to skip or shorten it.

## Goals / Non-Goals

**Goals:**
- Let users disable the host-reachability ping, or reduce its packet count, without code changes.
- Preserve today's exact behavior by default (backward compatible).
- Keep the change minimal and confined to the existing config + CLI plumbing.

**Non-Goals:**
- No change to the JDBC connect / pool warm-up gate.
- No cross-platform `ping` flag handling (the tool targets Linux/Docker; `ping -c` is correct there). Windows-local `ping -n` remains a pre-existing limitation, out of scope.

## Decisions

**D1: `ping` is an integer, not a boolean.**
`ping -c N` is inherently parameterized by a count. Exposing that count directly means one knob serves both "disable" and "tune how many packets". Alternative considered: a boolean `ping` on/off plus a separate `pingCount`. Rejected because it hides the real parameter and forces the count to stay hardcoded. `ping = 0` is the literal "send zero packets", not an invented sentinel.

**D2: Default `ping = 10`.**
Reproduces current behavior exactly — zero behavior change for existing configs. Alternative considered: default `0` (skip by default). Rejected: it would silently remove a diagnostic others may rely on and changes behavior for everyone.

**D3: Reject negative values during validation.**
`ping -c -1` is meaningless and behaves oddly at the shell, so `Config.validate()` MUST throw for `ping < 0`, consistent with the existing `IllegalArgumentException` guards (null url/user/password/output, empty sql).

**D4: Guard `0` in Java, never pass it to the shell.**
`if (config.ping > 0) rt.exec("ping -c " + config.ping + " " + host)`. `ping -c 0` behaves inconsistently across systems, so `0` MUST short-circuit before any shell-out rather than being passed through.

**D5: Settable via config-file property and CLI arg; CLI overrides file.**
Mirrors how `thread`/`repeat`/`prefix`/`failure` already work (file default in `Config.loadFromFile`, override in `Main`'s `Options`/`cmd.hasOption`). Keeps the surface consistent and lets CI/CD toggle it per-run without editing a file.

**D6: Terse property name `ping`, documented in the template.**
Matches the codebase style (`pool`, `init`, `thread`, `repeat`, `failure`). Readability of "10 of what?" is handled by a comment in `config.ini.template` and an echo line in `Config.validate()`.

**D7: Streamlit widget mirrors `repeat`/`thread`/`failure` (added beyond v1).**
A `number_input` (default `10`, `min=0`) in the runner's "Advance parameters" section, always passed as `--ping` so the UI is authoritative over the config file — consistent with how the runner already passes `-r`/`-t`/`-f`. `min=0` makes it impossible to enter the negatives that `Config.validate()` rejects, and `0` disables the check.

## Risks / Trade-offs

- **[Slightly less self-documenting than a boolean]** `ping = 10` doesn't say "packets" on its face → mitigated by the template comment and a `validate()` echo (e.g. `ping : 10`, or `ping : 0 (disabled)`).
- **[Malformed input surface]** An int invites bad values (negative, non-numeric) → mitigated by D3; `NumberFormatException` from `Integer.parseInt` is also caught as a load-time config error.
- **[Pre-existing platform fragility]** `ping -c` is Linux/macOS; a Windows-local run would break → not introduced by this change; explicitly a non-goal here.
- **[No migration needed]** Default reproduces prior behavior, so deployment is drop-in. Rollback = revert the change.

## Open Questions

None blocking. The Streamlit UI control was the only deferred item and has since been added (see D7).
