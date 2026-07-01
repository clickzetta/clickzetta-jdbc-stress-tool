## 1. Config layer

- [x] 1.1 Add `int ping = 10;` field to `Config.java` (default preserves current behavior)
- [x] 1.2 Parse it in `Config.loadFromFile`: `ping = Integer.parseInt(prop.getProperty("ping", "10"));`
- [x] 1.3 In `Config.validate()`, reject negatives (`if (ping < 0) throw new IllegalArgumentException("ping must be >= 0");`) and echo the value (e.g. `ping : 10`, or `ping : 0 (disabled)`)

## 2. Main — ping block

- [x] 2.1 In `Main.validate()` (around `Main.java:42-61`), guard the ping block with `if (config.ping > 0)` so `0` spawns no process
- [x] 2.2 Replace the hardcoded `ping -c 10` with `ping -c <config.ping>` inside the guarded block

## 3. Main — CLI override

- [x] 3.1 Register a `ping` option in the `Options` builder (`Main.java:157-228`), matching the existing `Option.builder()` style, taking one arg
- [x] 3.2 Apply the override in the arg-parsing block (`Main.java:270-290`): `if (cmd.hasOption("ping")) config.ping = Integer.parseInt(cmd.getOptionValue("ping"));`

## 4. Docs

- [x] 4.1 Document the `ping` property in `streamlit/config.ini.template` with a comment (number of ICMP packets; `0` disables; default `10`)

## 5. Verify

- [x] 5.1 `mvn package` builds cleanly
- [x] 5.2 Manual check: no `ping` value → `ping -c 10` runs (default, backward compatible)
- [x] 5.3 Manual check: `ping=0` (file or `--ping 0`) → no `ping` process spawned, validation proceeds to JDBC step
- [x] 5.4 Manual check: `ping=3` → `ping -c 3` runs
- [x] 5.5 Manual check: `ping=-1` → config validation fails and the test does not start
- [x] 5.6 Manual check: config `ping=0` + CLI `--ping 3` → `ping -c 3` runs (CLI overrides file)

## 6. Streamlit UI (added beyond v1 scope)

- [x] 6.1 Add a `ping` `number_input` widget in `streamlit/run.py` "Advance parameters" section (default `10`, `min_value=0`; `0` disables), using the existing `load_value`/`store_value` session-state pattern
- [x] 6.2 Pass `--ping <ping_count>` in the run command (always passed, UI authoritative — consistent with `-r`/`-t`/`-f`)
