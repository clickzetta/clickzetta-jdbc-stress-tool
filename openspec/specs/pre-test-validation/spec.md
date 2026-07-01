## Purpose
The validation phase that runs before the stress test: a host-reachability check (ICMP `ping`) followed by the JDBC connect / connection-pool warm-up. Only the JDBC connection validation gates whether the test runs; the host-reachability check is best-effort.

## Requirements

### Requirement: Configurable host-reachability packet count
The system SHALL provide a `ping` configuration value (config-file property `ping` and CLI arg `--ping`) that controls the number of ICMP packets sent during the pre-test host-reachability check, mapping directly to `ping -c N`. The default value SHALL be `10`.

#### Scenario: Default count preserves existing behavior
- **WHEN** no `ping` value is provided in the config file or on the CLI
- **THEN** the system runs `ping -c 10 <host>` during validation

#### Scenario: Custom packet count
- **WHEN** `ping` is set to `3`
- **THEN** the system runs `ping -c 3 <host>` during validation

### Requirement: Disabling the host-reachability check
The system SHALL treat `ping = 0` as "send zero packets" and SHALL skip the host-reachability check entirely, spawning no `ping` process. The system SHALL proceed directly to the JDBC connection validation.

#### Scenario: Zero disables the ping pre-check
- **WHEN** `ping` is set to `0`
- **THEN** no `ping` process is spawned and validation continues to the JDBC connect / pool warm-up step

### Requirement: Negative ping values are rejected
The system SHALL reject `ping` values less than zero as a configuration error during validation, before any test runs.

#### Scenario: Negative value fails validation
- **WHEN** `ping` is set to a negative value (e.g. `-1`)
- **THEN** config validation fails with an error and the stress test does not start

### Requirement: CLI overrides config-file ping value
When both are supplied, the CLI `--ping` value SHALL take precedence over the config-file `ping` property.

#### Scenario: CLI overrides a disabled file value
- **WHEN** the config file sets `ping = 0` and the CLI passes `--ping 3`
- **THEN** the system runs `ping -c 3 <host>`

#### Scenario: CLI disables a file-enabled value
- **WHEN** the config file sets `ping = 10` and the CLI passes `--ping 0`
- **THEN** no `ping` process is spawned

### Requirement: Host-reachability check is non-fatal
The host-reachability check SHALL be best-effort: any failure (unreachable host, `ping` binary missing, exception) SHALL be reported but SHALL NOT abort the test. Only the JDBC connection validation gates whether the test runs.

#### Scenario: Ping failure does not block the test
- **WHEN** `ping > 0` and the host is unreachable or `ping` errors
- **THEN** the error is reported and validation continues to the JDBC connect / pool warm-up step, which independently determines whether the test proceeds
