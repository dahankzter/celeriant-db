# Client protocol 0.3.0

These assignments are frozen for the coordinated breaking server, Rust, .NET and Go release. Internode traffic uses version 4. Persisted WAL formats retain their existing versions.

| Client version | Body codec |
| --- | --- |
| 4 | Bincode, existing fixed-width little-endian configuration |
| 5 | MessagePack, existing array representation |

Every other version is rejected before execution. Every client connection opens with an Identify, whatever the server requires, and the exchange validates the version. Any other first frame gets a `ProtocolError` response (type 6) and the connection closes. Connections pin their opening version and reject changes. Clients complete a version-checked handshake before sending mutations, including after reconnect or pool expansion. There is no downgrade.

The Identify request carries `known_dict_sha256`. The response carries the server's `compression_dict_sha256`, plus `compression_dict_bytes` only when the shas differ. Clients seed with the built-in dictionary's sha, so a built-in server never ships bytes. The dictionary ceiling is 1 MiB (1,048,576 bytes), and the handshake has its own read limit, independent of the caller's response cap:

| Handshake read | Limit |
| --- | --- |
| V4 IdentifyResponse, client read | ceiling + 128 |
| V5 IdentifyResponse, server write | 2 × ceiling + 128 (MessagePack encodes the dictionary as an integer array; a byte ≥ 0x80 takes 2 bytes) |
| Non-Identify reply to an Identify | min(caller cap, the Identify limit), checked from the header before any body read |

Data frames stay under the caller's cap.

| Response type ID | Body fields, in order |
| --- | --- |
| 13: WriteConflictResponse | correlation_id: optional u128, conflicts: vector of AggregateConflict |
| 14: DeleteConflictResponse | correlation_id: optional u128, conflicts: vector of AggregateConflict |

AggregateConflict fields are `key: AggregateKey`, `expected: u64`, `current: u64`. The key retains the codec's existing binary representation. Lists are nonempty, unique by key, and sorted by unsigned numeric `(org_id, aggregate_type_id, aggregate_id)`. No JSON key, GenericError envelope or legacy fallback is permitted. Numeric client classifications remain write 2003 and delete 4002. Write 2014 is reserved for MalformedGuardEntry; write 2015 and delete 4008 are reserved for conflict response capacity admission failures.

The 17-byte frame header is unchanged. Both conflict responses use the variable response writer, including its small-body uncompressed path. Both compressed and uncompressed lengths must fit the configured ceiling. Compression expansion falls back to the fitting uncompressed body.

Before mutation, reserve `25 + 74 * N` uncompressed body bytes with checked arithmetic, where N counts all entries with expected versions. This admission limit applies even when all pins would succeed. Servers check their response ceiling; clients independently check their receive ceiling. Defaults are 16 MiB server request and 64 MiB server response. Matching request and response limits alone does not prove a response fits.

The exact bincode bound is `25 + 64*N`; MessagePack is at most `24 + 74*N`. Shared fixture assignments live in `fixtures/client-protocol/assignments.txt`; final codec tests must verify maximal unsigned values and vector boundaries against these bounds.

## TailShard (unreleased)

Request type 11 and response type 15 use the same codecs as other messages: V4 bincode, V5 MessagePack arrays.

| Type ID | Body fields, in order |
| --- | --- |
| 11: TailShardRequest | correlation_id: optional u128, shard_id: u64, after_wal_seq: u64, max_bytes: u32, wait_ms: u32, orgs: optional set of u128 |
| 15: TailShardResponse | correlation_id: optional u128, shard_id: u64, entries: vector of TailEntry, read_wal_seq: u64 |

TailEntry fields are `wal_seq: u64`, `server_timestamp: u64`, `kind: TailEntryKind`. The variants of TailEntryKind are:

| Variant | V4 index | Payload, in order |
| --- | --- | --- |
| Events | 0 | aggregate_key: AggregateKey, batch: AggregateEventBatch (as in ReadResponse) |
| Delete | 1 | MetablockSoftDelete: aggregate_key, allow_recreate: bool, allow_sequence_continuation: bool, aggregate_version: u64, event_seq: u64, client_id: u128, user_id: optional u128 |
| Trim | 2 | MetablockSoftTrim: aggregate_key, keep_from_aggregate_version: u64, aggregate_version: u64, event_seq: u64, client_id: u128, user_id: optional u128 |

V4 writes the variant index as a little-endian u32, then the payload. V5 (rmp-serde) writes a single-key map keyed by the variant name, with the payload as an array: `{"Events": [aggregate_key, batch]}`, `{"Delete": [...]}`, `{"Trim": [...]}`. `MetablockSoftDelete` and `MetablockSoftTrim` are the WAL structs sent as they are, so their field order is part of the contract. Enum representation to be agreed with maintainers before non-Rust clients implement it.

Errors arrive as GenericError with codes 12000 to 12003. Cursor ahead (12002) carries `{"tip_wal_seq":N}`.
