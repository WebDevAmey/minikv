# MiniKV

A distributed key-value store built from scratch in TypeScript, using a hand-rolled implementation of the **Raft consensus algorithm** over raw TCP sockets. No HTTP framework, no gRPC, no external consensus library — just `node:net` and the algorithm itself.

This project exists to understand Raft by building it: leader election, log replication, quorum commits, crash recovery, and the failure modes that a from-scratch implementation inevitably surfaces.

> Looking for diagrams and step-by-step flows (leader election, write path, recovery)? See **[ARCHITECTURE.md](./ARCHITECTURE.md)**.

## Features

- **Leader election** with randomized timeouts and term-based voting
- **Log replication** with majority (quorum) acknowledgement before commit
- **Heartbeats** to suppress unnecessary elections and propagate commit progress
- **Crash recovery** — each node persists its state machine, Raft metadata, and log to disk, and replays them on restart
- **A tiny text-based wire protocol** for both client operations (`PUT`/`GET`/`DELETE`) and inter-node Raft RPCs (`REQUEST_VOTE`/`HEARTBEAT`/`APPEND_ENTRY`)
- **An interactive CLI client** for talking to any node in the cluster

## Tech Stack

| Layer | Choice |
|---|---|
| Language | TypeScript (strict mode) |
| Runtime | Node.js |
| Networking | `node:net` (raw TCP, no HTTP) |
| Dev execution | [`tsx`](https://github.com/privatenumber/tsx) |
| Persistence | Plain JSON files on local disk (per node) |
| Dependencies | None at runtime — `typescript`/`tsx`/`@types/node` are dev-only |

## Project Structure

```
minikv/
├── README.md
├── ARCHITECTURE.md
├── package.json
├── tsconfig.json
└── src/
    ├── server.ts             # A cluster node: TCP server + Raft engine + KV store
    ├── client.ts             # Interactive TCP REPL client
    ├── data-<nodeId>.json    # (generated) snapshot of the committed KV store
    ├── raft-<nodeId>.json    # (generated) Raft's persistent metadata
    └── log-<nodeId>.json     # (generated) the replicated Raft log
```

`server.ts` is the entire node: it is simultaneously a Raft peer and the thing that serves client reads/writes — there is no separate "consensus layer" process.

## Getting Started

Install dependencies:

```bash
npm install
```

Start a 3-node cluster (one command per terminal). The three nodes are hardcoded in `server.ts` as `node1:4000`, `node2:4001`, `node3:4002`, so the `<port>`/`<nodeId>` pair you pass must match that table:

```bash
# Terminal 1
npx tsx src/server.ts 4000 node1

# Terminal 2
npx tsx src/server.ts 4001 node2

# Terminal 3
npx tsx src/server.ts 4002 node3
```

Within a few seconds one node will win an election and print `became LEADER for term N`. Connect a client to **any** node — reads work everywhere, writes only against the leader:

```bash
# Terminal 4
npx tsx src/client.ts 4000
```

### Example session

```
Connected to node on port 4000
Commands: PUT key value | GET key | DELETE key
> PUT name Amey
OK index=1 term=2
> GET name
Amey
> DELETE name
OK index=2 term=2
> GET name
NOT_FOUND
```

If you connect to a follower instead of the leader:

```
> PUT name Amey
ERROR Not leader
```

`GET` still works against a follower, since reads are answered from local state without consulting the leader (see [Known Limitations](#known-limitations--simplifications)).

## Wire Protocol

Every message is a single line of space-separated tokens, terminated by `\n`. There is no framing beyond newlines and no length prefixes.

| Command | Sent by | Format | Possible responses |
|---|---|---|---|
| `PUT` | Client | `PUT <key> <value>` | `OK index=<i> term=<t>` · `ERROR Not leader` · `ERROR Usage: PUT key value` · `ERROR Majority not reached` |
| `GET` | Client | `GET <key>` | `<value>` · `NOT_FOUND` · `ERROR Usage: GET key` |
| `DELETE` | Client | `DELETE <key>` | `OK index=<i> term=<t>` · `NOT_FOUND` · `ERROR Not leader` |
| `REQUEST_VOTE` | Node → Node | `REQUEST_VOTE <term> <candidateId>` | `VOTE_GRANTED` · `VOTE_DENIED` |
| `HEARTBEAT` | Leader → Node | `HEARTBEAT <term> <leaderId> <commitIndex>` | `ALIVE` · `STALE` |
| `APPEND_ENTRY` | Leader → Node | `APPEND_ENTRY <term> <leaderCommit> <index> <entryTerm> <command>` | `APPENDED` · `REJECTED` |
| `PING` | Anyone | `PING` | `PONG` |
| `QUIT` | Client | `QUIT` | `BYE` |

`PUT`/`GET`/`DELETE` are only ever sent by a client. Everything else is a Raft RPC exchanged between the three nodes.

## Persistence Model

Every node owns three JSON files, named by its `nodeId`. Nothing is shared between nodes on disk — replication happens purely over the network.

| File | Written by | Contents | Purpose |
|---|---|---|---|
| `data-<nodeId>.json` | `saveData()` | The KV store (`Map` flattened to an object) | Snapshot of the **applied** state machine |
| `raft-<nodeId>.json` | `saveRaftState()` | `currentTerm`, `votedFor`, `commitIndex`, `lastApplied` | Raft's durable metadata — required so a restarted node can't vote twice in the same term or forget what it already committed |
| `log-<nodeId>.json` | `saveLog()` | Array of `{ index, term, command }` | The replicated log — the source of truth that the state machine is replayed from |

On boot, a node loads all three files, clamps `commitIndex`/`lastApplied` to what the log actually contains, replays any entries it hadn't applied yet, and only then starts listening and joins the cluster as a follower. See [ARCHITECTURE.md](./ARCHITECTURE.md#5-node-restart--recovery) for the full recovery flow.

## Known Limitations / Simplifications

This is a learning implementation, not a production Raft. Deviations from the Raft paper worth knowing about:

- **Reads aren't linearizable.** `GET` is answered from whatever is in local memory, on whichever node you're connected to — including a follower that may be behind, or a leader that has just lost quorum without knowing it yet.
- **No log-matching check on `APPEND_ENTRY`.** Real Raft rejects an entry unless the follower's log agrees with the leader at `prevLogIndex`/`prevLogTerm`. Here, a follower simply accepts any entry for an index it doesn't already have — under reordering or partition, this can let logs diverge in ways stock Raft's consistency check would prevent.
- **No "up-to-date log" check on `REQUEST_VOTE`.** Real Raft refuses to vote for a candidate whose log is behind the voter's own. Here, a vote is granted purely by term/`votedFor` bookkeeping, so a candidate with a less-complete log could in principle win an election.
- **No snapshotting or log compaction.** `log-<nodeId>.json` grows forever; there's no `InstallSnapshot` equivalent.
- **Static cluster membership.** The 3-node list is a hardcoded array in `server.ts`; nodes can't be added or removed at runtime.
- **No auth or transport security.** Anything that can open a TCP connection to a node's port can issue client commands *and* Raft RPCs.
- **No client redirect.** A write against a follower fails with `ERROR Not leader` instead of being forwarded to the actual leader.

## Possible Extensions

- Add `prevLogIndex`/`prevLogTerm` consistency checking to `APPEND_ENTRY`
- Add the log-up-to-date check to `REQUEST_VOTE`
- Snapshot + compact the log once it passes a size threshold
- Forward writes from a follower to the current leader instead of erroring
- Make cluster membership configurable instead of hardcoded
