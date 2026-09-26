# ⚡ ByteCache — A Redis Server Built From Raw TCP Sockets

<p align="center">
  <img src="./assets/Banner.png" alt="ByteCache Banner" width="100%" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Java-25-orange?style=for-the-badge&logo=openjdk&logoColor=white" />
  <img src="https://img.shields.io/badge/Build-Maven-blue?style=for-the-badge&logo=apachemaven&logoColor=white" />
  <img src="https://img.shields.io/badge/Protocol-RESP2-red?style=for-the-badge" />
  <img src="https://img.shields.io/badge/Framework-None-critical?style=for-the-badge" />
  <img src="https://img.shields.io/badge/Concurrency-Thread--per--Connection-blueviolet?style=for-the-badge" />
  <img src="https://img.shields.io/badge/Transactions-MULTI%2FEXEC-success?style=for-the-badge" />
  <img src="https://img.shields.io/badge/status-active%20development-yellow?style=for-the-badge" />
</p>

> **ByteCache** is a Redis-inspired, in-memory data store built entirely from **raw Java sockets** — no Redis binary underneath, no Spring Boot, no networking library doing the hard part for you. It speaks real **RESP2** well enough that an unmodified `redis-cli` can talk to it, runs a **command-dispatcher architecture** across generic, string, numeric, stream, and transaction commands, handles **persistent, concurrent client connections**, and implements both **Redis Streams** (with blocking reads) and **Redis-style transactions** (`MULTI` / `EXEC` / `DISCARD`) from first principles.

> [!NOTE]
> This is a systems-engineering showcase, not a Redis replacement. The interesting part isn't the feature list — it's that every layer between "a client typed `SET`" and "a value sits in memory" was written and understood by hand: socket lifecycle, byte-stream framing, protocol parsing, command dispatch, per-connection transaction state, and concurrent shared storage.

> [!WARNING]
> ByteCache is actively evolving. Persistence, benchmarking, and stronger concurrency guarantees on compound operations are in progress — see [Known Limitations](#-known-limitations) and the [Roadmap](#️-roadmap) for the honest current state.

---

## ⭐ Support

If this project helped you understand what's really happening underneath Redis — or underneath any client-server system — consider giving it a star. It genuinely helps.

---

## 📖 Table of Contents

- [🚀 Why This Project Exists](#-why-this-project-exists)
- [✨ Key Features](#-key-features)
- [🏗️ High-Level Architecture](#️-high-level-architecture)
- [🔬 The Journey of a Command](#-the-journey-of-a-command)
- [🧩 Command Architecture](#-command-architecture)
- [🔁 Transactions Engine](#-transactions-engine)
- [🗄️ Storage Model & State Ownership](#️-storage-model--state-ownership)
- [🌊 Redis Streams Internals](#-redis-streams-internals)
- [📡 RESP Protocol & Wire Examples](#-resp-protocol--wire-examples)
- [🧵 Concurrency Model](#-concurrency-model)
- [⚠️ Known Concurrency Limitations](#️-known-concurrency-limitations)
- [🔌 Supported Commands](#-supported-commands)
- [❌ Failure Semantics](#-failure-semantics)
- [🔍 Engineering Deep Dives](#-engineering-deep-dives)
- [🐛 Interesting Bugs & Debugging Stories](#-interesting-bugs--debugging-stories)
- [🧭 Design Decisions & Trade-offs](#-design-decisions--trade-offs)
- [📂 Project Structure](#-project-structure)
- [⚙️ How to Run](#️-how-to-run)
- [🧪 Testing Strategy](#-testing-strategy)
- [💻 Example Sessions](#-example-sessions)
- [⚔️ ByteCache vs Redis](#️-bytecache-vs-redis)
- [🕰️ Architecture Evolution](#️-architecture-evolution)
- [🧗 Learning Milestones](#-learning-milestones)
- [🚫 Known Limitations](#-known-limitations)
- [🔮 Future Vision — Planned Architecture](#-future-vision--planned-architecture)
- [🗺️ Roadmap](#️-roadmap)
- [🏆 Resume-Worthy Engineering](#-resume-worthy-engineering)
- [📚 What I'm Learning](#-what-im-learning)
- [🤝 Contribution](#-contribution)
- [⚠️ Disclaimer](#️-disclaimer)
- [📜 License](#-license)

---

## 🚀 Why This Project Exists

Most developers only ever meet Redis through a client library:

```java
redis.set("user:42", "Anuj");
```

Clean. Convenient. And it hides almost everything interesting.

ByteCache deliberately starts several layers lower — at the point where none of that convenience exists yet:

```
TCP connection → raw bytes → RESP parsing → command dispatch → in-memory data structures
```

Building it means confronting the questions a framework usually answers *for* you:

- What does a Redis command actually look like on the wire?
- Why can't a single `Socket.read()` call be treated as a single command?
- How does one server handle hundreds of persistent client connections at once?
- What does `MULTI` / `EXEC` actually queue, and where does that queue live?
- How are Redis Stream IDs ordered, validated, and auto-generated — and how does a *blocking* `XREAD` even work?
- What breaks when two threads touch the same in-memory map, or the same transaction queue, at the same time?

There's no Spring Boot here on purpose — networking, protocol handling, and command dispatch are the point, not implementation details hidden behind an annotation.

<p align="center">
  <img src="./assets/Overall_Concept.png" alt="Concept Diagram — Client to Server to Store" width="85%" />
  <br/>
  <i>Client → TCP socket → RESP parser → command dispatcher → in-memory store, with every arrow hand-implemented</i>
</p>

---

## ✨ Key Features

- ✅ **Zero frameworks** — pure `java.net`, no Netty, no Spring, no Redis client library underneath
- ✅ **Real wire compatibility** — speaks RESP2 well enough for an unmodified `redis-cli` session to work against it
- ✅ **Command-dispatcher architecture** — commands are grouped into `Generic`, `String`, `Numeric`, `Stream`, and `Transaction` categories, dispatched through a `Map<String, Command>` rather than a hardcoded if/else chain
- ✅ **Hand-rolled protocol parsing** — incremental buffering across partial reads, correctly splits pipelined commands arriving in a single read
- ✅ **Redis-style transactions** — `MULTI`, command queuing, `EXEC`, `DISCARD`, per-connection transaction state, and concurrent transactions across multiple clients
- ✅ **Thread-per-connection concurrency** — every client gets its own thread; all threads share one `ConcurrentHashMap`-backed store, while transaction state stays connection-local
- ✅ **TTL with lazy expiration** — `SET ... PX <ms>` support with expiry checked (and enforced) at read time, no background sweeper
- ✅ **Redis Streams, including blocking reads** — `XADD` with explicit/partial/auto-generated IDs, `XRANGE` for range queries, and a blocking `XREAD` for real-time consumption
- ✅ **Verified against a real client** — tested via actual `redis-cli` TCP sessions, including surviving `redis-cli`'s own startup handshake commands
- ✅ **CodeCrafters-guided, independently built** — uses the *Build Your Own Redis* challenge as a progression map and compatibility check, not a source of copied solutions

---

## 🏗️ High-Level Architecture

All client traffic flows straight into a listening `ServerSocket` — there's no gateway or proxy layer here, ByteCache **is** the server. Every accepted connection gets its own thread, its own `ClientConnection`, and its own `ClientContext` (for transaction state) — but every thread reads from and writes to the **same** shared `RedisStore`.

<p align="center">
  <img src="./assets/Detailed_Architecture.png" alt="Detailed Architecture Diagram" width="90%" />
  <br/>
  <i>Full layered view — Client, Server, Connection, Protocol, Command Dispatch, and Storage layers, one arrow per responsibility handoff</i>
</p>

```
                 ┌─────────────────────────────┐
                 │            Client           │
                 │      redis-cli / TCP        │
                 └──────────────┬──────────────┘
                                │  TCP byte stream
                                ▼
                 ┌─────────────────────────────┐
                 │        RedisServer          │
                 │  ServerSocket.accept()      │
                 │  spawns one thread / client │
                 └──────────────┬──────────────┘
                                │  one Socket per client
                                ▼
                 ┌─────────────────────────────┐
                 │      ClientConnection       │
                 │  InputStream / OutputStream │
                 │  owns a ClientContext       │
                 └──────────────┬──────────────┘
                                │  raw RESP bytes
                                ▼
                 ┌─────────────────────────────┐
                 │         RespParser          │
                 │  bytes → complete commands  │
                 │  handles partial reads      │
                 └──────────────┬──────────────┘
                                │  List<String> command
                                ▼
                 ┌─────────────────────────────┐
                 │     CommandDispatcher       │
                 │  Map<String, Command>       │
                 │  Generic/String/Numeric/    │
                 │  Stream/Transaction         │
                 └──────────────┬──────────────┘
                                ▼
                 ┌─────────────────────────────┐
                 │         RedisStore          │
                 │  ConcurrentHashMap          │
                 │  Strings + Streams + TTL    │
                 └─────────────────────────────┘
```

### Responsibility boundaries

| Component | Responsibility |
|---|---|
| **`Main`** | Starts the server. |
| **`RedisServer`** | Owns the listening `ServerSocket`, accepts clients, spawns a `ClientConnection` thread per client. |
| **`ClientConnection`** | Owns one TCP connection — reads bytes, feeds the parser, forwards parsed commands to the dispatcher, writes RESP responses. Knows nothing about *which* commands exist. |
| **`ClientContext`** | Connection-local state: whether the connection is inside a transaction, and its queued commands. |
| **`RespParser`** | Knows *how* RESP is structured. Doesn't know what `SET` or `XADD` mean. |
| **`CommandDispatcher`** | Knows *which* commands exist and routes a parsed command to the right `Command` implementation. Returns a RESP error for anything it doesn't recognize. |
| **`Command` implementations** | One class per command (or a small family per category), each responsible for its own semantics. |
| **`RedisStore`** | Owns the actual in-memory data, shared across every client connection. |
| **`StoredValue` / `RedisStream` / `StreamEntry` / `StreamId`** | Model the supported data types and their internal state. |

---

## 🔬 The Journey of a Command

The architecture diagram above shows *layers*. This section shows what actually happens, object by object, for one real command.

### Forward path: `SET user:42 Anuj`

```
redis-cli
   │
   │ RESP bytes
   ▼
TCP
   │
   ▼
ClientConnection
   │
   │ InputStream.read()
   ▼
byte[]
   │
   ▼
RespParser
   │
   │ ["SET", "user:42", "Anuj"]
   ▼
CommandDispatcher
   │
   ▼
SetCommand
   │
   ▼
RedisStore
   │
   ▼
ConcurrentHashMap
   │
   ▼
StoredValue
```

### Return path: the response travels back

```
StoredValue
    ↓
SetCommand
    ↓
+OK\r\n
    ↓
OutputStream
    ↓
TCP
    ↓
redis-cli
```

No step is skipped or hidden: the bytes that arrive are not the same as the object that gets parsed, which is not the same as the command that executes, which is not the same as the bytes that go back out. Making that chain explicit — rather than "the server processes the command" — is most of what this project is actually about.

---

## 🧩 Command Architecture

Early on, ByteCache handled commands with a hardcoded chain of `if/else` checks inside the connection-handling code. That doesn't scale past a handful of commands, and it also means the networking layer has to know about application semantics — a violation of the separation-of-concerns principle the whole project is built around.

Commands are now organized by category, each with its own package, and routed through a single dispatcher:

```
Command
   │
   ├── Generic
   │    ├── PING
   │    ├── ECHO
   │    └── TYPE
   │
   ├── String
   │    ├── SET
   │    └── GET
   │
   ├── Numeric
   │    └── INCR
   │
   ├── Stream
   │    ├── XADD
   │    ├── XRANGE
   │    └── XREAD
   │
   └── Transaction
        ├── MULTI
        ├── EXEC
        └── DISCARD
```

```
CommandDispatcher
        │
        └── Map<String, Command>
```

`ClientConnection` no longer needs to know that `SET` or `XADD` exist — it just hands a parsed command list to the dispatcher and writes back whatever RESP response comes out. That single change is what let `redis-cli` interoperability actually work (see [Interesting Bugs & Debugging Stories](#-interesting-bugs--debugging-stories) below).

---

## 🔁 Transactions Engine

Transactions are one of the largest pieces of the current implementation — `MULTI`, command queuing, `EXEC`, `DISCARD`, per-connection transaction state, and multiple simultaneous transactions across different clients.

### Queuing phase

```
Client A
   │
   ├── MULTI
   ├── SET foo 41
   ├── INCR foo
   │
   ▼
ClientContext A
   └── queue:
       [SET foo 41]
       [INCR foo]
```

While a connection is inside `MULTI`, commands aren't executed immediately — they're appended to that connection's queue and answered with `+QUEUED\r\n`.

### Execution phase

```
EXEC
 ↓
CommandDispatcher
 ↓
execute queued commands, in order
 ↓
capture each command's RESP response
 ↓
construct one RESP array of results
```

### Redis transactions are not rollback transactions

This is the most important thing this section teaches, and it's easy to assume otherwise if you're used to SQL transactions:

> **Command failure ≠ transaction rollback.**

```
MULTI
INCR foo      ← error (foo isn't an integer)
INCR bar      ← succeeds
EXEC
```

produces:

```
[
    (error) ERR value is not an integer or out of range
    (integer) 42
]
```

The successful `INCR bar` is **not** undone by the failed `INCR foo`. `EXEC` guarantees the queued commands run in order without another client's commands interleaving between them — it does not guarantee all-or-nothing semantics. Modeling this correctly (rather than assuming SQL-style rollback) was one of the more interesting design decisions in the project.

### Transaction state is connection-local; data is server-global

```
              RedisStore
             shared state
             /          \
            /            \
           ▼              ▼
 ClientContext A     ClientContext B
      │                    │
   Queue A              Queue B
      │                    │
   MULTI...             MULTI...
```

Two clients can each be mid-transaction at the same time, with completely independent queues, while both still read and write the same underlying `RedisStore` once their `EXEC` runs. Keeping transaction bookkeeping out of `RedisStore` and inside per-connection `ClientContext` objects is one of the cleaner boundaries in the current architecture.

---

## 🗄️ Storage Model & State Ownership

Everything server-global lives behind one deceptively simple structure:

```java
Map<String, Object> data;   // backed by ConcurrentHashMap
```

```
RedisStore
│
└── data : Map<String, Object>
     ├── "name"     → StoredValue { value: "Anuj", expiresAt: null }
     ├── "session"  → StoredValue { value: "abc123", expiresAt: <timestamp> }
     └── "events"   → RedisStream { entries: [...] }
```

The `Object` value type is deliberate: one key namespace, many Redis data types, without over-committing to a rigid schema up front.

**Strings** are wrapped in `StoredValue`, which carries both the value and an optional expiry — so a normal key and a `PX`-expiring key share the exact same storage path.

```
SET session abc123 PX 5000
        ↓
"session" → StoredValue { value = "abc123", expiresAt = now + 5000 }
```

> [!NOTE]
> Expiration is **lazy**: the timestamp is only checked when the key is next accessed, at which point an expired key is evicted on the spot. There's no background sweeper thread — the trade-off (memory isn't reclaimed until the next access) is explicit and intentional, not an oversight.

### Who owns what?

Not all state is equal — some belongs to a single connection, some is shared server-wide. Making that boundary explicit is one of the more important design decisions in ByteCache:

| State | Owner | Shared across clients? |
|---|---|---|
| TCP socket | `ClientConnection` | No |
| RESP parser buffer | `RespParser` (per connection) | No |
| Transaction queue / in-`MULTI` flag | `ClientContext` | No |
| Redis key/value data | `RedisStore` | **Yes** |
| Stream entries | `RedisStream` (inside `RedisStore`) | **Yes** |
| Server listening socket | `RedisServer` | Server-wide (singleton) |

---

## 🌊 Redis Streams Internals

Streams are modeled with a small, focused set of classes:

```
RedisStore → "events" → RedisStream → List<StreamEntry>
                                            ├── StreamEntry { StreamId, Map<String,String> fields }
                                            ├── StreamEntry
                                            └── StreamEntry
```

Example:

```
XADD events 1000-0 user Anuj action login
XADD events 1001-0 user Rahul action logout
```

```
"events" → RedisStream
              ├── [0] ID: 1000-0  { user: Anuj,  action: login  }
              └── [1] ID: 1001-0  { user: Rahul, action: logout }
```

### Stream IDs are structured, not stringly-typed

A stream ID like `1000-7` is **not** stored as a raw string — it's a comparable object:

```java
class StreamId {
    long millisecondsTime;
    long sequenceNumber;
}
```

which lets the stream enforce strictly increasing order on every insert.

### Supported `XADD` ID forms

| Form | Meaning |
|---|---|
| `1000-0` | Explicit, fully specified ID |
| `1000-*` | Explicit timestamp, auto-generated sequence number |
| `*` | Fully auto-generated timestamp + sequence number |

For entries sharing a millisecond, the sequence number preserves order:

```
1000-0 → 1000-1 → 1000-2 → 1000-3
```

Every explicit ID is validated against the current stream tail before insertion — a lower or equal ID is rejected with a RESP error.

### Reading streams: `XRANGE` and `XREAD`

`XRANGE` answers "give me the entries between these two IDs" — a straightforward scan over the ordered `List<StreamEntry>` with inclusive bounds.

`XREAD` is more interesting when used in **blocking** mode: a client can ask to wait until a new entry is appended rather than polling. Conceptually:

```
XREAD BLOCK <ms> STREAMS events $
        │
        ▼
   check: any entries after last-seen ID?
        │
   ┌────┴────┐
   │         │
  yes        no
   │         │
   ▼         ▼
 return    wait (up to timeout) for a new XADD, then re-check
```

That "check, then wait, then re-check" shape is a classic source of race conditions if the underlying state can change between the check and the wait — see [Known Concurrency Limitations](#️-known-concurrency-limitations).

---

## 📡 RESP Protocol & Wire Examples

ByteCache speaks the same wire format `redis-cli` expects. Seeing the actual bytes makes it obvious this is protocol work, not just "calling Redis commands."

**`PING`** — client to server:

```
*1\r\n
$4\r\n
PING\r\n
```

Response:

```
+PONG\r\n
```

**`SET name Anuj`**:

```
*3\r\n
$3\r\n
SET\r\n
$4\r\n
name\r\n
$4\r\n
Anuj\r\n
```

Response:

```
+OK\r\n
```

**`GET name`** — response:

```
$4\r\n
Anuj\r\n
```

**`GET` on a missing or expired key**:

```
$-1\r\n
```

**`INCR foo`** — response (RESP integer):

```
:42\r\n
```

**`EXEC`** returning two queued results (`OK`, then `42`):

```
*2\r\n
+OK\r\n
:42\r\n
```

### Why the parser buffers

TCP hands you a **byte stream**, not a stream of commands. A single command can arrive split across reads:

```
read #1 → "*1\r\n$4"
read #2 → "\r\nPING\r\n"
```

...or several commands can arrive bundled together in one read:

```
read #1 → PING + SET + GET, all in one buffer
```

`RespParser` buffers until a full command is available before handing it off:

```
TCP
  ↓
byte stream
  ↓
RESP message framing
  ↓
RespParser
  ↓
complete commands, one at a time
```

This is arguably the single most important lesson the project teaches:

> **One `read()` is not one request.**

---

## 🧵 Concurrency Model

ByteCache uses a **thread-per-connection** model:

```
Main server thread
       ├── accept Client A → Thread A
       ├── accept Client B → Thread B
       └── accept Client C → Thread C
```

Each thread owns its own socket and its own `ClientContext` — but every thread shares the **same** `RedisStore`:

```
             RedisStore (ConcurrentHashMap)
                  /        |        \
             Thread A   Thread B   Thread C
                 │           │          │
           ClientContext ClientContext ClientContext
             (private)    (private)    (private)
```

It's a deliberately simple model: connections and threads have a 1:1 relationship, transaction state is explicitly private per connection, and shared storage is made safe via `ConcurrentHashMap`. That gives a clean baseline to compare against thread-pool or event-loop designs later (see [Roadmap](#️-roadmap)).

---

## ⚠️ Known Concurrency Limitations

`ConcurrentHashMap` makes individual `get`/`put` operations safe — it does **not** automatically make multi-step (*compound*) operations atomic. Being explicit about that distinction matters more than claiming everything is thread-safe.

A few concrete examples in the current implementation:

- **`INCR` is check-then-act.** Conceptually it's `GET → parse → +1 → SET`. Two threads incrementing the same key at nearly the same instant could interleave those steps.
- **Streams are `ArrayList`-backed.** Appending to a stream while another thread is reading a range from it has synchronization considerations beyond what a single `ConcurrentHashMap.get()` provides.
- **Blocking `XREAD` has a classic check-then-wait shape.** If the stream changes in the gap between "check for new entries" and "start waiting," there's a possible missed-wakeup race.

> The current implementation intentionally uses a simple concurrency model. Some compound operations are not yet fully atomic across client threads, and stream-level synchronization is an area flagged for future hardening rather than something already solved.

Documenting this honestly is more convincing than claiming full thread-safety and being wrong about it.

---

## 🔌 Supported Commands

| Command | Category | Status |
|---|---|:---:|
| `PING` | Generic | ✅ |
| `ECHO` | Generic | ✅ |
| `TYPE` | Generic | ✅ |
| `SET` (incl. `PX`) | String | ✅ |
| `GET` | String | ✅ |
| `INCR` | Numeric | ✅ |
| `XADD` | Stream | ✅ |
| `XRANGE` | Stream | ✅ |
| `XREAD` (incl. blocking) | Stream | ✅ |
| `MULTI` | Transaction | ✅ |
| `EXEC` | Transaction | ✅ |
| `DISCARD` | Transaction | ✅ |

Every command not in this table is met with a RESP error rather than being silently ignored or crashing the connection — see [Failure Semantics](#-failure-semantics).

---

## ❌ Failure Semantics

Successful paths are only half the story. ByteCache distinguishes several categories of failure, each with its own RESP-level behavior:

| Failure | Example | Behavior |
|---|---|---|
| Unknown command | `FOOBAR` | RESP error from `CommandDispatcher` |
| Command-level error | `INCR abc` where `abc` isn't an integer | RESP error, connection stays open |
| Transaction-state error | `EXEC` without a preceding `MULTI` | RESP error |
| Wrong data type | A string operation on a stream key (or vice versa) | RESP error |
| Stream ID violation | Inserting a decreasing/duplicate stream ID | RESP error, entry rejected |
| Malformed RESP | A syntactically invalid frame | Parser-level RESP error |

---

## 🔍 Engineering Deep Dives

**1. TCP is just a byte stream.** `ServerSocket`, `Socket`, `InputStream`, `OutputStream` are used directly — TCP has no idea what `SET` or `XADD` mean, it only moves bytes. RESP is what gives those bytes application meaning.

**2. Parsing is separate from execution.** `RespParser` only turns bytes into `["SET", "name", "Anuj"]`. It never decides what `SET` *does* — that decoupling is what let the command-dispatcher architecture replace a hardcoded chain without touching the networking layer at all.

**3. Connections are persistent.** A connection isn't torn down after one command — an entire `MULTI` ... `EXEC` sequence, interleaved with `PING`s and `GET`s from other clients, can all ride the same long-lived TCP connection.

**4. Expiration is lazy, on purpose.** No background thread scans for expired keys. `GET` checks `expiresAt` at read time and evicts on the spot — smaller and more explicit, at the cost of not reclaiming memory until the next access.

**5. Stream ordering is structural, not textual.** IDs compare as `(millisecondsTime, sequenceNumber)` tuples, so `1000-2 < 1000-3 < 1001-0` is enforced by the type system, not string comparison tricks.

**6. Transactions queue commands, they don't queue rollback plans.** `EXEC` runs each queued command and collects its result — there is no undo log, because that's how real Redis transactions behave too (see [Transactions Engine](#-transactions-engine)).

---

## 🐛 Interesting Bugs & Debugging Stories

**"`redis-cli` appeared stuck on connect."**

Root cause, traced at the wire level rather than in application code:

```
redis-cli
 ↓
sends COMMAND DOCS / INFO SERVER on startup
 ↓
ClientConnection's fixed command whitelist
 ↓
command silently ignored
 ↓
redis-cli sits waiting for a response that never comes
```

A real Redis client issues housekeeping commands beyond whatever subset a hobby server implements — the networking layer can't assume a fixed whitelist of "known-good" commands. The fix was architectural, not a patch: move command awareness entirely out of `ClientConnection` and into `CommandDispatcher`, which now returns a proper RESP error for anything it doesn't recognize instead of the connection silently dropping the command:

```
ClientConnection
       ↓
RespParser
       ↓
CommandDispatcher
       ↓
unknown command → -ERR (not silence)
```

That single fix is also what's described in [Command Architecture](#-command-architecture) above — the bug is what motivated the refactor.

**Cross-OS TCP testing.** During development the Java server runs on Windows while `redis-cli` runs from WSL:

```
Windows Java server
        ↑
        │ TCP
        ↓
WSL redis-cli
```

`localhost` doesn't always resolve the way you'd expect across that boundary — the WSL-to-Windows gateway address is sometimes needed instead, depending on local network configuration. Minor in isolation, but a good reminder that "it works on my machine" has layers even *within* one machine.

---

## 🧭 Design Decisions & Trade-offs

### Why thread-per-connection?

**Chosen because:**
- Simple mental model — one thread, one client, one story to follow in a debugger
- Makes blocking I/O behavior visible instead of hidden behind a reactor
- Easy to reason about correctness during early development

**Trade-off:**
- Doesn't scale as efficiently as an event-loop or thread-pool architecture for very large connection counts — each thread carries real OS overhead. Comparing this against alternatives is explicit future work (see [Roadmap](#️-roadmap)).

### Why lazy TTL expiration?

**Pros:**
- Simple to implement and reason about
- No background sweeper thread, no extra scheduling complexity
- Fewer moving parts to get wrong

**Cons:**
- Expired-but-unaccessed keys stay in memory indefinitely — there's no proactive reclamation

### Why `ConcurrentHashMap`?

```
Multiple client threads
        ↓
shared RedisStore
        ↓
ConcurrentHashMap
```

It guarantees individual `get`/`put` calls are safe under concurrent access from multiple client threads. It does **not** guarantee that multi-step logic built on top of those calls (like `INCR`) is atomic — see [Known Concurrency Limitations](#️-known-concurrency-limitations) for exactly where that matters today.

---

## 📂 Project Structure

```
ByteCache/
├── src/main/java/
│   ├── Main.java
│   ├── server/
│   │   └── RedisServer.java
│   ├── connection/
│   │   ├── ClientConnection.java
│   │   └── ClientContext.java
│   ├── protocol/
│   │   └── RespParser.java
│   ├── command/
│   │   ├── Command.java
│   │   ├── CommandDispatcher.java
│   │   ├── generic/
│   │   ├── string/
│   │   ├── numeric/
│   │   ├── stream/
│   │   └── transaction/
│   └── storage/
│       ├── RedisStore.java
│       ├── StoredValue.java
│       ├── RedisStream.java
│       ├── StreamEntry.java
│       └── StreamId.java
├── pom.xml
└── README.md
```

The structure deliberately avoids both failure modes: a single 1000-line `Main.java`, and forty interfaces before the server can handle a second command. New classes appear when a real responsibility shows up — the `command/` package and `ClientContext` are both examples of structure that arrived *after* a concrete need (a growing command list, and transaction state) rather than upfront.

```
Server lifecycle → Connection lifecycle → Protocol parsing → Command dispatch → Data storage
```

---

## ⚙️ How to Run

### 1. Requirements

| Requirement | Notes |
|---|---|
| JDK 25+ | Or a compatible Java version |
| Maven | Used for build lifecycle |
| `redis-cli` | Optional, but recommended for interoperability testing |
| OS | Windows / Linux / macOS with a TCP-capable environment |

### 2. Build

From the project root:

```bash
mvn clean compile
```

### 3. Run

Start the server via `Main`. It listens on the default Redis port:

```
6379
```

### 4. Explore with `redis-cli`

```bash
redis-cli -p 6379
```

```text
PING
SET name Anuj
GET name
TYPE name
SET session abc PX 5000
GET session
INCR counter
```

Stream commands:

```text
XADD events 1000-0 user Anuj
XADD events 1001-0 user Rahul
XADD events 1002-* user Anuj
XADD events * user Anuj
XRANGE events 1000-0 1001-0
XREAD BLOCK 5000 STREAMS events $
TYPE events
```

Transaction commands:

```text
MULTI
SET foo 41
INCR foo
EXEC
```

> [!NOTE]
> If the Java server runs on Windows and `redis-cli` runs from WSL, you may need the WSL-to-Windows gateway address instead of `localhost`, depending on your network configuration.

---

## 🧪 Testing Strategy

Beyond "I ran some commands," ByteCache is exercised against a deliberate set of behavioral scenarios, grouped by subsystem:

**Networking**
- Partial RESP command split across reads
- Multiple commands arriving in a single read
- Persistent connection carrying many commands
- Client disconnect mid-session

**Strings**
- `GET` on a missing key
- `SET` then `GET`
- `PX` expiration
- Access after expiration (lazy eviction)

**Streams**
- Explicit stream ID
- Auto-generated stream ID (`*` and `ms-*`)
- Multiple entries sharing one millisecond
- Invalid / decreasing stream ID
- `XRANGE` over a bounded window
- `XREAD` (non-blocking)
- `XREAD BLOCK` (blocking, with and without a timeout)

**Transactions**
- `MULTI` then `EXEC`
- `MULTI` then `DISCARD`
- `EXEC` without a preceding `MULTI`
- `DISCARD` without a preceding `MULTI`
- A failing command inside an otherwise-successful `EXEC`
- Multiple simultaneous transactions on different connections

Validation itself happens at several levels — direct compilation of the full source tree, real TCP interaction (not just direct Java method calls), actual `redis-cli` sessions for wire-level compatibility, and the CodeCrafters **Build Your Own Redis** checks as an external compatibility signal rather than a source of copied solutions.

---

## 💻 Example Sessions

Real terminal transcripts speak louder than a paragraph claiming "transactions work."

**A successful transaction:**

```
$ redis-cli -p 6379

> MULTI
OK

> SET foo 6
QUEUED

> INCR foo
QUEUED

> EXEC
1. OK
2. (integer) 7

> GET foo
"7"
```

**A transaction with a failing command inside it:**

```
> MULTI
OK

> INCR foo
QUEUED

> INCR bar
QUEUED

> EXEC
1. (error) ERR value is not an integer or out of range
2. (integer) 42
```

Note that the second command still ran and returned a real result — the first command's failure didn't roll anything back.

---

## ⚔️ ByteCache vs Redis

Not a competitive comparison — a way of showing that ByteCache is *intentionally* implementing selected mechanisms to understand them, not trying to be a smaller Redis.

| Area | ByteCache | Redis |
|---|---|---|
| Purpose | Educational implementation | Production datastore |
| Transport | TCP | TCP |
| Protocol | RESP2 subset | Full Redis protocol ecosystem |
| Storage | In-memory Java objects | Highly optimized native implementation |
| Concurrency | Thread-per-connection | Event-driven single-threaded core + background threads |
| Persistence | Planned | Mature (RDB/AOF) |
| Data types | Strings, Streams | Broad set (lists, sets, hashes, sorted sets, streams, ...) |
| Streams | Append, range, blocking read | Full Streams feature set (consumer groups, etc.) |
| Transactions | `MULTI`/`EXEC`/`DISCARD`, no rollback | Mature transaction semantics, no rollback (same philosophy) |
| Scale | Learning project | Production system |

---

## 🕰️ Architecture Evolution

The current architecture didn't appear fully formed — it grew in response to real requirements as they showed up, which is a large part of the point of the project:

```
Stage 1 — TCP Server
   ↓
ClientConnection with hardcoded command handling

Stage 2 — Protocol Separation
   ↓
TCP → RespParser → CommandDispatcher → Command implementations → RedisStore

Stage 3 — Command Categories
   ↓
Command
   ├── generic
   ├── string
   ├── numeric
   ├── stream
   └── transaction

Stage 4 — Connection-Local State
   ↓
Connection
 ├── ClientContext (transaction state)
 └── shared RedisStore (server-wide data)
```

Each stage was a response to a concrete limitation of the previous one — Stage 2 happened because `redis-cli` needed graceful handling of unknown commands (see [Interesting Bugs & Debugging Stories](#-interesting-bugs--debugging-stories)); Stage 4 happened because transactions needed somewhere to keep per-connection queue state without polluting the shared store.

---

## 🧗 Learning Milestones

```
01 — TCP server
     ↓
02 — RESP parsing
     ↓
03 — Persistent connections
     ↓
04 — In-memory storage
     ↓
05 — TTL / lazy expiration
     ↓
06 — Streams (XADD)
     ↓
07 — Command-dispatcher architecture
     ↓
08 — Stream range & blocking reads (XRANGE, XREAD)
     ↓
09 — Transactions (MULTI/EXEC/DISCARD)
     ↓
10 — Concurrent transactions across clients
     ↓
11 — Persistence (planned)
     ↓
12 — Performance benchmarking (planned)
```

---

## 🚫 Known Limitations

Being transparent here makes the README more trustworthy, not weaker:

- In-memory only — all data disappears on restart
- Implements a subset of RESP2, not the full protocol
- Implements a subset of Redis commands
- Thread-per-connection concurrency model (not yet compared against alternatives)
- No authentication
- No persistence yet (no RDB/AOF equivalent)
- No replication or clustering
- No production-grade memory management
- Some compound operations (`INCR`, stream reads/writes, blocking `XREAD`) are not yet fully atomic under concurrent access — see [Known Concurrency Limitations](#️-known-concurrency-limitations)
- Limited protocol-level validation for malformed input
- No performance benchmarks yet

---

## 🔮 Future Vision — Planned Architecture

The current server — a real command-dispatcher architecture, streams with blocking reads, and working transactions — is the **foundation**. The direction below is where ByteCache is headed once the items in the [Roadmap](#️-roadmap) land.

<p align="center">
  <img src="./assets/Planned_Architecture.png" alt="Planned Future Architecture" width="90%" />
  <br/>
  <i>The envisioned system — an event-loop or thread-pool front end, stronger compound-operation atomicity, a durability layer for recovery after restart, and a benchmarking harness for throughput/latency</i>
</p>

### Where each piece is headed

| Area | Today | Planned |
|---|---|---|
| **Concurrency** | Thread-per-connection | Compare against thread-pool and event-loop (e.g. NIO selector-based) models under real load |
| **Compound-operation safety** | `INCR` / stream ops are check-then-act | Explicit synchronization or CAS-based atomicity |
| **Command surface** | 12 commands across 5 categories | Broader RESP value types, more Redis data types |
| **Durability** | Pure in-memory, nothing survives restart | Write-ahead logging / append-only file, crash recovery, fsync trade-offs |
| **Observability** | None yet | Structured logging; commands/sec, latency, active connections, error rate |
| **Performance** | Undocumented | Benchmark thread-per-connection vs thread-pool vs NIO across 100 / 1,000 / 10,000+ clients, tracking throughput and p95/p99 latency |
| **Testing** | Manual + `redis-cli` sessions | Protocol-level test client, automated integration suite |

> [!NOTE]
> These are directions, not commitments with dates — see the [Roadmap](#️-roadmap) table below for the actual state of each item today.

---

## 🗺️ Roadmap

| Feature | Status |
|---|---|
| Raw TCP server (`ServerSocket` / `Socket`) | ✅ Completed |
| Persistent, multi-client connections | ✅ Completed |
| Thread-per-connection concurrency | ✅ Completed |
| RESP request parsing (partial reads + pipelining) | ✅ Completed |
| RESP simple strings / bulk strings / integers / errors | ✅ Completed |
| Command-dispatcher architecture (Generic/String/Numeric/Stream/Transaction) | ✅ Completed |
| `PING`, `ECHO`, `TYPE`, `SET` (+`PX`), `GET`, `INCR` | ✅ Completed |
| Redis Streams — `XADD` (explicit/partial/auto IDs), monotonic validation | ✅ Completed |
| Stream range queries — `XRANGE` | ✅ Completed |
| Stream reads — `XREAD`, including blocking mode | ✅ Completed |
| Transactions — `MULTI`, `EXEC`, `DISCARD`, per-connection state | ✅ Completed |
| Concurrent transactions across multiple clients | ✅ Completed |
| `redis-cli` interoperability (unknown-command handling) | ✅ Completed |
| Atomicity for compound operations (`INCR`, stream ops) | ❌ Not Started |
| Thread pool / event-loop architecture comparison | ❌ Not Started |
| Throughput & latency benchmarking | ❌ Not Started |
| Persistence & recovery semantics | ❌ Not Started |
| Structured logging & metrics | ❌ Not Started |
| Protocol-level automated test client / integration suite | ❌ Not Started |

---

## 🏆 Resume-Worthy Engineering

- Built a Redis-compatible TCP server in Java using raw `ServerSocket`/`Socket` APIs and implemented incremental RESP2 parsing that correctly handles partial reads and pipelined commands.
- Designed a persistent, multi-client architecture using thread-per-connection execution over a shared, concurrency-safe in-memory store.
- Refactored a hardcoded command chain into a category-based command-dispatcher architecture (`Generic`/`String`/`Numeric`/`Stream`/`Transaction`), diagnosed and fixed via a real `redis-cli` interoperability bug.
- Implemented Redis Streams with structured stream IDs, monotonic ordering validation, automatic ID generation, range queries (`XRANGE`), and blocking reads (`XREAD`).
- Implemented Redis-style transactions with per-connection command queues, `EXEC` response aggregation, `DISCARD`, and correctly modeled non-rollback failure semantics under concurrent transactions.
- Identified and documented specific concurrency limitations (compound-operation atomicity, blocking-read race conditions) rather than overclaiming thread-safety.

---

## 📚 What I'm Learning

```
Networking → TCP byte streams → Application protocols → RESP framing/parsing
   → Command dispatch → In-memory data structures → TTL / data lifetime
   → Concurrency and shared state → Streams and ordered identifiers
   → Blocking operations → Transactions and connection-local state
   → Performance / architecture → Persistence and durability
```

Building each layer directly gives hands-on experience with: TCP socket programming in Java, blocking I/O, connection lifecycle management, byte streams and message framing, protocol parsing, RESP serialization, persistent connections, multi-client servers, thread-per-connection concurrency, concurrent shared state, key-value storage, TTL and lazy expiration, Redis-style data types, ordered stream IDs, automatic identifier generation, blocking reads, transaction queuing and non-rollback failure semantics, error handling, API/data-model boundaries, incremental architecture design, and testing at the network boundary.

---

## 🤝 Contribution

This project is under active development. Feel free to open an issue or submit a pull request with improvements, fixes, or ideas for new commands and data types.

---

## ⚠️ Disclaimer

ByteCache is an **educational, Redis-inspired implementation**. It is not Redis, doesn't attempt to reproduce Redis's full internal architecture, and is not intended for production use.

Its purpose is to build first-hand understanding of networking, protocols, concurrency, transactions, in-memory storage, and backend systems — by implementing the mechanisms, not just consuming them.

---

## 📜 License

License TBD before public release.

<p align="center">
  Built with ☕ Java and a genuine curiosity about what's underneath Redis.
</p>