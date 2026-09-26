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
  <img src="https://img.shields.io/badge/status-active%20development-yellow?style=for-the-badge" />
</p>

> **ByteCache** is a Redis-inspired, in-memory data store built entirely from **raw Java sockets** — no Redis binary underneath, no Spring Boot, no networking library doing the hard part for you. It speaks real **RESP2** well enough that an unmodified `redis-cli` can talk to it, handles **persistent, concurrent client connections** with a hand-rolled thread-per-connection model, and implements **Redis Streams** — ordered entries, structured IDs, monotonic validation — from first principles.

> [!NOTE]
> This is a systems-engineering showcase, not a Redis replacement. The interesting part isn't the feature list — it's that every layer between "a client typed `SET`" and "a value sits in memory" was written and understood by hand: socket lifecycle, byte-stream framing, protocol parsing, command dispatch, concurrent storage.

> [!WARNING]
> ByteCache is actively evolving. New commands, data types, and a persistence layer are in progress — see the [Roadmap](#-roadmap) below for what's built today versus what's planned.

---

## ⭐ Support

If this project helped you understand what's really happening underneath Redis — or underneath any client-server system — consider giving it a star. It genuinely helps.

---

## 📖 Table of Contents

- [🚀 Why This Project Exists](#-why-this-project-exists)
- [✨ Key Features](#-key-features)
- [🏗️ High-Level Architecture](#️-high-level-architecture)
- [🔁 Request Lifecycle](#-request-lifecycle)
- [🗄️ Storage Model](#️-storage-model)
- [🌊 Redis Streams Internals](#-redis-streams-internals)
- [📡 RESP Protocol](#-resp-protocol)
- [🧵 Concurrency Model](#-concurrency-model)
- [🔌 Supported Commands](#-supported-commands)
- [🔍 Engineering Deep Dives](#-engineering-deep-dives)
- [📂 Project Structure](#-project-structure)
- [⚙️ How to Run](#️-how-to-run)
- [🧪 Testing & Verification](#-testing--verification)
- [🧭 Design Principles](#-design-principles)
- [🔮 Future Vision — Planned Architecture](#-future-vision--planned-architecture)
- [🗺️ Roadmap](#️-roadmap)
- [🏆 Engineering Highlights](#-engineering-highlights)
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
TCP connection → raw bytes → RESP parsing → commands → execution → in-memory data structures
```

Building it means confronting the questions a framework usually answers *for* you:

- What does a Redis command actually look like on the wire?
- Why can't a single `Socket.read()` call be treated as a single command?
- How does one server handle hundreds of persistent client connections at once?
- Where does a key's TTL actually **live**, and when is it checked?
- How are Redis Stream IDs ordered, validated, and auto-generated?
- What breaks when two threads touch the same in-memory map at the same time?

There's no Spring Boot here on purpose — networking and protocol handling are the point, not implementation details hidden behind an annotation.

<p align="center">
  <img src="./assets/Overall_Concept.png" alt="Concept Diagram — Client to Server to Store" width="85%" />
  <br/>
  <i>Client → TCP socket → RESP parser → command dispatch → in-memory store, with every arrow hand-implemented</i>
</p>

---

## ✨ Key Features

- ✅ **Zero frameworks** — pure `java.net`, no Netty, no Spring, no Redis client library underneath
- ✅ **Real wire compatibility** — speaks RESP2 well enough for an unmodified `redis-cli` session to work against it
- ✅ **Hand-rolled protocol parsing** — incremental buffering across partial reads, correctly splits pipelined commands arriving in a single read
- ✅ **Thread-per-connection concurrency** — every client gets its own thread; all threads share one `ConcurrentHashMap`-backed store
- ✅ **TTL with lazy expiration** — `SET ... PX <ms>` support with expiry checked (and enforced) at read time, no background sweeper
- ✅ **Redis Streams from scratch** — `XADD` with explicit IDs, partially generated IDs (`ms-*`), and fully generated IDs (`*`), with monotonic ordering enforced on every insert
- ✅ **Polymorphic in-memory storage** — one `Map<String, Object>` cleanly hosting multiple Redis data types (`StoredValue`, `RedisStream`) behind a single key namespace
- ✅ **Verified against a real client** — tested via actual `redis-cli` TCP sessions, not just unit tests against Java method calls
- ✅ **CodeCrafters-guided, independently built** — uses the *Build Your Own Redis* challenge as a progression map and compatibility check, not a source of copied solutions

---

## 🏗️ High-Level Architecture

All client traffic flows straight into a listening `ServerSocket` — there's no gateway or proxy layer here, ByteCache **is** the server. Every accepted connection gets its own thread and its own `ClientConnection`, but every thread reads from and writes to the **same** shared store.

<p align="center">
  <img src="./assets/Detailed_Architecture.png" alt="Detailed Architecture Diagram" width="90%" />
  <br/>
  <i>Full layered view — Client, Server, Connection, Protocol, Command, and Storage layers, one arrow per responsibility handoff</i>
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
                 │  owns connection lifecycle  │
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
                 │      Command Handling       │
                 │  PING / ECHO / SET / GET    │
                 │  TYPE / XADD                │
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
| **`ClientConnection`** | Owns one TCP connection — reads bytes, feeds the parser, executes commands, writes RESP responses. |
| **`RespParser`** | Knows *how* RESP is structured. Doesn't know what `SET` or `XADD` mean. |
| **`RedisStore`** | Owns the actual in-memory data, shared across every client connection. |
| **`StoredValue` / `RedisStream` / `StreamEntry` / `StreamId`** | Model the supported data types and their internal state. |

---

## 🔁 Request Lifecycle

Trace a single command end to end:

```
SET name Anuj
```

```
redis-cli
   │  RESP bytes
   ▼
TCP Socket
   ▼
ClientConnection  ──(InputStream.read())──▶  RespParser
                                                  │  ["SET", "name", "Anuj"]
                                                  ▼
                                          Command handling
                                                  │
                                                  ▼
                                            RedisStore
                                                  │  data.put("name", StoredValue(...))
                                                  ▼
                                          In-memory state
                                                  │
                                                  ▼
                                          RESP response (+OK\r\n)
                                                  │
                                                  ▼
                                            TCP Socket ──▶ redis-cli
```

A stream command follows the same shape, but the payload gets richer at each stage:

```
XADD events * user Anuj action login
   ↓
["XADD", "events", "*", "user", "Anuj", "action", "login"]
   ↓
handleXAdd()  →  RedisStore  →  RedisStream  →  StreamEntry(StreamId, fields)
   ↓
RESP bulk-string ID response
```

---

## 🗄️ Storage Model

Everything lives behind one deceptively simple structure:

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

---

## 📡 RESP Protocol

ByteCache speaks the same wire format `redis-cli` expects. `PING` arrives roughly as:

```
*1\r\n$4\r\nPING\r\n
```

and gets a simple-string reply:

```
+PONG\r\n
```

A bulk string like `Anuj` is framed as:

```
$4\r\n
Anuj\r\n
```

### Why the parser buffers

TCP hands you a **byte stream**, not a stream of commands. A single command can be split across reads:

```
read #1 → "*1\r\n$4"
read #2 → "\r\nPING\r\n"
```

...or several commands can arrive bundled in one:

```
read #1 → command A + command B + command C
```

`RespParser` buffers until a full command is available before handing it off. This is arguably the single most important lesson the project teaches:

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

Each thread owns its own socket — but every thread shares the **same** `RedisStore`:

```
             RedisStore (ConcurrentHashMap)
                  /        |        \
             Thread A   Thread B   Thread C
```

It's a deliberately simple model: connections and threads have a 1:1 relationship, while storage is explicitly shared and made safe via `ConcurrentHashMap`. That gives a clean baseline to compare against thread-pool or event-loop designs later (see [Roadmap](#-roadmap)).

---

## 🔌 Supported Commands

| Command | Example | Behavior |
|---|---|---|
| `PING` | `PING` | Returns `+PONG` — basic connectivity/protocol check |
| `ECHO` | `ECHO hello` | Returns `hello` as a RESP bulk string |
| `SET` | `SET name Anuj` | Stores a string, returns `+OK` |
| `SET ... PX` | `SET session abc123 PX 5000` | Stores a string with a millisecond TTL, checked lazily |
| `GET` | `GET name` | Returns the value as a bulk string, or `$-1` if missing/expired |
| `TYPE` | `TYPE name` / `TYPE events` / `TYPE missing` | Returns `string`, `stream`, or `none` |
| `XADD` | `XADD events 1000-0 user Anuj` | Appends a stream entry, returns the (possibly generated) ID |

`XADD` supports all three ID forms described in [Redis Streams Internals](#-redis-streams-internals) — explicit, partially generated, and fully generated.

---

## 🔍 Engineering Deep Dives

**1. TCP is just a byte stream.** `ServerSocket`, `Socket`, `InputStream`, `OutputStream` are used directly — TCP has no idea what `SET` or `XADD` mean, it only moves bytes. RESP is what gives those bytes application meaning.

**2. Parsing is separate from execution.** `RespParser` only turns bytes into `["SET", "name", "Anuj"]`. It never decides what `SET` *does* — that decoupling keeps protocol parsing from becoming entangled with application logic.

**3. Connections are persistent.** A connection isn't torn down after one command — `PING`, `SET`, `GET`, `TYPE`, and `XADD` can all ride the same TCP connection, exactly like real Redis usage.

**4. Expiration is lazy, on purpose.** No background thread scans for expired keys. `GET` checks `expiresAt` at read time and evicts on the spot — smaller and more explicit, at the cost of not reclaiming memory until the next access.

**5. Stream ordering is structural, not textual.** IDs compare as `(millisecondsTime, sequenceNumber)` tuples, so `1000-2 < 1000-3 < 1001-0` is enforced by the type system, not string comparison tricks.

---

## 📂 Project Structure

```
ByteCache/
├── src/main/java/
│   ├── Main.java
│   ├── server/
│   │   └── RedisServer.java
│   ├── connection/
│   │   └── ClientConnection.java
│   ├── protocol/
│   │   └── RespParser.java
│   └── storage/
│       ├── RedisStore.java
│       ├── StoredValue.java
│       ├── RedisStream.java
│       ├── StreamEntry.java
│       └── StreamId.java
├── pom.xml
└── README.md
```

The structure deliberately avoids both failure modes: a single 1000-line `Main.java`, and forty interfaces before the server can handle a second command. New classes appear when a real responsibility shows up — not before.

```
Server lifecycle → Connection lifecycle → Protocol parsing → Command behavior → Data storage
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
```

Stream commands:

```text
XADD events 1000-0 user Anuj
XADD events 1001-0 user Rahul
XADD events 1002-* user Anuj
XADD events * user Anuj
TYPE events
```

> [!NOTE]
> If the Java server runs on Windows and `redis-cli` runs from WSL, you may need the WSL-to-Windows gateway address instead of `localhost`, depending on your network configuration.

---

## 🧪 Testing & Verification

ByteCache is built incrementally using the CodeCrafters **Build Your Own Redis** challenge as a progression guide — not a source of copied solutions. Verification happens at several levels:

1. **Direct compilation** — every architectural change is compiled from the full source tree.
2. **Real TCP interaction** — the server is exercised through an actual TCP client, not just direct Java method calls.
3. **`redis-cli` compatibility** — using a real Redis client validates wire-level correctness, not just internal logic.
4. **Boundary experiments** — deliberate attention to partial TCP reads, multiple commands arriving together, persistent connections, missing/expired keys, invalid or non-increasing stream IDs, generated IDs, concurrent clients, and wrong-type operations.
5. **CodeCrafters checks** — used as an external compatibility signal and roadmap, while the primary goal stays independent understanding and implementation.

---

## 🧭 Design Principles

| Principle | What it means in practice |
|---|---|
| **Understand before abstracting** | New classes appear when a real responsibility exists, not ahead of time. |
| **No black boxes** | `Socket`, `InputStream`, `ConcurrentHashMap` are studied, not treated as magic. |
| **Protocol before convenience** | The project works at the level of wire bytes and RESP framing, not a high-level client API. |
| **Separate concerns** | Networking doesn't know about stream IDs; the parser doesn't know what `SET` means; storage doesn't know about TCP. |
| **Shared state must be intentional** | Every connection has its own thread, but the store is shared — and that relationship is explicit in the design. |
| **Verify behavior, not just compilation** | A server can compile perfectly and still fail at the network boundary, so real clients and real TCP interactions are part of development. |
| **Learn from simplifications** | Every place ByteCache is smaller than Redis is a prompt to ask: *what does real Redis do here, and why?* |

---

## 🔮 Future Vision — Planned Architecture

The current server — six commands, two data types, thread-per-connection — is the **foundation**. The direction below is where ByteCache is headed once the concurrency, persistence, and protocol work in the [Roadmap](#-roadmap) lands.

<p align="center">
  <img src="./assets/Planned_Architecture.png" alt="Planned Future Architecture" width="90%" />
  <br/>
  <i>The envisioned system — an event-loop or thread-pool front end, a broader command/data-type surface, a durability layer for recovery after restart, and a benchmarking harness for throughput/latency</i>
</p>

### Where each piece is headed

| Area | Today | Planned |
|---|---|---|
| **Concurrency** | Thread-per-connection | Compare against thread-pool and event-loop (e.g. NIO selector-based) models under real load |
| **Command surface** | 6 commands, 2 data types | Broader RESP value types, more Redis data types, stronger validation/error semantics |
| **Streams** | Append + ordering validation | Read/query operations (`XRANGE`, `XREAD`-style), richer stream semantics |
| **Durability** | Pure in-memory, nothing survives restart | Investigate persistence strategies and recovery semantics |
| **Observability** | None yet | Structured logging, throughput/latency benchmarks |
| **Testing** | Manual + `redis-cli` sessions | Protocol-level test client, integration suite, broader unit coverage |

> [!NOTE]
> These are directions, not commitments with dates — see the [Roadmap](#-roadmap) table below for the actual state of each item today.

---

## 🗺️ Roadmap

| Feature | Status |
|---|---|
| Raw TCP server (`ServerSocket` / `Socket`) | ✅ Completed |
| Persistent, multi-client connections | ✅ Completed |
| Thread-per-connection concurrency | ✅ Completed |
| RESP request parsing (partial reads + pipelining) | ✅ Completed |
| RESP simple strings / bulk strings / errors | ✅ Completed |
| `PING`, `ECHO`, `SET`, `GET`, `TYPE` | ✅ Completed |
| `SET ... PX` + lazy expiration | ✅ Completed |
| Redis Streams (`XADD`, explicit/partial/auto IDs) | ✅ Completed |
| Monotonic stream ID validation | ✅ Completed |
| Shared `ConcurrentHashMap` storage | ✅ Completed |
| Separate command dispatch from connection management | ✅ Completed |
| Broader command coverage | ❌ Not Started |
| Stream read/query operations | ❌ Not Started |
| Additional Redis data types | ❌ Not Started |
| Thread pool / event-loop architecture | ❌ Not Started |
| Throughput & latency benchmarking | ❌ Not Started |
| Persistence & recovery semantics | ❌ Not Started |
| Structured logging | ❌ Not Started |
| Protocol-level test client / integration suite | ❌ Not Started |

---

## 🏆 Engineering Highlights

Concepts this project demonstrates hands-on, that frameworks usually hide:

- **Networking from first principles** — a TCP server built directly on `ServerSocket` / `Socket`.
- **Protocol implementation** — incremental RESP request parsing and response serialization, with no Redis client/server underneath.
- **Concurrent server architecture** — persistent connections via thread-per-connection, backed by a shared thread-safe store.
- **In-memory data modeling** — a polymorphic `Map<String, Object>` storage layer supporting multiple data types, with type-specific behavior kept in dedicated classes.
- **Expiration semantics** — TTL metadata and lazy expiration via absolute expiry timestamps.
- **Redis Streams** — ordered entries, structured IDs, explicit/timestamp-based/fully-automatic ID generation.
- **Systems-level debugging** — using real TCP traffic and `redis-cli` to investigate byte-level behavior, persistent connections, partial reads, and protocol framing.

---

## 📚 What I'm Learning

```
Networking → TCP byte streams → Application protocols → RESP framing/parsing
   → Command execution → In-memory data structures → TTL / data lifetime
   → Concurrency and shared state → Streams and ordered identifiers
   → Performance / architecture → Persistence and durability
```

Building each layer directly gives hands-on experience with: TCP socket programming in Java, blocking I/O, connection lifecycle management, byte streams and message framing, protocol parsing, RESP serialization, persistent connections, multi-client servers, thread-per-connection concurrency, concurrent shared state, key-value storage, TTL and lazy expiration, Redis-style data types, ordered stream IDs, automatic identifier generation, error handling, API/data-model boundaries, incremental architecture design, and testing at the network boundary.

---

## 🤝 Contribution

This project is under active development. Feel free to open an issue or submit a pull request with improvements, fixes, or ideas for new commands and data types.

---

## ⚠️ Disclaimer

ByteCache is an **educational, Redis-inspired implementation**. It is not Redis, doesn't attempt to reproduce Redis's full internal architecture, and is not intended for production use.

Its purpose is to build first-hand understanding of networking, protocols, concurrency, in-memory storage, and backend systems — by implementing the mechanisms, not just consuming them.

---

## 📜 License

License TBD before public release.

<p align="center">
  Built with ☕ Java and a genuine curiosity about what's underneath Redis.
</p>