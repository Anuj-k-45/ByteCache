# ⚡ ByteCache

**A Redis-inspired in-memory data store built from raw TCP sockets — no Spring Boot, no Redis server underneath, no networking black boxes.**

[![Java](https://img.shields.io/badge/Java-25-orange?logo=openjdk)](https://openjdk.org/)
[![Build](https://img.shields.io/badge/build-Maven-blue?logo=apachemaven)](https://maven.apache.org/)
[![Protocol](https://img.shields.io/badge/protocol-RESP2-red)]
[![Status](https://img.shields.io/badge/status-active%20development-yellow)]

> ByteCache is a from-scratch implementation of the core ideas behind Redis, built to understand what actually happens between a client sending `SET`, `GET`, or `XADD` and data being stored in memory.

---

## Table of Contents

- [Why This Project Exists](#why-this-project-exists)
- [What ByteCache Currently Implements](#what-bytecache-currently-implements)
- [Architecture](#architecture)
- [Request Lifecycle](#request-lifecycle)
- [Storage Model](#storage-model)
- [Redis Streams Internals](#redis-streams-internals)
- [RESP Protocol](#resp-protocol)
- [Concurrency Model](#concurrency-model)
- [Supported Commands](#supported-commands)
- [Engineering Deep Dives](#engineering-deep-dives)
- [Current Status](#current-status)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Testing & Verification](#testing--verification)
- [Design Principles](#design-principles)
- [Roadmap](#roadmap)
- [What I'm Learning](#what-im-learning)
- [Disclaimer](#disclaimer)

---

## Why This Project Exists

Redis is usually consumed through a client library:

```text
Application
    │
    └── redis.set("user:42", "Anuj")
             │
             ▼
          Redis
```

That is convenient, but it hides most of the interesting systems work.

ByteCache deliberately starts much lower:

```text
TCP connection
      ↓
raw bytes
      ↓
RESP parsing
      ↓
commands
      ↓
command execution
      ↓
in-memory data structures
```

The goal is not to replace Redis.

The goal is to understand, by implementing it, questions such as:

- What does a Redis command actually look like on the wire?
- How does a Java `Socket` expose a TCP byte stream?
- Why can't one `read()` call be treated as one command?
- How can a server handle multiple commands over one persistent connection?
- How do different Redis data types fit into one key-value store?
- How are stream IDs ordered and automatically generated?
- What happens when multiple client threads access the same in-memory state?
- Where does TTL information actually live?
- What responsibilities belong to networking, protocol parsing, command execution, and storage?

This project is intentionally built without Spring Boot so that the networking and protocol layers remain visible.

---

# What ByteCache Currently Implements

ByteCache has progressed beyond a basic TCP echo server and currently implements a working subset of Redis-like behavior.

| Area | Current implementation |
|---|---|
| Transport | TCP via Java `ServerSocket` / `Socket` |
| Connection handling | Persistent client connections |
| Multi-client support | Thread-per-connection |
| Wire protocol | RESP-style request parsing and RESP responses |
| Request parsing | Incremental buffering across socket reads |
| Commands | `PING`, `ECHO`, `SET`, `GET`, `TYPE`, `XADD` |
| Strings | In-memory string values |
| Expiration | `SET ... PX <milliseconds>` with lazy expiration |
| Data types | String + Stream |
| Streams | Ordered `StreamEntry` objects |
| Stream IDs | Explicit IDs, `milliseconds-*`, and `*` |
| Stream validation | Monotonic ID validation |
| Shared state | `ConcurrentHashMap` |
| Errors | RESP error responses for supported invalid operations |
| External client | Tested with `redis-cli` over TCP |
| Framework | None — plain Java networking APIs |

The implementation is intentionally smaller than Redis itself. The focus is on understanding the mechanisms behind the features rather than reproducing the entire Redis codebase.

---

# Architecture

The current architecture separates the major responsibilities without introducing abstractions that the project does not yet need.

```text
                         ┌─────────────────────────────┐
                         │            Client           │
                         │      redis-cli / TCP        │
                         └──────────────┬──────────────┘
                                        │
                                  TCP byte stream
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │       RedisServer           │
                         │                              │
                         │ ServerSocket.accept()       │
                         │ Creates client connection   │
                         └──────────────┬──────────────┘
                                        │
                              one Socket per client
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │      ClientConnection       │
                         │                              │
                         │ InputStream / OutputStream │
                         │ connection lifecycle       │
                         └──────────────┬──────────────┘
                                        │
                                  raw RESP bytes
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │        RespParser           │
                         │                              │
                         │ bytes → complete commands  │
                         │ handles partial reads      │
                         │ handles multiple commands  │
                         └──────────────┬──────────────┘
                                        │
                              List<String> command
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │      Command Handling       │
                         │                              │
                         │ PING / ECHO / SET / GET    │
                         │ TYPE / XADD                │
                         └──────────────┬──────────────┘
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │         RedisStore          │
                         │                              │
                         │ ConcurrentHashMap          │
                         │ String values              │
                         │ Redis streams              │
                         │ TTL metadata               │
                         └─────────────────────────────┘
```

### Responsibility boundaries

**`Main`**

Starts the server.

**`RedisServer`**

Owns the listening `ServerSocket`, accepts incoming clients, and creates a dedicated `ClientConnection` thread for each client.

**`ClientConnection`**

Owns one established TCP connection. It reads bytes, feeds them into the parser, executes complete commands, and writes RESP responses.

**`RespParser`**

Knows how RESP is structured. It does not know what `SET`, `GET`, or `XADD` mean.

**`RedisStore`**

Owns the actual in-memory data. It is shared by all client connections.

**Storage classes**

`StoredValue`, `RedisStream`, `StreamEntry`, and `StreamId` model the supported Redis data types and their internal state.

---

# Request Lifecycle

Consider:

```text
SET name Anuj
```

The actual flow is approximately:

```text
redis-cli
   │
   │ RESP bytes
   ▼
TCP Socket
   │
   ▼
ClientConnection
   │
   │ InputStream.read()
   ▼
RespParser
   │
   │ ["SET", "name", "Anuj"]
   ▼
Command handling
   │
   ▼
RedisStore
   │
   │ data.put("name", StoredValue(...))
   ▼
In-memory state
   │
   ▼
RESP response
   │
   │ +OK\r\n
   ▼
TCP Socket
   │
   ▼
redis-cli
```

For a stream command:

```text
XADD events * user Anuj action login
```

the path becomes:

```text
Client
  ↓
TCP bytes
  ↓
RespParser
  ↓
["XADD", "events", "*", "user", "Anuj", "action", "login"]
  ↓
handleXAdd()
  ↓
RedisStore
  ↓
RedisStream
  ↓
StreamEntry
  ↓
StreamId + field/value map
  ↓
RESP bulk-string ID response
```

---

# Storage Model

The central structure is:

```java
Map<String, Object>
```

implemented as a `ConcurrentHashMap`.

Conceptually:

```text
RedisStore
│
└── data : Map<String, Object>
     │
     ├── "name"     → StoredValue
     │                 ├── value = "Anuj"
     │                 └── expiresAt = null
     │
     ├── "session"  → StoredValue
     │                 ├── value = "abc123"
     │                 └── expiresAt = timestamp
     │
     └── "events"   → RedisStream
                       └── entries
```

The `Object` value is intentional: one key can represent different supported Redis data types.

For example:

```text
"name"   → StoredValue
"age"    → StoredValue
"events" → RedisStream
```

### String values

A string is represented by:

```java
StoredValue
```

which contains:

```text
value
expiresAt
```

This allows normal values and expiring values to use the same storage abstraction.

For:

```text
SET name Anuj
```

the conceptual state is:

```text
"name"
   │
   ▼
StoredValue
   ├── value     = "Anuj"
   └── expiresAt = null
```

For:

```text
SET session abc123 PX 5000
```

the state is:

```text
"session"
   │
   ▼
StoredValue
   ├── value     = "abc123"
   └── expiresAt = currentTimeMillis + 5000
```

Expiration is currently **lazy**: when a key is accessed, its expiration timestamp is checked and the key is removed if expired.

---

# Redis Streams Internals

Streams are currently represented using a small set of focused Java classes:

```text
RedisStore
   │
   └── key: "events"
          │
          ▼
      RedisStream
          │
          └── List<StreamEntry>
                  │
                  ├── StreamEntry
                  ├── StreamEntry
                  └── StreamEntry
```

Each `StreamEntry` contains:

```text
StreamEntry
├── StreamId
│   ├── millisecondsTime
│   └── sequenceNumber
│
└── Map<String, String>
    ├── field → value
    ├── field → value
    └── ...
```

For example:

```text
XADD events 1000-0 user Anuj action login
XADD events 1001-0 user Rahul action logout
```

produces conceptually:

```text
"events"
    │
    ▼
RedisStream
    │
    └── entries
         │
         ├── [0]
         │    ├── ID: 1000-0
         │    └── fields:
         │         user   → Anuj
         │         action → login
         │
         └── [1]
              ├── ID: 1001-0
              └── fields:
                   user   → Rahul
                   action → logout
```

### Stream IDs

A stream ID is not stored as a single string internally.

For:

```text
1000-7
```

the Java object contains:

```text
StreamId
├── millisecondsTime = 1000
└── sequenceNumber   = 7
```

The textual form is produced by `toString()`.

IDs are comparable, allowing the stream to enforce increasing order.

### Supported XADD ID forms

ByteCache currently supports:

```text
1000-0     explicit ID
1000-*     generate sequence number
*          generate timestamp + sequence number
```

For repeated entries with the same millisecond timestamp:

```text
1000-0
1000-1
1000-2
1000-3
```

The sequence number preserves ordering within the same millisecond.

The stream also validates that a newly inserted explicit ID is greater than the current stream tail.

---

# RESP Protocol

ByteCache communicates using the Redis Serialization Protocol style used by `redis-cli`.

For example:

```text
PING
```

arrives approximately as:

```text
*1\r\n
$4\r\n
PING\r\n
```

and the server responds:

```text
+PONG\r\n
```

A bulk-string response such as:

```text
Anuj
```

is encoded as:

```text
$4\r\n
Anuj\r\n
```

### Why the parser buffers data

TCP provides a **byte stream**, not a stream of commands.

A single command may be split across multiple `read()` calls:

```text
read #1 → "*1\r\n$4"
read #2 → "\r\nPING\r\n"
```

Or several commands may arrive in one read:

```text
read #1 → command A + command B + command C
```

Therefore the parser maintains buffered bytes until a complete RESP command is available.

This is one of the most important networking lessons in the project:

> **One `read()` is not one request.**

---

# Concurrency Model

ByteCache currently uses a **thread-per-connection** model.

The server accepts clients continuously:

```text
Main server thread
       │
       ├── accept Client A → Thread A
       ├── accept Client B → Thread B
       ├── accept Client C → Thread C
       └── ...
```

Each client thread owns its socket connection:

```text
Thread A → Socket A → Client A
Thread B → Socket B → Client B
Thread C → Socket C → Client C
```

However, the store is shared:

```text
             RedisStore
                 │
        ConcurrentHashMap
          /       |       \
         /        |        \
    Thread A   Thread B   Thread C
```

`ConcurrentHashMap` is therefore used because multiple client threads can access the same store concurrently.

This is deliberately a simple concurrency model. It makes the relationship between connections, threads, and shared state explicit and provides a foundation for later comparison with event-loop and other server architectures.

---

# Supported Commands

## `PING`

```text
PING
```

Response:

```text
+PONG
```

Used as the basic connectivity and protocol test.

---

## `ECHO`

```text
ECHO hello
```

Response:

```text
hello
```

Implemented as a RESP bulk string.

---

## `SET`

```text
SET name Anuj
```

Stores a string value and returns:

```text
+OK
```

### With expiration

```text
SET session abc123 PX 5000
```

The value expires after the specified number of milliseconds.

Expiration is currently checked lazily when the key is accessed.

---

## `GET`

```text
GET name
```

Returns the stored value as a RESP bulk string.

For a missing or expired key:

```text
$-1
```

---

## `TYPE`

```text
TYPE name
TYPE events
TYPE missing
```

Returns:

```text
string
stream
none
```

This works by inspecting the Java object associated with the key.

---

## `XADD`

### Explicit ID

```text
XADD events 1000-0 user Anuj
```

### Auto-generated sequence number

```text
XADD events 1000-* user Anuj
```

### Fully auto-generated ID

```text
XADD events * user Anuj
```

A generated ID is returned as a RESP bulk string:

```text
"1000-0"
```

Fields are stored as:

```text
field → value
```

pairs inside each `StreamEntry`.

---

# Engineering Deep Dives

## 1. TCP is a byte stream

The project intentionally works directly with:

```java
ServerSocket
Socket
InputStream
OutputStream
```

This makes the transport layer visible.

TCP does not understand:

```text
PING
SET
GET
XADD
```

It only transports ordered bytes.

RESP gives those bytes application-level meaning.

---

## 2. Protocol parsing is separate from command execution

`RespParser` does not decide what `SET` means.

It only turns:

```text
RESP bytes
```

into:

```text
["SET", "name", "Anuj"]
```

Command handling then decides what to do with that structure.

This separation prevents protocol parsing from becoming tightly coupled to application behavior.

---

## 3. Persistent connections

A client connection is not closed after one command.

Conceptually:

```text
Connection
   │
   ├── PING
   ├── SET name Anuj
   ├── GET name
   ├── TYPE name
   ├── XADD events *
   └── ...
```

The same TCP connection can therefore carry multiple commands.

---

## 4. Lazy expiration

TTL does not require a background cleanup thread in the current implementation.

Instead:

```text
GET key
   │
   ▼
Does key exist?
   │
   ▼
Has expiresAt been reached?
   │
   ├── No  → return value
   │
   └── Yes → remove key → return missing
```

This keeps the current implementation small while making expiration semantics explicit.

---

## 5. Stream ID ordering

Stream IDs are represented as:

```text
(millisecondsTime, sequenceNumber)
```

Comparison is lexicographic:

```text
first compare millisecondsTime
then compare sequenceNumber
```

Therefore:

```text
1000-2 < 1000-3
1000-3 < 1001-0
```

This ordering is enforced when adding entries.

---

# Current Status

### Implemented

- [x] Raw TCP server
- [x] `ServerSocket` connection acceptance
- [x] Per-client `Socket`
- [x] Java input/output streams
- [x] Persistent client connections
- [x] Multiple simultaneous clients
- [x] Thread-per-connection model
- [x] RESP request parsing
- [x] Incremental parsing across partial reads
- [x] Multiple commands in a single read
- [x] RESP simple strings
- [x] RESP bulk strings
- [x] RESP errors
- [x] `PING`
- [x] `ECHO`
- [x] `SET`
- [x] `GET`
- [x] `SET PX`
- [x] Lazy expiration
- [x] `TYPE`
- [x] Redis Streams
- [x] Explicit stream IDs
- [x] `milliseconds-*` stream IDs
- [x] `*` auto-generated stream IDs
- [x] Stream ID ordering validation
- [x] Stream field/value pairs
- [x] Shared `ConcurrentHashMap` storage
- [x] Basic wrong-type protection
- [x] `redis-cli` interoperability testing

### In active development

The project is continuing toward broader Redis behavior and stronger engineering guarantees. Planned work is intentionally driven by the next concrete requirement rather than by prematurely building a large framework.

---

# Project Structure

Current structure:

```text
ByteCache/
│
├── src/
│   └── main/
│       └── java/
│           │
│           ├── Main.java
│           │
│           ├── server/
│           │   └── RedisServer.java
│           │
│           ├── connection/
│           │   └── ClientConnection.java
│           │
│           ├── protocol/
│           │   └── RespParser.java
│           │
│           └── storage/
│               ├── RedisStore.java
│               ├── StoredValue.java
│               ├── RedisStream.java
│               ├── StreamEntry.java
│               └── StreamId.java
│
├── pom.xml
└── README.md
```

### Why these boundaries?

The project avoids both extremes:

**Too little structure**

```text
Main.java
    └── 1000 lines of everything
```

and:

**Too much premature abstraction**

```text
40 interfaces
20 factories
10 strategies
before the server has a second command
```

Instead, classes are introduced around real responsibilities:

```text
Server lifecycle
       ↓
Connection lifecycle
       ↓
Protocol parsing
       ↓
Command behavior
       ↓
Data storage
```

---

# Getting Started

## Requirements

- JDK 25 or compatible Java version
- Maven
- `redis-cli` for convenient interoperability testing
- Windows/Linux/macOS with a TCP-capable environment

## Build

From the project root:

```bash
mvn clean compile
```

The project can also be compiled directly from the Java source tree during development.

## Run

Start the server through the application's `Main` class.

The default server port is:

```text
6379
```

The server will wait for TCP clients.

## Test with redis-cli

If the client can reach the server directly:

```bash
redis-cli -p 6379
```

Then:

```text
PING
SET name Anuj
GET name
TYPE name
SET session abc PX 5000
GET session
```

For stream behavior:

```text
XADD events 1000-0 user Anuj
XADD events 1001-0 user Rahul
XADD events 1002-* user Anuj
XADD events * user Anuj
TYPE events
```

In the current development environment, the Java server runs on Windows while `redis-cli` is also used from WSL. The WSL-to-Windows gateway address may therefore be used instead of `localhost`, depending on the local network configuration.

---

# Testing & Verification

ByteCache is developed incrementally using the CodeCrafters **Build Your Own Redis** challenge as a progression guide and behavioral reference.

The project is not treated as a collection of copied solutions.

Validation is performed through several levels:

### 1. Direct compilation

Every architectural change is compiled from the full source tree.

### 2. Real TCP interaction

The server is exercised through an actual TCP client rather than only direct Java method calls.

### 3. `redis-cli`

Using a real Redis client makes the wire-level compatibility meaningful.

### 4. Boundary experiments

Particular attention is given to cases such as:

- partial TCP reads
- multiple commands arriving together
- persistent connections
- missing keys
- expired keys
- invalid stream IDs
- duplicate/non-increasing stream IDs
- generated stream IDs
- multiple clients sharing the same store
- wrong data type operations

### 5. CodeCrafters

CodeCrafters checks are useful as an external compatibility signal and roadmap, while the primary goal remains understanding and independently implementing the underlying behavior.

---

# Design Principles

## Understand before abstracting

New classes and abstractions are introduced when a real responsibility appears.

The architecture evolves with the server instead of being designed as a large framework before the requirements exist.

## No black boxes

If the project uses:

```java
Socket
InputStream
ConcurrentHashMap
```

the important behavior of those components is studied rather than treated as magic.

## Protocol before convenience

The project works with actual wire-level bytes and RESP framing instead of hiding communication behind a high-level Redis client API.

## Separate concerns

Networking should not know how stream IDs work.

The RESP parser should not know what `SET` means.

Storage should not know how TCP works.

## Shared state must be intentional

Every client connection has its own thread, but the Redis store is shared.

That relationship is explicit in the architecture and is part of the concurrency design.

## Verify behavior, not just compilation

A server can compile perfectly while still failing at the network boundary.

Therefore, real clients and real TCP interactions are part of development.

## Learn from simplifications

ByteCache is intentionally smaller than Redis.

Each simplification creates an opportunity to ask:

> What does real Redis do here, and why?

---

# Roadmap

The next stages are driven by actual requirements rather than an arbitrary feature checklist.

### Protocol & command system

- [ ] Expand RESP value types as required
- [ ] Improve command validation and error semantics
- [ ] Further separate command dispatch from connection management
- [ ] Add broader command coverage

### Data structures

- [x] Strings
- [x] Streams
- [x] TTL metadata
- [ ] Additional Redis data types as required by the roadmap

### Streams

- [x] Explicit IDs
- [x] `milliseconds-*` IDs
- [x] `*` IDs
- [x] Monotonic ID validation
- [x] Field/value storage
- [ ] Stream read/query operations
- [ ] Additional stream semantics

### Concurrency & performance

- [x] Multiple clients
- [x] Shared concurrent store
- [x] Thread-per-connection model
- [ ] Measure behavior under concurrent load
- [ ] Investigate thread pools / event-loop architecture
- [ ] Benchmark command throughput and latency

### Persistence & durability

- [ ] Investigate persistence requirements
- [ ] Compare in-memory state with durable storage
- [ ] Explore recovery semantics

### Engineering quality

- [ ] Broader unit-test coverage
- [ ] Integration test suite
- [ ] Protocol-level test client
- [ ] Structured logging
- [ ] Performance benchmarks
- [ ] Failure and connection lifecycle testing

---

# What I'm Learning

ByteCache is intended to bridge the gap between high-level backend development and systems-oriented engineering.

The learning path is:

```text
Networking
    ↓
TCP byte streams
    ↓
Application protocols
    ↓
RESP framing/parsing
    ↓
Command execution
    ↓
In-memory data structures
    ↓
TTL / data lifetime
    ↓
Concurrency and shared state
    ↓
Streams and ordered identifiers
    ↓
Performance / architecture
    ↓
Persistence and durability
```

By building each layer directly, the project provides hands-on experience with:

- TCP socket programming in Java
- Blocking I/O
- Connection lifecycle management
- Byte streams and message framing
- Protocol parsing
- RESP serialization
- Persistent connections
- Multi-client servers
- Thread-per-connection concurrency
- Concurrent shared state
- Key-value storage
- TTL and lazy expiration
- Redis-style data types
- Ordered stream IDs
- Automatic identifier generation
- Error handling
- API/data-model boundaries
- Incremental architecture design
- Testing at the network boundary

---

# Resume-Level Engineering Highlights

The project demonstrates several backend and systems concepts that are easy to hide behind frameworks:

**Networking from first principles**

Implemented a TCP server directly with Java's `ServerSocket` and `Socket` APIs.

**Protocol implementation**

Implemented incremental RESP request parsing and RESP response serialization rather than using a Redis server/client implementation underneath.

**Concurrent server architecture**

Implemented persistent connections using a thread-per-connection model with a shared thread-safe in-memory store.

**In-memory data modeling**

Designed a polymorphic `Map<String, Object>` storage layer supporting strings and streams while keeping type-specific behavior in dedicated classes.

**Expiration semantics**

Implemented TTL metadata and lazy expiration using absolute expiration timestamps.

**Redis Streams**

Implemented ordered stream entries, structured stream IDs, explicit IDs, timestamp-based sequence generation, and fully automatic IDs.

**Systems-oriented debugging**

Used real TCP traffic and `redis-cli` to investigate byte-level behavior, persistent connections, partial reads, and protocol framing.

---

# Disclaimer

ByteCache is an **educational Redis-inspired implementation**.

It is not Redis, does not attempt to reproduce Redis's complete internal architecture, and is not intended for production use.

The project exists to develop first-hand understanding of networking, protocols, concurrency, in-memory storage, and backend systems by implementing the mechanisms rather than only consuming them.

---

# License

License TBD before public release.
