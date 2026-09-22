# ⚡ ByteCache

**A Redis-inspired in-memory key-value server, built from raw TCP sockets up — no frameworks, no shortcuts.**

[![Java](https://img.shields.io/badge/Java-25-orange?logo=openjdk)](https://openjdk.org/)
[![Build](https://img.shields.io/badge/build-Maven-blue?logo=apachemaven)](https://maven.apache.org/)
[![Status](https://img.shields.io/badge/status-in%20development-yellow)]()
[![License](https://img.shields.io/badge/license-TBD-lightgrey)]()

> Most backend developers use Redis every day. Very few have opened it up to see what actually happens between a client typing `SET key value` and a value landing in memory. ByteCache is my attempt to close that gap — by rebuilding the core of Redis from a bare TCP socket, one layer at a time.

---

## Table of Contents

- [Why This Project Exists](#why-this-project-exists)
- [What ByteCache Actually Does](#what-bytecache-actually-does)
- [Architecture](#architecture)
- [Engineering Deep Dives](#engineering-deep-dives)
- [Current Status](#current-status)
- [Roadmap](#roadmap)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [Testing Philosophy](#testing-philosophy)
- [Project Structure](#project-structure)
- [Design Principles](#design-principles)
- [What I'm Learning](#what-im-learning)
- [Disclaimer](#disclaimer)

---

## Why This Project Exists

Frameworks like Spring Boot are excellent at hiding complexity — sockets, byte streams, protocol parsing, and thread management all disappear behind a few annotations. That's great for shipping products. It's terrible for understanding what's actually happening underneath.

ByteCache deliberately avoids that comfort. Instead of importing a Redis client or scaffolding with Spring, it starts at `java.net.ServerSocket` and works upward — building a TCP server, a RESP protocol parser, a command dispatcher, and an in-memory store, all by hand.

**The goal isn't to replace Redis.** It's to be able to answer, from first-hand experience, questions like:

- What actually happens on the wire when a client sends `SET foo bar`?
- Why does RESP use a byte-stream-friendly format instead of JSON?
- What breaks when two clients write to the same key at the same time — and how do you fix it?
- Why did the real Redis team choose a single-threaded event loop instead of a thread pool?

---

## What ByteCache Actually Does

At a high level, ByteCache accepts TCP connections, speaks the Redis **RESP** wire protocol, parses incoming commands, and executes them against an in-memory key-value store — the same conceptual pipeline that powers real Redis:

```
Client (redis-cli or custom TCP client)
        │  raw TCP bytes
        ▼
  Network Server           — accepts & manages connections
        ▼
  RESP Protocol Layer      — parses/serializes the wire format
        ▼
  Command Dispatcher       — routes parsed commands to handlers
        ▼
  Command Handlers         — PING, SET, GET, DEL, EXISTS, ...
        ▼
  In-Memory KV Store       — the actual data, with TTL/expiry
```

Nothing on this path comes from a library. Every arrow above is code I wrote and can explain line by line.

---

## Architecture

```
                         ┌───────────────────────────┐
                         │          Client           │
                         │  redis-cli / custom TCP   │
                         └────────────┬──────────────┘
                                      │  TCP
                                      ▼
                         ┌───────────────────────────┐
                         │        TCP Server         │
                         │  ServerSocket / accept()  │
                         └────────────┬──────────────┘
                                      ▼
                         ┌───────────────────────────┐
                         │       RESP Parser         │
                         │  bytes → structured cmds  │
                         └────────────┬──────────────┘
                                      ▼
                         ┌───────────────────────────┐
                         │    Command Dispatcher     │
                         └───────┬──────────┬────────┘
                                 ▼          ▼
                         ┌────────────┐ ┌────────────┐
                         │  Handler   │ │  Handler   │  ← extensible per-command
                         └──────┬─────┘ └──────┬─────┘
                                └──────┬───────┘
                                       ▼
                             ┌───────────────────┐
                             │   In-Memory Store │
                             │  KV + TTL/expiry  │
                             └───────────────────┘
```

The architecture is intentionally minimal right now. New abstractions (connection pools, event loops, persistence layers) get introduced only when a real requirement demands them — not preemptively. See [Design Principles](#design-principles).

---

## Engineering Deep Dives

Each phase of this project maps to a concrete systems concept, not just a feature checkbox:

| Layer | What's Being Solved | Concepts Explored |
|---|---|---|
| **Networking** | How does a client and server actually talk over a network? | Sockets, blocking I/O, `ServerSocket`/`Socket`, streams |
| **Protocol (RESP)** | How do you turn a raw byte stream into structured messages? | Wire formats, serialization, parsing state machines |
| **Command Execution** | How do you route a parsed command to the right behavior cleanly? | Dispatch patterns, extensibility, error handling |
| **Storage** | How do you manage data lifetime, not just data presence? | Key-value maps, TTL, lazy vs. active expiration |
| **Concurrency** | What happens when multiple clients hit shared state at once? | Threading, synchronization, race conditions, alternative concurrency models |

---

## Current Status

ByteCache is in its **initial networking stage**. What's working today:

- ✅ TCP server listening on `localhost:6379`
- ✅ Accepting client connections via `ServerSocket`
- ✅ Reading/writing through Java socket streams
- ✅ Returning a valid RESP simple-string response

```
$ redis-cli -p 6379 PING
+PONG
```

Not yet implemented: full request parsing, persistent connections, the command pipeline, and concurrency handling — all tracked below.

---

## Roadmap

### Networking
- [x] TCP server
- [x] Client connection acceptance
- [ ] Client request reading
- [ ] Persistent client connections
- [ ] Multiple simultaneous clients
- [ ] Connection lifecycle handling

### RESP Protocol
- [ ] RESP at the byte level
- [ ] Simple strings / errors / integers
- [ ] Bulk strings / arrays
- [ ] Full RESP parser + serializer

### Commands
`PING` → `SET` → `GET` → `DEL` → `EXISTS` → *(more as the project grows)*

### Storage
- [ ] In-memory key-value store
- [ ] Key lookup / deletion
- [ ] Expiration & TTL
- [ ] Expired-key handling (lazy vs. active)

### Concurrency
- [ ] Multi-client handling
- [ ] Thread-per-connection model
- [ ] Shared-state safety
- [ ] Comparison against alternative concurrency models (event loop, thread pool)

### Engineering Hardening
- [ ] Unit + integration tests
- [ ] Manual TCP test client
- [ ] Structured error handling & logging
- [ ] Clean package boundaries
- [ ] Performance benchmarking

---

## Tech Stack

| Technology | Role | Why This, Not Something Else |
|---|---|---|
| **Java** | Core implementation | Gives direct control over sockets, threads, and memory — the things this project exists to learn |
| **Maven** | Build & dependency management | Standard, predictable Java tooling; keeps focus on the server, not the build system |
| **Raw TCP (`java.net`)** | Transport layer | No networking abstraction — every packet path is visible and understood |
| **RESP** | Wire protocol | The actual protocol real Redis and `redis-cli` speak, enabling real interoperability testing |
| **JUnit** | Testing (planned) | Verifies behavior, not just compilation, as the command/storage layers grow |

**No Spring Boot.** This is intentional — Spring hides the exact concepts (connection handling, serialization, request routing) this project is designed to expose.

---

## Getting Started

### Requirements
- JDK 25 (or compatible)
- Maven
- Any TCP-capable client (`redis-cli`, `nc`, or a custom client)

### Build

```bash
mvn clean compile
```

### Run

```bash
mvn exec:java   # or run Main.java directly from your IDE
```

The server listens on:

```
localhost:6379
```

### Try it

```bash
redis-cli -p 6379 PING
# +PONG
```

---

## Testing Philosophy

ByteCache uses the CodeCrafters "Build Your Own Redis" challenge as a **reference for progression and specification only** — not as the test suite. Development and validation are independent, using:

- Manual TCP clients (including a custom Java test client)
- Unit tests for parsing/storage logic
- Integration tests for end-to-end command flows
- Direct wire-level inspection of RESP traffic
- CodeCrafters checks used only as an optional external sanity check

This matters because passing an external test suite proves compatibility — it doesn't prove *understanding*. Building the verification tooling by hand is part of the learning goal.

---

## Project Structure

**Today:**

```
bytecache/
├── src/
│   ├── main/java/Main.java
│   └── test/
├── pom.xml
└── README.md
```

**Where it's headed** (structure follows responsibility, not guesswork):

```
src/
├── main/java/
│   ├── Main.java
│   ├── server/       # connection acceptance & lifecycle
│   ├── network/       # socket I/O
│   ├── protocol/       # RESP parsing & serialization
│   ├── command/       # dispatch & handlers
│   └── storage/       # in-memory store, TTL/expiry
└── test/
```

---

## Design Principles

- **Understand before abstracting** — new classes appear when a real responsibility needs separating, not preemptively.
- **No black boxes** — if a Java API or library hides an important mechanism, the project investigates what's underneath.
- **Simplicity over cleverness** — the goal is understanding the system, not showcasing architecture for its own sake.
- **Verify behavior, not just compilation** — a green build doesn't mean a network server works correctly; the project is tested through direct experimentation.
- **Learn from the gap** — every simplification is compared back against real Redis to understand *why* Redis made the choices it did.

---

## What I'm Learning

This project is a structured, multi-phase path from application-level backend work toward lower-level systems programming:

```
Networking → Protocols → Parsing → Data Structures → Concurrency → Memory → Architecture
```

Concretely, by the end of this project I'll be able to speak from experience about:

- How TCP delivers a byte stream, and why application protocols exist on top of it
- How a text-based wire protocol like RESP is designed and parsed
- Trade-offs in key-value storage design (lookup, deletion, expiration)
- Thread-safety failure modes in networked servers, and how to fix them
- Why Redis's real design choices (e.g., single-threaded event loop) exist

---

## Disclaimer

ByteCache is an **educational project** inspired by Redis. It is not Redis, is not production-compatible with Redis, and is not intended for production use. Its purpose is to build a first-hand, from-the-wire-up understanding of backend and networking fundamentals.

---

## License

*License TBD — will be added before public release.*