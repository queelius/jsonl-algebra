# All I Kept Was a `.jsonl` File

> I Spent Weeks Designing a “Lightweight Database.”
>  All I Kept Was a `.jsonl` File.

---

### 1. The Premise

I wanted a tiny, version‑control‑friendly database format—simpler than SQLite, richer than CSV.
“Easy,” I thought. “CSV plus a JSON schema sidecar, maybe a CLI, relational joins, views…”

Spoiler: every new layer solved a problem the previous layer created.

---

### 2. The Detour — Adding Layers

| Layer                                         | Why I Added It          | What Went Wrong                         |
| --------------------------------------------- | ----------------------- | --------------------------------------- |
| **CSV + per‑table JSON schema**               | Types, keys, validation | Duplication; header drift; quoting hell |
| **Relation objects with streaming iterators** | Lazy memory use         | Exhausted iterators, subtle bugs        |
| **View files & materialization logic**        | “SQL‑like” power        | IO noise, unclear semantics             |
| **Full CLI w/ persist commands**              | Nice UX                 | Just wrappers for wrappers              |

Each addition felt clever—until it didn’t.

---

### 3. The Un‑building Phase

I asked, *“What irreducible need remains if I delete this layer?”*
Piece by piece I removed:

* per‑table schemas
* reset logic
* auto‑persist tricks
* even the catalog for foreign keys

…and nothing essential broke.

---

### 4. What Survived

A single convention:

> **One JSON object per line, in a file named `*.jsonl`.**

That’s it.
Need really advanced filtering? Pipe through `jq` or run a five‑line Python script.

---

### 5. Why Simpler Won

| Perks of `.jsonl`      | Hidden Cost Avoided         |
| ---------------------- | --------------------------- |
| Human‑diffable text    | No binary dumps             |
| Streamable GBs         | No exhausted generators     |
| Nested structures free | No quoting gymnastics       |
| Zero tooling lock‑in   | No API promises to maintain |

---

### 6. The Real Lesson: Social Coordination

Most “big wins” in data tooling aren’t technical genius; they’re social:

* **CSV** won because everyone agreed to tolerate its warts.
* **Markdown** won because “good enough” beats rich‑text battles.
* **Git** won because a single mental model (“commit”) spread like a meme.

If a convention is:

1. **Plaintext**
2. **Easy to explain in one sentence**
3. **Good‑enough interop**

…then the coordination win dwarfs any feature checklist.

`.jsonl` clears that bar. Anything I’d add would mostly be **me** having fun—not solving a mass pain‑point.

---

### 7. What I Actually Shipped

Nothing—except this insight:

> **“Put each record on its own JSON line, commit it, and move on.”**

I closed my repo, published this post, and kept the loader script (\~30 LOC) in a gist.

---

### 8. When *Not* to Use `.jsonl`

* You need ACID transactions → SQLite/DuckDB
* You need strict typing & joins at scale → Postgres / BigQuery
* You need columnar analytics → Parquet + DuckDB

But for configs, logs, small/medium datasets, and exploratory hacks:
`.jsonl` + `jq` + Git is unbeatable in simplicity‑per‑byte.

---

### 9. Takeaways

1. **Architect by deletion.** Start adding; keep deleting until pain appears.
2. **Ship conventions, not frameworks.** Naming the pattern is often enough.
3. **Judge by coordination cost.** The simplest format lots of people accept beats a genius format nobody standardizes on.

I didn’t deliver a shiny package. (Well, I did deliver a shiny JSONL. I'm not a monster.)
