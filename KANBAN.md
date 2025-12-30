## # Execution-Focused Refactor Task List (Kanban-ready)

---

## Sanity commands (manual)

**Core cache crate verification:**
```bash
# From stream-download-cache directory
cargo test --lib --offline --target-dir /tmp/cache-target
cargo test --test integration_tests --offline --target-dir /tmp/cache-target
```

**Core storage verification (baseline):**
```bash
# From workspace root  
cargo check -p stream-download
```

**HLS verification (currently expected to fail - uses legacy API):**
```bash
# From workspace root
cargo check -p stream-download-hls  # Expected to fail - needs migration
```

**Integration verification:**
```bash
# From workspace root
cargo test -p stream-download-cache  # All cache tests
```