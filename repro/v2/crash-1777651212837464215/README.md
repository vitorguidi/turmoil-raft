## Commits

- **Failing:** `099ba50` — oracle checks all `role == Leader` nodes, including zombie leaders
- **Passing:** `repro/v2/oracle-leader-selection` — oracle skips zombie leaders whose term is below the cluster-wide max

## Root cause

The `assert_leader_completeness` oracle checked every node with `role == Leader`.
In a 3-node cluster, a node can remain `role == Leader` long after the rest of
the cluster has moved on to a higher term (a "zombie leader" that hasn't
received any message from the new term yet).

When the *real* leader (highest term) steps down to follower (e.g. it hears
about an even newer election), the zombie becomes the **only** node with
`role == Leader`. The oracle then validates the zombie's log against committed
entries from the real leader — and panics because the zombie's log diverged
at uncommitted indices that the real leader later committed.

This is **not a Raft safety violation**. The zombie leader's stale entries will
be overwritten once it contacts the current-term majority. No committed entry
can ever be lost because the election restriction prevents the zombie from
winning a future election without first catching up.

## Fix

Compare each leader's term against the **global** max term (across all servers,
not just leaders). Skip any leader whose term is strictly less — it's a zombie.

```diff
-        let max_leader_term = leaders.iter().map(|(_, t)| *t).max();
+        // A leader is stale (zombie) if any server in the cluster has
+        // already moved to a higher term.  Only check leaders whose term
+        // equals the global max — those are the only ones whose log is
+        // still authoritative.
+        let max_term = states.iter().map(|s| s.term).max().unwrap_or(0);

         for (leader_idx, leader_term) in leaders {
-            if Some(leader_term) != max_leader_term {
+            if leader_term < max_term {
                 continue;
             }
```

## Reproduction

```bash
RUST_LOG=info RAFT_FUZZ=1 FUZZ_MODE=reproduce \
  FUZZ_FILE=repro/v2/crash-1777651212837464215/config.json \
  cargo test --test raft_simulation raft_fuzz -- --nocapture
```
