# Agent Instructions

This project uses **bd** (beads, 1.x) for issue tracking. Run `bd prime` for the workflow.

## Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id> --reason "..."  # Complete work
bd export -o .beads/issues.jsonl     # Write the issues to the tracked file
```

Beads keeps its database in `.beads/embeddeddolt` (not tracked). There is no
Dolt remote and no daemon: `.beads/issues.jsonl`, committed with the code, is
the copy that leaves the machine. Do not use `bd sync` or `bd dolt push` (they
are the Dolt-remote loop) and do not set `sync.remote`.

## Working an issue

Issues came over from `doc/BACKLOG.md` (removed 2026-10-23). Each keeps its
old ID at the front of its title and as its external ref (`backlog-N18`), so
`bd search N18` finds the issue a commit, a doc or an older issue mentions.

- One issue at a time, one commit per issue, the full suite on a fresh
  database after each.
- Verify the issue's `file:line` anchors first: they are as of the commit
  that last touched the issue and drift.
- Fix it, add the test that would have caught it, and close the issue naming
  that test in `--reason`, in the same commit as the fix (export first), so
  `git log -S<test name>` finds it.
- An issue that is deliberately not going to be fixed is closed with
  `--reason "wontfix: <one line of reasoning>"` so it is not re-investigated.
- Findings get filed as issues as soon as they are found, with `file:line`
  anchors and where they came from.

## Landing the Plane (Session Completion)

1. **File issues for remaining work**
2. **Run quality gates** (if code changed): tests, linters, builds
3. **Update issue status**: close finished work, update in-progress items
4. **Commit, when asked to**:
   ```bash
   bd export -o .beads/issues.jsonl
   git add .beads/issues.jsonl   # with the code changes
   git commit -m "..."
   ```
5. **Hand off**: summarize changes, validation, issue status, and anything
   not committed or pushed
