
# CLAUDE.md

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Git Commits

- The user makes all commits. Never run `git commit`, `git push` or anything else that changes history.
- Commit messages must never mention Claude or any AI tool: no co-author trailers, no "generated with" lines.
- **`gmtcc`** means "give me the commit command". Reply with a single command the user can paste, in this format:

  ```
  git commit -m "x" -m "y"
  ```

  where `x` is a succinct commit title and `y` is a detailed description of what changed and why, written as a list so it reads clearly on GitHub: one change per line, each starting with a short bracketed title, e.g.

  ```
  git commit -m "Add tender results" -m "- [Tender results] New event-based dataset from FMIP, one row per security.
  - [PENDING] Notes the tender data's quirks."
  ```

## Sessions

- **`gmid`** means "give me your id". Run `ListAgents` and reply with this session's name only, e.g. `vetro-back-89`.