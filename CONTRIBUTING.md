# Contributing to Tributary

Thanks for your interest in contributing. This doc covers the bits of
workflow that aren't obvious from the code.

## Branching

- `master` — protected, release branch. Never push directly. Every commit on
  master triggers a release via `.github/workflows/release.yml`.
- `develop` — integration branch. Feature branches merge here first.
- `feature/...`, `fix/...`, `refactor/...`, `docs/...`, `test/...`,
  `chore/...` — branch from `develop`.

Pull requests target `develop` for normal work and `master` only when cutting
a release.

## Conventional Commits — required for auto-versioning

Tributary uses [Conventional Commits](https://www.conventionalcommits.org/)
so that [python-semantic-release](https://python-semantic-release.readthedocs.io/)
can decide the next version from the commit history. The
**squash-merge commit title** is what gets parsed, so your PR title must
follow the convention.

### Format

```
<type>(<scope>): <short summary in imperative mood>

<optional body — explain the why, not the what>

<optional footer — BREAKING CHANGE or issue refs>
```

### Types and their release impact

| Type | Version bump | Example |
|------|--------------|---------|
| `feat` | **minor** (`0.2.0` → `0.3.0`) | `feat(workers): add Kafka queue backend` |
| `fix` | **patch** (`0.2.0` → `0.2.1`) | `fix(redis): handle expired inflight messages` |
| `perf` | **patch** | `perf(chunker): skip empty splits` |
| `refactor` | no release | `refactor(cli): extract pipeline builder` |
| `docs` | no release | `docs: add distributed example` |
| `test` | no release | `test(workers): cover nack retry path` |
| `chore` | no release | `chore(deps): bump aiofiles` |
| `ci` | no release | `ci: add coverage artifact upload` |
| `build` | no release | `build: tighten wheel contents` |
| `style` | no release | `style: format with ruff` |
| `security` | no release | `security: pin cryptography` |

### Breaking changes → major bump

Any commit with `BREAKING CHANGE:` in the body or footer, or a `!` after the
type, triggers a **major** version bump regardless of the type:

```
feat(workers)!: rename BaseQueue.poll to BaseQueue.receive

BREAKING CHANGE: BaseQueue.poll has been renamed to BaseQueue.receive.
Update all backend implementations and callers accordingly.
```

While Tributary is on `0.x.y`, breaking changes bump the **minor** version
(`0.2.0` → `0.3.0`) per the `major_on_zero = false` setting in
`pyproject.toml`. Once we reach `1.0.0`, `BREAKING CHANGE` will bump the
major.

### PR label / footer overrides

If the auto-decision doesn't fit what you want — e.g., a PR contains only
`fix:` commits but you know it warrants a minor bump — add one of these
lines to the **PR description** or the squash-merge commit footer:

```
Release-As: major
Release-As: minor
Release-As: patch
```

The override wins over commit-type analysis.

## What the release pipeline does

When a PR merges to `master`, `.github/workflows/release.yml` runs:

1. **Test** — full pytest suite on master HEAD
2. **Analyze commits** since the last tag, decide next version
3. **Bump** `pyproject.toml` and `src/tributary/__init__.py`
4. **Update** `CHANGELOG.md` with auto-generated entries
5. **Commit + tag** the bump (`chore(release): v0.3.0 [skip ci]`)
6. **Build** the wheel and sdist (`uv build`)
7. **Publish** to PyPI via trusted publisher
8. **Create** a GitHub Release from the tag with changelog excerpt

If no commits since the last tag warrant a release (e.g., only `docs`,
`chore`, `test`), the whole pipeline no-ops — no tag, no publish.

## CI for feature branches

`.github/workflows/ci.yml` runs on every push to any branch except `master`
and on every PR. It runs lint + tests with coverage and uploads the
coverage XML as an artifact. It never publishes anything.

## Local development

```bash
uv sync
uv run pytest -v                        # run tests
uv run pytest --cov=tributary          # with coverage
```

## When in doubt

If you're unsure whether a change warrants a minor, patch, or no release,
ask in the PR before merging. It's much easier to adjust the commit message
before squash-merge than to unpublish a PyPI version.
