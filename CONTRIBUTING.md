# Contributing to Flight Intelligence Platform

Thank you for your interest in contributing! This document outlines the guidelines and processes for contributing to this project.

---

## Branch Protection Rules

The `main` branch is protected with the following rules (configured in GitHub repo settings):

| Rule | Description |
|------|-------------|
| **Require Pull Request** | All changes must go through a PR — no direct pushes to `main` |
| **Require CI Checks** | The `Lint & Test` and `Security Scan` jobs must pass before merging |
| **Require Review** | At least 1 approving review is required before merge |
| **No Force Pushes** | Force pushes to `main` are disabled to protect commit history |

> **Setup:** These rules must be configured manually in **Settings → Branches → Add branch protection rule** for `main`.

---

## Development Workflow

### 1. Create a Feature Branch

```bash
git checkout -b feature/your-feature-name
```

### 2. Make Your Changes

- Follow existing code style (flake8 enforced in CI)
- Add tests for new functionality in `tests/`
- Update documentation if adding new features

### 3. Run Checks Locally

```bash
# Lint
flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics

# Tests with coverage
python -m pytest tests/ -v --cov=scripts --cov=spark/schemas --cov-report=term-missing
```

### 4. Push and Create a Pull Request

```bash
git push origin feature/your-feature-name
```

Then open a PR against `main` on GitHub. CI will automatically run:
- **Lint & Test** — flake8 linting + pytest with coverage (Python 3.10 & 3.11)
- **Security Scan** — pip-audit vulnerability check
- **Schema Validation** — Pandera contract verification

### 5. Get Review and Merge

Once CI passes and your PR is approved, it can be merged into `main`.

---

## Creating a Release

To publish a new release:

```bash
git tag v1.0.0
git push --tags
```

This triggers the `release.yml` workflow, which auto-generates a changelog and creates a GitHub Release.

---

## CI/CD Workflows

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `ci_cd.yml` | Push/PR to `main` | Lint, test, schema validation, Docker build |
| `ci_basic.yml` | Manual dispatch | Generate flight data and commit to repo |
| `ci_integration.yml` | Push to `main` | Docker Compose health check (core services) |
| `ci_security.yml` | Push/PR to `main` | pip-audit dependency vulnerability scan |
| `ci_docs.yml` | Push to `main` | Auto-generate API docs to GitHub Pages |
| `ci_data_quality.yml` | Weekly (Mon 9AM) | Scheduled data quality report |
| `release.yml` | Tag push (`v*`) | Auto-generate release with changelog |
