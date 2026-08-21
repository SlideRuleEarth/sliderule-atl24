# AGENTS.md

ATL24 near-shore bathymetry plugin for the SlideRule science data processing
system. Builds a C++ shared module (`atl24.so`) loaded by a SlideRule server —
this is **not** a standalone application.

## Critical: sibling repo dependencies

The build expects two sibling repos checked out next to this one:
- `../sliderule` — the SlideRule server/library (provides headers under
  `include/sliderule`, the `job_runner.lua`, and the `slideruleearth` target).
- `../atl24_v2_algorithms` — header-only ATL24 v2 algorithm library + `models/atl24.tgz`.

CMake shells out to `git describe` in all three repos to embed version info, so
all three must be valid git checkouts. Override paths with `SLIDERULE=` and
`ATL24=` make vars if not at the default location.

## Build

Wraps CMake (build tree lives in `build/`). Order matters:

```
make config      # cmake configure (Release, needs ATL24DIR)
make             # build -> build/atl24.so
make install     # install .so + tables/*.csv + atl24.tgz into sliderule confdir
```

- Requires: **gcc14-g++** (default `MAKECFG=-DCMAKE_CXX_COMPILER=gcc14-g++`),
  **C++23**, `xgboost`, `LibArchive`, `Lua 5.3`, `libuuid`.
- `make config-stage-debug` builds Debug with **AddressSanitizer** enabled
  (`-fsanitize=address`) and installs into `../sliderule/stage/sliderule`.
- `make clean` runs cmake clean; `make distclean` removes `build/` entirely.

## Tests

pytests are **integration tests against a live SlideRule cluster**, not offline
unit tests. `conftest.py` calls `sliderule.init(domain=...)`.

```
pytest pytests --domain slideruleearth.io --organization sliderule
```

Options: `--domain`, `--organization`, `--desired_nodes`. Tests assert on exact
counts from known granules (e.g. `test_atl24g2` checks specific photon/flag
totals), so they fail if the server, algorithm version, or model changes.

Other test dirs: `selftests/*.lua` (Lua self-tests), `systests/` (result
comparison scripts). No CI workflows in this repo.

## Layout

- `package/` — C++ plugin sources (`Atl24Runner`, `Atl24Uncertainty`,
  `Atl24Writer`, `BlunderRunner`, `PluginFields`, `atl24_plugin.cpp` registers
  the Lua `atl24` module). `KdExperiment.*` exists but is not in the CMake build.
- `endpoints/*.lua` — server API endpoints (`atl24g2`, `atl24kd`) that wire
  dataframes -> runners -> writer. These run inside the SlideRule server.
- `tables/*.csv` — SNR/THU/Transport ATLAS lookup tables installed with the plugin.
- `utils/` — generation & QA scripts (`gen_atl24r*`, `check_atl24r*`, monitors).
- `docker/atl24/` — plugin build + runtime image. `docker/atl24d/` — a separate
  conda-based (`atl24meta` env) runner, distinct from the C++ plugin.

## Docker / run helpers (from Makefile)

- `make docker-runner` — rsyncs this repo + `../sliderule` + `../atl24_v2_algorithms`
  into `stage/` and builds the `sliderule:runner` image.
- `make test-docker-run` / `make test-atl24-run` — run a granule through the
  runner locally.

## Release

`make tag VERSION=vX.Y.Z` writes `version.txt`, commits, tags, pushes, and cuts
a GitHub release. `make release` = distclean + tag + build + publish. Keep
`version.txt` in sync (currently `v3.0.0`; it is compiled in as `BINID`).
