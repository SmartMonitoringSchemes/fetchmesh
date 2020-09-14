<p align="center">
  <img src="/docs/logo.png" height="150"><br/>
  <i>A Python library for working with the RIPE Atlas anchoring mesh.</i><br/><br/>
  <a href="https://github.com/SmartMonitoringSchemes/fetchmesh/actions">
    <img src="https://github.com/SmartMonitoringSchemes/fetchmesh/workflows/CI/badge.svg">
  </a>
  <a href="https://codecov.io/gh/SmartMonitoringSchemes/fetchmesh">
    <img src="https://codecov.io/gh/SmartMonitoringSchemes/fetchmesh/branch/master/graph/badge.svg?token=6w9W4QBFQx">
  </a>
  <a href="https://github.com/SmartMonitoringSchemes/fetchmesh/raw/gh-pages/fetchmesh.pdf">
    <img src="https://img.shields.io/badge/documentation-pdf-blue.svg?style=flat">
  </a>
</p>

fetchmesh is a tool to simplify working with Atlas [anchoring mesh](https://atlas.ripe.net/about/anchors/) measurements. It can download results concurrently, process large files in streaming without requiring a large amount of memory, and clean measurement results. It uses Facebook [Zstandard](https://facebook.github.io/zstd/) algorithm for fast data compression.

- [**Documentation**](https://github.com/SmartMonitoringSchemes/fetchmesh/raw/gh-pages/fetchmesh.pdf) — Consult the quick start guide and the documentation.
- [**Issues**](https://github.com/SmartMonitoringSchemes/fetchmesh/issues) — See what is broken / currently not working.

## :rocket: Quick Start

```bash
# Requires Python 3.7+
pip install --upgrade pip
pip install --upgrade git+ssh://git@github.com/SmartMonitoringSchemes/fetchmesh.git

fetchmesh --help
# Usage: fetchmesh [OPTIONS] COMMAND [ARGS]...
# ...
```

See the [documentation](https://github.com/SmartMonitoringSchemes/fetchmesh/raw/gh-pages/fetchmesh.pdf) for more.

*Logo: Pizza by Denis Shumaylov from the Noun Project (Creative Commons).*
