# Release Checklist

* [ ] Make a `release_vYYYY.MM.X` branch for your release changes.
* [ ] Update `__version__.py` to reflect the new release version.
* [ ] Create changelog for the new version. 
  * [ ] Copy `docs/changelog/changelog_dev.rst` to `docs/changelog/changelog_YYYY_MM_X.rst`.
  * [ ] Remove the example change, update version names, and release date in `changelog_YYYY_MM_X.rst`
  * [ ] Clear out the dev changelog for the next version.
  * [ ] Add a link to TOC in `docs/changelog/index.rst`
* [ ] Write a migration guide for the new version. Link from TOC.
* [ ] Build docs locally and ensure they render correctly.
* [ ] Verify integration tests are working on CI
* [ ] On Eagle
  * [ ] Download the new default singularity image.
  * [ ] Build a new environment
  * [ ] Run a test batch
* [ ] Tag release, make release in GitHub
* [ ] Update RTD to include latest release
* [ ] Build environment for release on Eagle