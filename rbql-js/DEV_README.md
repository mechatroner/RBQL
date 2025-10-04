# Publishing the package to npm
1. Update version in package.json. Make sure it is synced with unit tests js version - run the unit tests. Python and JS version don't have to be in sync!
2. Run `git clean -fd` just in case.
3. Run `npm publish`.

Note: you need to be authorize in order to publish so in the new system you might need to run `npm adduser` first,
if you run `npm publish` without authorizing it would prompt you to do it anyway, so no big deal.

