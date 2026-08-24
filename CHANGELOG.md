# Changelog

## [0.1.2](https://github.com/jmpargana/musil/compare/musil-v0.1.1...musil-v0.1.2) (2026-08-24)


### Bug Fixes

* broker config deserialization for standalone mode ([7a9244b](https://github.com/jmpargana/musil/commit/7a9244b67cafbd1b64f826fbde6f3f8f829f0656))
* integration test hangs on consumer (long-running process) ([440299b](https://github.com/jmpargana/musil/commit/440299b79b64ba6d439e808944b720fc3bbc158b))
* prefix all crate names with musil- to avoid crates.io conflicts ([6a3f40d](https://github.com/jmpargana/musil/commit/6a3f40d1492035117a27539013ce5297d2ecba70))
* remove non-existent integration test target from CI ([57ca581](https://github.com/jmpargana/musil/commit/57ca581c567c18b61d424e83255d4371ae0d86cb))
* scope miri to musil-raft only, remove continue-on-error ([014f87a](https://github.com/jmpargana/musil/commit/014f87a1ec9fff8c7ffa433a025220a7ea05e9c2))
* simplify docker-compose to single broker for integration tests ([49c471a](https://github.com/jmpargana/musil/commit/49c471a1cf71deb560cfdd7f99219900296d1e9c))
* update integration test imports and add audit.toml ([f024176](https://github.com/jmpargana/musil/commit/f0241762a2eb7f57da226ac51e9d4b4bb60d25b5))
* update README badges to working URLs ([64ccf17](https://github.com/jmpargana/musil/commit/64ccf17ee907407963b9012ecef7bb9b3e310d23))

## [0.1.1](https://github.com/jmpargana/musil/compare/musil-v0.1.0...musil-v0.1.1) (2026-08-21)


### Features

* add comprehensive CI workflow (lint, test, coverage, security) ([36bfef7](https://github.com/jmpargana/musil/commit/36bfef7c554984ffff800a9e1cd93b897dd15cb7))
* add dependabot config and README with badges ([38c40ee](https://github.com/jmpargana/musil/commit/38c40ee28681e347649e67d5e73d9606a74781f7))
* add Dockerfile (cargo-chef) and docker-compose cluster ([fc0b926](https://github.com/jmpargana/musil/commit/fc0b926043c619555bff384747024096bca0f532))
* add integration test workflow with compose cluster ([0b4e3f1](https://github.com/jmpargana/musil/commit/0b4e3f1b95bde8f8bf69d58e798fad0271544ff0))
* add metadata createtopic req/res ([92bc4cf](https://github.com/jmpargana/musil/commit/92bc4cf89dab7c9efbccd0d98c5eb4706fc9f170))
* add metadata to protocol ([8adc64d](https://github.com/jmpargana/musil/commit/8adc64def76ed416e4ae26b5b9a3f6e15ba22bc4))
* add release workflow with release-please, binary builds, Docker, and crates.io publishing ([2a39753](https://github.com/jmpargana/musil/commit/2a397532535240c67a3f543d10a5ae30e51bcb15))
* add VS Code devcontainer with full Rust toolchain ([2f288ec](https://github.com/jmpargana/musil/commit/2f288ec7264c72024c74201e2be853c77e6f4c3d))
* broker config from toml file + seeder ([de1d5f7](https://github.com/jmpargana/musil/commit/de1d5f7647cbcd70e34fb2bfad2305c5e444ff6f))
* continuous consumer ([2c814f2](https://github.com/jmpargana/musil/commit/2c814f200095d1a0ee9da797ad79e877eefc4a14))
* e2e ([9c21c4e](https://github.com/jmpargana/musil/commit/9c21c4ee2192f648c1a271eec679be12d6e6ccd0))
* error code enum ([dd644a2](https://github.com/jmpargana/musil/commit/dd644a2d0a8a0c626bae5ea8f52b5bc0a9405b67))
* full fetch flow ([c63bc01](https://github.com/jmpargana/musil/commit/c63bc015a42e89ac02c97d98fc4e253307fb6265))
* full producer flow ([27bdf24](https://github.com/jmpargana/musil/commit/27bdf247c6464c5b76bd06110b48f02721054076))
* full producer flow ([cf580fa](https://github.com/jmpargana/musil/commit/cf580fa50aeef5f5b6579987a219dc4ae76ac19a))
* implement decoder ([a376434](https://github.com/jmpargana/musil/commit/a376434b3daf175a41ba9686386eef7716a6cb2f))
* implement missing truncation ([14b9315](https://github.com/jmpargana/musil/commit/14b9315db101f1bb874d9bd9dee2bffe53a9c17e))
* implement network parser ([d9176a4](https://github.com/jmpargana/musil/commit/d9176a41b59c8ebe577e7db8bbe9e22985094129))
* implement raftlog on raftpartition ([08979f6](https://github.com/jmpargana/musil/commit/08979f6cc1bfe47989d7647067d6c781ae2c1cc0))
* improve logging ([46e19c3](https://github.com/jmpargana/musil/commit/46e19c3d16c6257fbf43ae28dc2b3505056ab151))
* introduce replica ([b4d9934](https://github.com/jmpargana/musil/commit/b4d9934927723d2af6291226e4de91bf25198459))
* metadata ([d3b8e83](https://github.com/jmpargana/musil/commit/d3b8e83af1eef9de4a40257083fa84d67097b8c8))
* partially add timeindex ([5429431](https://github.com/jmpargana/musil/commit/5429431380b4ca366e17d5baaefe24a027f8e835))
* raft algo ([52c9d9d](https://github.com/jmpargana/musil/commit/52c9d9de6e3267081ab39ded8bba5f5ec6b7a27e))
* working consumer ([3a44fd6](https://github.com/jmpargana/musil/commit/3a44fd6eb7e5d56593122693b36a7e139a42b72f))


### Bug Fixes

* add CC0-1.0 to deny.toml, version fields to path deps, remove Cargo.lock from gitignore ([2098bb8](https://github.com/jmpargana/musil/commit/2098bb8dfabdadc989ab732380ded1d0a0ee9901))
* add missing crate ([eb214b5](https://github.com/jmpargana/musil/commit/eb214b50ed7d3c03183ea37ad7a506e5f4cb7057))
* add tests and remove bugs in arctor ([561c69c](https://github.com/jmpargana/musil/commit/561c69c2c92281c46d9fb0375c60ed3ea1518d3c))
* bugs in frame encode ([f3810bb](https://github.com/jmpargana/musil/commit/f3810bb76a2a378c3f6330c5534ba5a38e41012e))
* build ([c10a63c](https://github.com/jmpargana/musil/commit/c10a63c1abeafc37e77a1d84e981fd78c3d81720))
* cleanup leo and hw from actor ([a179658](https://github.com/jmpargana/musil/commit/a179658755a4e0e51b6d6ccddb54fc26bd4d26df))
* compilable ([610cfc8](https://github.com/jmpargana/musil/commit/610cfc802d7162de51ddb6fabf838619c456393f))
* compiling ([d10976a](https://github.com/jmpargana/musil/commit/d10976ab9f042433a4d236f9f8a70d8c96f9db2d))
* conffig propagation ([3d4f456](https://github.com/jmpargana/musil/commit/3d4f456da385a0040afb3d4547a4f407eb4d8a82))
* consumer get's all topics ([574fcec](https://github.com/jmpargana/musil/commit/574fcec51080454d246ea561bc3897ae0fd61d3e))
* disable miri isolation for filesystem-using tests ([7b78104](https://github.com/jmpargana/musil/commit/7b7810446e12f399a2be26c875164c42c8365eee))
* keep storage path correct ([48dd87f](https://github.com/jmpargana/musil/commit/48dd87f9f1b51fa748f2674fb21913f2d78f16e2))
* lint ([57fcdf7](https://github.com/jmpargana/musil/commit/57fcdf75a733cb34967e8f79eedc0d6e1f550300))
* make compile ([3e3d75d](https://github.com/jmpargana/musil/commit/3e3d75d5e838cd8ada2cef04ffdd7ed308a57063))
* over segment bytes generates another segment ([c712892](https://github.com/jmpargana/musil/commit/c7128926ca5cf1f68c2c781dca262d987fcec2ee))
* partition e2e test ([3ffcdac](https://github.com/jmpargana/musil/commit/3ffcdac731abbf06afed816afcb68df2386d2818))
* producer partitioning seed ([53ad4d3](https://github.com/jmpargana/musil/commit/53ad4d35b49bb371af0c70d57366fe04e3a608b4))
* record skip pos ([80c8333](https://github.com/jmpargana/musil/commit/80c83330ffb7f5c7fd8c170e19ec2752c06624e0))
* resolve CI failures across all workflow jobs ([04e6225](https://github.com/jmpargana/musil/commit/04e62255b75fbaf254139e577635127f4319fef7))
* resolve release-please workspace version parsing failure ([91e6216](https://github.com/jmpargana/musil/commit/91e6216984dfa0ddfcd3ef59b6af6c3baedbb6fe))
* single byte representation to batch and record ([ab07ef5](https://github.com/jmpargana/musil/commit/ab07ef531afffaa3e0d0a37e65e06ff3a0305ffe))
* use PAT_TOKEN for release-please PR creation ([1785a2b](https://github.com/jmpargana/musil/commit/1785a2bf3901ae5879033a39eeaed0ffd9c5b293))
* working rcu ([7861df9](https://github.com/jmpargana/musil/commit/7861df908783a73bb23496e9005de81348ed2969))
