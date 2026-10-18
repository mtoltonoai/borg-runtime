#!/usr/bin/env python3
"""Fixed local host phases, invoked only through installed operator metadata.

No account/credential/cgroup setup, reset, ad-hoc migration, arbitrary command, rollback,
or uncertain phase replay. Systemd/HTTP are the explicit integration test seam.
"""
from __future__ import annotations
import argparse, copy, contextlib, fcntl, hashlib, importlib.util, json, os, re, shlex, stat, struct, sys, tarfile, time
from pathlib import Path
_COMMON_SHA = '436dd20bfb545383f27c941e48f4e131b0d60e006db0cf4d2c627e3e2cfa39f2'

def _walk_error(error):
    # An unreadable or vanished inventory is not proof that it is empty.
    raise error

def _common():
    path=Path(__file__).with_name('rollout_common_v2.py'); raw=path.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=_COMMON_SHA: raise ValueError('installed rollout common module changed')
    m=importlib.util.module_from_spec(importlib.util.spec_from_file_location('rollout_common_v2',path));exec(compile(raw,str(path),'exec'),m.__dict__);return m
c=_common()

# Only this already selected producer/policy may advance its observation clock.
# This does not admit reclamation, changed authority/materials, or deleted history.
_CLEANUP_SOURCE_SHA = 'a4c913604c5e95cdd39d0be3fd76f6b2101f6427a9c7aff44ceaada409577ab1'
_CLEANUP_SOURCE_SHAS = frozenset({_CLEANUP_SOURCE_SHA,
    'bef4283ed36a64f71f9d2e0b0efaf1e738cdd5e4814389de4c7e85bd8dd92e73'})
_CLEANUP_POLICY_SHA = 'bb5d7bad225865bb9c810b075f0162ab9eb511b5158171398ea37150a1bf7f92'
_PROCESS_SOURCE_SHA = 'c13c470c5b414e0df7793e5140095344df09f10f1f6888c0431533bfbf93a6bb'
_STOP_SOURCE_SHA = '4c44680700823fb6d0bd0ff165819ed60e03a20475b0b38857ac2449089a877d'
# Exact retained-hold producer; this adds observation, never reclamation authority.
_RETAINED_HOLD_SOURCE_SHA = '4b0e33c95ae3f0d1c37a4a8f6af8dab9838ec7661c162824d7983b857242d3ea'


# Exact package producers selected independently of the storage-format contract.
# The protected profile already pins this host module; no new profile authority.
_CODEX_HOME_SOURCES = {'src/worker/cleanup.rs': '5cbe54319ba54f53064519c72240e5ea75070cc9a5879be548fd20c4971a87b9', 'src/worker/cleanup_plugins.rs': 'c1b9a658179748c196f3b855b26e08a1528162240820015b562f8e3ce1223be0', 'src/worker/cleanup_copyout.rs': '5ff2196a5604a821f0b7352b3ee07dfd9b4823e1750c419f394fa221d0af2a81', 'src/worker/git_cleanup.rs': 'c291bc2926daf930e66522e52ea6c0f53199269beced4bce1124f26c08e9fd8c', 'src/worker/mod.rs': '421adaad9edbd099e62396fc97a2ee9576a9834400496b6d6404d88825ba2de5', 'src/worker/process.rs': 'c13c470c5b414e0df7793e5140095344df09f10f1f6888c0431533bfbf93a6bb', 'src/worker/publication.rs': 'aeb43aed056d217cd7ef3b99860abbc79a23f556b546247230a6af0805785cdf', 'src/worker/pull_request.rs': 'a12c849d0e7bed582d88e565484646558ab837c317397abf5026dd73ecc90dec', 'src/worker/codex.rs': '2c0a8e3e7c28f244026031ded24004a7a68fbc4e402cee6b49e34777ce13b277', 'src/worker/acceptance.rs': 'edb4a355552c0caeadd5226c9db006649b31e7857eae993041b0831679af1465', 'src/worker/codex_mcp.rs': 'facb4509da923c9d47512e0966c52e793ca0cc4660fb342678ed5f00a3ba26e8', 'src/worker/collaboration_notice.rs': 'b6924a898af2dc8990512421819f5d58f1b1cee7eea7d00d899455fa3e3fdc19', 'src/worker/handoff.rs': '6b5b8defcfe68cad4ea8597f5a0e696885dba6b275099dcb617a3fc6a7c2f314'}
# Exact supplied-turn producer/readers; an alternative complete family,
# never a partial union with the historical wire20 package above.
_SUPPLIED_CODEX_HOME_SOURCES = {'src/charter.rs': '199b421f562aa86f95a23af8e1443fc68a13cfc2ecbce9e83ae13e601f5eb5c1', 'src/controller/application_schema.rs': 'b8eec42f51a06f77d6919da40bf61f196a556337bdc5d6a01f386eda4ed5a398', 'src/controller/charter_outputs.rs': '625a9e009fe8006533bacae95d7c91b3ef033a9c0a2e5ae1842f2ea8bd727d72', 'src/controller/charters.rs': '6dffda29905607fabab610f0c260cbed81c49e0cd9b90e4a5b7d9989d39a95a0', 'src/controller/charters_supplied.rs': 'd2e356b9dbed699317e983bc289964ef209235c735aeec5256c9c4cb5c3b0f88', 'src/controller/charters_supplied_turns.rs': '2972b55d35c64f300c386865265a2f235a39a5733dea1cd688b522b9601bdcda', 'src/controller/database.rs': 'eeb1deba93021922be02d375876bea2fd4fa6579d255613c82ab42ac840bcb9c', 'src/protocol.rs': '805215abf436e3a039c548154bd977b759977cadb6cbe9d824523881faf84102', 'src/worker/acceptance.rs': 'edb4a355552c0caeadd5226c9db006649b31e7857eae993041b0831679af1465', 'src/worker/charter.rs': '40e95694112d6f04fe7b7d5ba0e7641269e094e6219de7e2f0a0a3d6df3d0b01', 'src/worker/charter_output.rs': '1a02200ca2107753dda0e555eaf0c42ff0257201c9f024f63c6fbc64f75bb4fb', 'src/worker/cleanup.rs': '5cbe54319ba54f53064519c72240e5ea75070cc9a5879be548fd20c4971a87b9', 'src/worker/cleanup_copyout.rs': '5ff2196a5604a821f0b7352b3ee07dfd9b4823e1750c419f394fa221d0af2a81', 'src/worker/cleanup_plugins.rs': 'c1b9a658179748c196f3b855b26e08a1528162240820015b562f8e3ce1223be0', 'src/worker/codex.rs': '37f52a3ea220f70108a3b979cd9bfcb4b92ddace077beb1c2dec91a00eb95e59', 'src/worker/codex_charter.rs': '81c409316aa2cb4b651942c51688bd6c1719588ababed672d0f5b6e7dbef6e58', 'src/worker/codex_charter_turns.rs': 'db9798de3c855716f3b6d65df0df2c66ec9dadf3c75e700b4d548aefc85c43c3', 'src/worker/codex_mcp.rs': 'cfaf4f47e885b3d617336d035e247210a0390d7a0c49ad3e280852f8e3a95f63', 'src/worker/collaboration_notice.rs': 'b6924a898af2dc8990512421819f5d58f1b1cee7eea7d00d899455fa3e3fdc19', 'src/worker/git_cleanup.rs': 'c291bc2926daf930e66522e52ea6c0f53199269beced4bce1124f26c08e9fd8c', 'src/worker/handoff.rs': 'c55ecfcc7563ec59e0983d9866c8e22f227415b9aa36c83b05b4279bbc1aa90e', 'src/worker/mod.rs': '37b5d09db037ad29742913c6d2e160ce8f0700b051fc14a353a44e589095b053', 'src/worker/process.rs': 'c13c470c5b414e0df7793e5140095344df09f10f1f6888c0431533bfbf93a6bb', 'src/worker/publication.rs': 'aeb43aed056d217cd7ef3b99860abbc79a23f556b546247230a6af0805785cdf', 'src/worker/pull_request.rs': 'a12c849d0e7bed582d88e565484646558ab837c317397abf5026dd73ecc90dec'}

# Complete independent directory-successor family; historical maps stay exact.
_DIRECTORY_CODEX_HOME_SOURCES = {'src/charter.rs': '199b421f562aa86f95a23af8e1443fc68a13cfc2ecbce9e83ae13e601f5eb5c1', 'src/controller/agents.rs': '7b80cf5df903ec13306ab5781bae2f8cd6726000f5690900ce8c837975d0ea6c', 'src/controller/application_schema.rs': '02ddbdaf24ef2c105e7f072b3a38dbba1646eba67e627d646f4ab0241737c76e', 'src/controller/charter_outputs.rs': '625a9e009fe8006533bacae95d7c91b3ef033a9c0a2e5ae1842f2ea8bd727d72', 'src/controller/charters.rs': '6dffda29905607fabab610f0c260cbed81c49e0cd9b90e4a5b7d9989d39a95a0', 'src/controller/charters_supplied.rs': 'd2e356b9dbed699317e983bc289964ef209235c735aeec5256c9c4cb5c3b0f88', 'src/controller/charters_supplied_turns.rs': '2972b55d35c64f300c386865265a2f235a39a5733dea1cd688b522b9601bdcda', 'src/controller/database.rs': '142edaa1bcc562b44817290f743e347e9e5cdb08a562b91e3b16ab503fcdf2c2', 'src/controller/schema23-from22.sql': '95e229e4f9140322e176f7fdec8cb05a010634c0462ed7bb97643025519a2634', 'src/protocol.rs': '79c2924e4565af1407bc19cb645600458e1f3133548c05a4ef1b6db9bd2006ba', 'src/worker/acceptance.rs': 'edb4a355552c0caeadd5226c9db006649b31e7857eae993041b0831679af1465', 'src/worker/charter.rs': '40e95694112d6f04fe7b7d5ba0e7641269e094e6219de7e2f0a0a3d6df3d0b01', 'src/worker/charter_output.rs': '1a02200ca2107753dda0e555eaf0c42ff0257201c9f024f63c6fbc64f75bb4fb', 'src/worker/cleanup.rs': '5cbe54319ba54f53064519c72240e5ea75070cc9a5879be548fd20c4971a87b9', 'src/worker/cleanup_copyout.rs': '5ff2196a5604a821f0b7352b3ee07dfd9b4823e1750c419f394fa221d0af2a81', 'src/worker/cleanup_plugins.rs': 'c1b9a658179748c196f3b855b26e08a1528162240820015b562f8e3ce1223be0', 'src/worker/codex.rs': '37f52a3ea220f70108a3b979cd9bfcb4b92ddace077beb1c2dec91a00eb95e59', 'src/worker/codex_charter.rs': '81c409316aa2cb4b651942c51688bd6c1719588ababed672d0f5b6e7dbef6e58', 'src/worker/codex_charter_turns.rs': 'db9798de3c855716f3b6d65df0df2c66ec9dadf3c75e700b4d548aefc85c43c3', 'src/worker/codex_mcp.rs': 'cfaf4f47e885b3d617336d035e247210a0390d7a0c49ad3e280852f8e3a95f63', 'src/worker/collaboration_notice.rs': 'b6924a898af2dc8990512421819f5d58f1b1cee7eea7d00d899455fa3e3fdc19', 'src/worker/git_cleanup.rs': 'c291bc2926daf930e66522e52ea6c0f53199269beced4bce1124f26c08e9fd8c', 'src/worker/handoff.rs': 'c55ecfcc7563ec59e0983d9866c8e22f227415b9aa36c83b05b4279bbc1aa90e', 'src/worker/mod.rs': '37b5d09db037ad29742913c6d2e160ce8f0700b051fc14a353a44e589095b053', 'src/worker/process.rs': 'c13c470c5b414e0df7793e5140095344df09f10f1f6888c0431533bfbf93a6bb', 'src/worker/publication.rs': 'aeb43aed056d217cd7ef3b99860abbc79a23f556b546247230a6af0805785cdf', 'src/worker/pull_request.rs': 'a12c849d0e7bed582d88e565484646558ab837c317397abf5026dd73ecc90dec'}

# Complete task-assignment producer family, separate from historical packages.
# Effective metadata remains independently pinned by the protected profile.
_DEFAULT_ASSIGNMENT_CODEX_HOME_SOURCES = {'src/charter.rs': '199b421f562aa86f95a23af8e1443fc68a13cfc2ecbce9e83ae13e601f5eb5c1', 'src/controller/agents.rs': '9f3e5a1aec00c834cecf8337a0606f459394f1194e6d0d258dd095a1e2459bd3', 'src/controller/application_schema.rs': '9bfc1aa5d84ff929da87fd3a8c896c663ea5e6db87f1f4cf9130f6a94a837c34', 'src/controller/charter_outputs.rs': '625a9e009fe8006533bacae95d7c91b3ef033a9c0a2e5ae1842f2ea8bd727d72', 'src/controller/charters.rs': '0ac014f950797cdb8b727809377f831ed8c943cd3744d025e7dbdcffcaaaca5c', 'src/controller/charters_supplied.rs': 'cb1b3b6bd1acd783a81fa447770adbb997539495b7d00788cd495bb85cad14a1', 'src/controller/charters_supplied_turns.rs': '2972b55d35c64f300c386865265a2f235a39a5733dea1cd688b522b9601bdcda', 'src/controller/database.rs': '276aad70f8606c6c8f24ad4becad691967db2a8e7a1e7c5e4d54d4ba494cae6b', 'src/controller/operations.rs': '14e6e9bc9f982258aeb5d49a7860c7ffb8a46f593fcb05782ca0157e3948e2d4', 'src/controller/operations/reports.rs': '01d9584f3aa0fe08af662440560096f164c6e91c1ca3395c8c471bf6362a475c', 'src/controller/schema23-from22.sql': '95e229e4f9140322e176f7fdec8cb05a010634c0462ed7bb97643025519a2634', 'src/controller/schema24-from23.sql': 'f2318a08f58659caa2239149e7e051c63e41219a8f483f1ed5e75383f278ef7c', 'src/controller/sessions.rs': '540c0624425b2602dd8f1ecdae6ba3352c3bf6af798936dff492b16eeba0a89e', 'src/controller/task_handoffs.rs': '013aaa6c4c1868fcba67b9a2b19cd505d12dc78a7c3241f98854aade178c0176', 'src/controller/task_submission.rs': '742e36501ab4fe7069bb3322290653bc42816aadfa3f945137d44bf38a163592', 'src/delegation.rs': 'd8c7e1f49f9abf98ca57f3f7a5ecf09c0bcf54fd908345436b5c541450188ebc', 'src/limits/mod.rs': '143822bcae39cb58fcef6bd02892e59bcc2fcec3963f53612b4bd2b382f443c5', 'src/mcp/mod.rs': 'e9125bdf9b808d001a7f49c909a0831b785e6362cd8ea45a533d777395987c57', 'src/native/loop_step.rs': 'd3a3d6372606b02bcd1cbec279d5d166102deb88a4b9aeae9b493d27914ef1c4', 'src/protocol.rs': '8ac0c2fe1a18781886cc283e5972f1356670a526769a67969bb0699262dffe3c', 'src/worker/acceptance.rs': 'edb4a355552c0caeadd5226c9db006649b31e7857eae993041b0831679af1465', 'src/worker/charter.rs': '700374cb2f81b3f1c17ffa37d3f8cb7bf3915bf9569843354f6c13d794320516', 'src/worker/charter_output.rs': 'adc92523746077c3db328b71b5b8208a0bb49bda3901fba73fd3b6ff044a1b40', 'src/worker/cleanup.rs': '5cbe54319ba54f53064519c72240e5ea75070cc9a5879be548fd20c4971a87b9', 'src/worker/cleanup_copyout.rs': '5ff2196a5604a821f0b7352b3ee07dfd9b4823e1750c419f394fa221d0af2a81', 'src/worker/cleanup_plugins.rs': 'c1b9a658179748c196f3b855b26e08a1528162240820015b562f8e3ce1223be0', 'src/worker/codex.rs': '37f52a3ea220f70108a3b979cd9bfcb4b92ddace077beb1c2dec91a00eb95e59', 'src/worker/codex_charter.rs': '81c409316aa2cb4b651942c51688bd6c1719588ababed672d0f5b6e7dbef6e58', 'src/worker/codex_charter_turns.rs': 'db9798de3c855716f3b6d65df0df2c66ec9dadf3c75e700b4d548aefc85c43c3', 'src/worker/codex_mcp.rs': '0a8f02e28ed1350a27f13fbe61e0cdea5380503916ab091bea9ef0fa6eb9e957', 'src/worker/collaboration_notice.rs': 'b6924a898af2dc8990512421819f5d58f1b1cee7eea7d00d899455fa3e3fdc19', 'src/worker/git_cleanup.rs': 'c291bc2926daf930e66522e52ea6c0f53199269beced4bce1124f26c08e9fd8c', 'src/worker/handoff.rs': 'a82ffdd531d8b5c27698722bbc329e00a26df87b5a561992c00b2cfebdbe96eb', 'src/worker/mod.rs': 'cdc4900fd348d3a7c19a9fdc7abb5cb3e1d9e8537843bee95e627bd4da868b21', 'src/worker/native_loop.rs': 'c0606578bd77a525e7fe8431da79522c765401f265db511a667f8df66688f8a5', 'src/worker/process.rs': 'c13c470c5b414e0df7793e5140095344df09f10f1f6888c0431533bfbf93a6bb', 'src/worker/publication.rs': '972cda207bf324f5393dbee390b559089ad8798a6c920e00457e09a2dc0e7dc9', 'src/worker/pull_request.rs': 'f0308a668abbf229851cf0a454570371a3315a32a55a3e60bed8b145eff2ac1b'}
# One complete composed producer/reader family: wire24/schema25/worker19.
# Historical families above retain their original pins. No per-file union or
# forward-format permission is derived from this cleanup-readback selection.
_COMPOSED_CODEX_HOME_FORMATS = {'wire':24,'controller_schema':25,'worker_format':19}
_COMPOSED_CODEX_HOME_SOURCES = {'src/charter.rs': '199b421f562aa86f95a23af8e1443fc68a13cfc2ecbce9e83ae13e601f5eb5c1', 'src/controller/agents.rs': '2a0fe3acbc7eaf188623eb56e45a13f041e6c95d6b5d8320a99870169a87459e', 'src/controller/application_schema.rs': '64b349c9d28839bf843841f55ebff03601a5eb6185fa32e4ff23bf90a45a5416', 'src/controller/charter_outputs.rs': '625a9e009fe8006533bacae95d7c91b3ef033a9c0a2e5ae1842f2ea8bd727d72', 'src/controller/charters.rs': 'dd310eadd153049ab1a90765cb37a23fdccfb134325c1d86263aedc5200c263f', 'src/controller/charters_supplied.rs': 'cb1b3b6bd1acd783a81fa447770adbb997539495b7d00788cd495bb85cad14a1', 'src/controller/charters_supplied_turns.rs': '116f76f7163e667276e29dc66dbb8f8dbde25da8e856f5ef3059f5ef01aa1135', 'src/controller/database.rs': '2faa5c0d449a52bf944668db6423b7dc7f87bca307a0f0cbd3b65b3b0c28b919', 'src/controller/identity_administration.rs': 'ba0b287a140fdf07e3ca37d7287f30f39717360aab36abb160a9a557ae579289', 'src/controller/machine_resources.rs': 'dfe38310e7b21d3a1e5bdeb05e3fac086d210b743b90c5dbfa8e43f4d0a54663', 'src/controller/mod.rs': '7dd6400fbfc9d9821eed52d321fc5eb872d89c11956c89f1c49afb3848f0bfba', 'src/controller/operations.rs': '2ed6b2ba2755cdb010952490f83003fc6d1c7fb7e8dec1da427cfbda12b38c3e', 'src/controller/operations/reports.rs': 'be8cf79f44ee2831398d74e0b542e40ab997220552f3259c5af5bffa1bf401ef', 'src/controller/operations/supplied_outer.rs': '0e1b828463a3a10b7780cf88629dbc82ab653304159a8d847baedf7bfb28cda5', 'src/controller/schema23-from22.sql': '95e229e4f9140322e176f7fdec8cb05a010634c0462ed7bb97643025519a2634', 'src/controller/schema24-from23.sql': 'f2318a08f58659caa2239149e7e051c63e41219a8f483f1ed5e75383f278ef7c', 'src/controller/sessions.rs': '540c0624425b2602dd8f1ecdae6ba3352c3bf6af798936dff492b16eeba0a89e', 'src/controller/task_creation.rs': '692580e630fdeb3de80bb78d1b539b120bcede67d9bc3dbafcce3094158df648', 'src/controller/task_handoffs.rs': '46423fb94cb1eb7f2a88682a551fad513e0f46b2c799c2a02f8e37133b36c7f9', 'src/controller/task_submission.rs': '742e36501ab4fe7069bb3322290653bc42816aadfa3f945137d44bf38a163592', 'src/delegation.rs': 'd8c7e1f49f9abf98ca57f3f7a5ecf09c0bcf54fd908345436b5c541450188ebc', 'src/lib.rs': 'f94c8e6892fe762de03b64f4a02973ef78c22c8702e426d0b1d68ed19148b8a2', 'src/limits/identity.rs': 'eb5b55eda454c3bd98a81b3c962428c0c02bc650042f7e288e0cd04112292293', 'src/limits/mod.rs': '1785dc591b79a85ded3e1f45e3bbebbad97740d5f105c33626de9ab503afbf71', 'src/limits/worker.rs': '6b727ae7279c58fcfc67e7cc43d5990c463104f982943189b8900a1b89fa8b94', 'src/machine_resources.rs': 'b98c54d7e79b87a54d0e4e4ce4367c047fe2779efabbbf107fa9d3fbc2a1887e', 'src/mcp/identity_administration.rs': '9c9aa2daae61cbcbb208e761310774059393a950d102c387f96d472eb0bdb347', 'src/mcp/mod.rs': '25fefff1d0e668ea724ed4708877440b02e16691c47b38fd9c672cd7f12a8310', 'src/native/loop_step.rs': 'b3e944f86359f41bc0959c8a689ea3dbbf251ed93ac1f9a23f82f29e2043695f', 'src/protocol.rs': 'f7a9ce3dc6a922d076f82e7926e66852e98a472afdcba2c40fbcae518d962173', 'src/protocol_identity.rs': '7a1fbda7127bb73950110d0c5bbdce5bc5394857bbc21846779cf0eb37af3281', 'src/supplied_outer.rs': '7844aab69b622ffcabe6693930248ec0c94b6fc5c022f151a55e2230bfbf3e91', 'src/worker/acceptance.rs': 'edb4a355552c0caeadd5226c9db006649b31e7857eae993041b0831679af1465', 'src/worker/charter.rs': '700374cb2f81b3f1c17ffa37d3f8cb7bf3915bf9569843354f6c13d794320516', 'src/worker/charter_output.rs': '77ee76cbb97a69fdd08b510ff3b9404afc64caa02a9fd6db350eba7a4403635b', 'src/worker/cleanup.rs': '4b0e33c95ae3f0d1c37a4a8f6af8dab9838ec7661c162824d7983b857242d3ea', 'src/worker/cleanup_copyout.rs': 'd1925748bfcf7bbc5415f8e0c1d1bbd05c86681d62afdb5b2d8efa1581648b69', 'src/worker/cleanup_plugins.rs': 'c1b9a658179748c196f3b855b26e08a1528162240820015b562f8e3ce1223be0', 'src/worker/codex.rs': 'd7e0e6923561c698c2ef68c8b57838f40430cbd802a57eb03d23df03f2fee0e5', 'src/worker/codex_charter.rs': '81c409316aa2cb4b651942c51688bd6c1719588ababed672d0f5b6e7dbef6e58', 'src/worker/codex_charter_turns.rs': 'db9798de3c855716f3b6d65df0df2c66ec9dadf3c75e700b4d548aefc85c43c3', 'src/worker/codex_mcp.rs': '935fc27ecbab0ef1399dc58ab4826f1d8ae79ae3842f1cf4bff9ff136e7e9ff1', 'src/worker/codex_retry.rs': 'ca3e0f8d26a57d6ab8a073b9b90573f781ee521db886446ccd6935820847e5a0', 'src/worker/collaboration_notice.rs': 'b6924a898af2dc8990512421819f5d58f1b1cee7eea7d00d899455fa3e3fdc19', 'src/worker/completion.rs': '9fde728db6228fe7377137c817390c224d4782ae4c99e070f8eb1cb9a72159a5', 'src/worker/git_cleanup.rs': 'c291bc2926daf930e66522e52ea6c0f53199269beced4bce1124f26c08e9fd8c', 'src/worker/handoff.rs': '2d7be98666db4fe56debc5d91e90553050dbe195f4b6ec2a4592cdf88e505c75', 'src/worker/machine_resources.rs': '7e34c7ec3bc87298a88cb42b216584cb16b70e82a14c96b1786e9048aea45775', 'src/worker/mod.rs': 'bced916dd3dbb0572f4955d0f65641f491b90ab80cf2ac84f76d89fe68da621a', 'src/worker/native_loop.rs': '78108dbb1d06b545d9684d35fff6e7d3e2ebaa237170a44d3abcdc2caf360657', 'src/worker/process.rs': 'c13c470c5b414e0df7793e5140095344df09f10f1f6888c0431533bfbf93a6bb', 'src/worker/publication.rs': '972cda207bf324f5393dbee390b559089ad8798a6c920e00457e09a2dc0e7dc9', 'src/worker/pull_request.rs': 'f0308a668abbf229851cf0a454570371a3315a32a55a3e60bed8b145eff2ac1b'}
_CODEX_JOURNALS = frozenset(("borg-thread.json", "borg-supplied-outer.json", "borg-usage.json", "borg-collaboration-config.json", "collaboration-codex", "collaboration-notices", "memory-notices", "supplied-charter", "completion"))
_CODEX_TRANSCRIPTS = ("sessions", "archived_sessions", "history.jsonl")
# Exact recovered worker20 writer family; historical families remain separate.
_RECOVERED_CODEX_HOME_FORMATS = {'wire':25,'controller_schema':26,'worker_format':20}
_RECOVERED_CODEX_HOME_SOURCES = {
    'src/charter.rs': '199b421f562aa86f95a23af8e1443fc68a13cfc2ecbce9e83ae13e601f5eb5c1',
    'src/controller/agents.rs': 'd3c607b7c194ae4892c343e11e48293b3e09b39f5a00d0cfe9e4d3ab98d6a04e',
    'src/controller/application_schema.rs': 'f177e8ca7839e0f9e5130ee0a50ebe7eee78d30c7979e8d930dc9fb10ab7f783',
    'src/controller/charter_outputs.rs': '625a9e009fe8006533bacae95d7c91b3ef033a9c0a2e5ae1842f2ea8bd727d72',
    'src/controller/charters.rs': '5e247a63e6e7041b9ce447ae3869ec46d513da926d606af0434ff4e5491299f4',
    'src/controller/charters_supplied.rs': 'cb1b3b6bd1acd783a81fa447770adbb997539495b7d00788cd495bb85cad14a1',
    'src/controller/charters_supplied_turns.rs': '116f76f7163e667276e29dc66dbb8f8dbde25da8e856f5ef3059f5ef01aa1135',
    'src/controller/database.rs': '7a9d59f9b32775d75f754b192fbbd0853ac28da1970b9d42a9f8c849fc3dfb40',
    'src/controller/identity_administration.rs': '4de92f2b656b360d9465604744e42c2a380a80e3596eba5db50df6cd2c37b254',
    'src/controller/identity_limit_fields.rs': '1b72fb2f79badd20eed9165b7deeddeb5b8451a93f69975e75a02ab5269ea1b2',
    'src/controller/machine_resources.rs': 'cd2617a4b80d732d4ccbd876dc39761db69046e17c15952d4f9d01cf9cd3d1cd',
    'src/controller/mod.rs': 'fd78de77cc2b7689fabfc2a73bd189c06a930651f6de264c6592dbf2c1d2ca96',
    'src/controller/operations.rs': 'f990f50ba053c38d962af18c332a300de836a442a3c00f9439e7cd8629665127',
    'src/controller/operations/reports.rs': '299c8630b98b053a6e50c33a313e4e78026d3cbc760326943769446869c909c7',
    'src/controller/operations/supplied_outer.rs': 'c51a074820b443b263bc93721c2f6a9fdbe329fb4bcae14e9a767e53d79a87b5',
    'src/controller/read_pages.rs': 'fa0c5bc3adb04a1a60150241e5240f8cf61986048d397030e215b5b31a8d715b',
    'src/controller/read_pagination.rs': '91e6260718f0173b452b3cbe730b6c6bfc68bfeaf01812e19d8ddacedb088956',
    'src/controller/read_retention.rs': '0def3401d029f1b14eab602b3d3f6520862da8ddb7a4022967030fce0d1b72f7',
    'src/controller/schema23-from22.sql': '95e229e4f9140322e176f7fdec8cb05a010634c0462ed7bb97643025519a2634',
    'src/controller/schema24-from23.sql': 'f2318a08f58659caa2239149e7e051c63e41219a8f483f1ed5e75383f278ef7c',
    'src/controller/sessions.rs': '540c0624425b2602dd8f1ecdae6ba3352c3bf6af798936dff492b16eeba0a89e',
    'src/controller/task_creation.rs': '692580e630fdeb3de80bb78d1b539b120bcede67d9bc3dbafcce3094158df648',
    'src/controller/task_handoffs.rs': '682ba3b2a2ba95639387628ef8d7a5c8ac13c294b0bbfb37d224727e6bffdd20',
    'src/controller/task_submission.rs': 'ad337c3ef6c8cfd2ab6f6e8f6a6196ab9f117645883431d34250a396208a85d2',
    'src/delegation.rs': 'd8c7e1f49f9abf98ca57f3f7a5ecf09c0bcf54fd908345436b5c541450188ebc',
    'src/lib.rs': 'b9eeb9c686c1531c04b186ad153193f06e5b189c6589afa3f30d258e48bba60e',
    'src/limits/identity.rs': 'eb5b55eda454c3bd98a81b3c962428c0c02bc650042f7e288e0cd04112292293',
    'src/limits/mod.rs': '1785dc591b79a85ded3e1f45e3bbebbad97740d5f105c33626de9ab503afbf71',
    'src/limits/worker.rs': '6b727ae7279c58fcfc67e7cc43d5990c463104f982943189b8900a1b89fa8b94',
    'src/machine_resources.rs': 'b98c54d7e79b87a54d0e4e4ce4367c047fe2779efabbbf107fa9d3fbc2a1887e',
    'src/mcp/identity_administration.rs': '9c9aa2daae61cbcbb208e761310774059393a950d102c387f96d472eb0bdb347',
    'src/mcp/mod.rs': '734eb4884aacd34acfb0ea14817d92a9241d11e59c59e4a200529cb2ab4f4d1e',
    'src/mcp/read_recovery.rs': '3856ef88f87e04e097b51679eec30d6e6b1c6f89bd704e622cd47dde375b290f',
    'src/native/loop_step.rs': 'b3e944f86359f41bc0959c8a689ea3dbbf251ed93ac1f9a23f82f29e2043695f',
    'src/protocol.rs': '9a04ea9917f388f10f055c1a341872f632da8626abf09092b5182e5edcd58fb6',
    'src/protocol_identity.rs': '7a1fbda7127bb73950110d0c5bbdce5bc5394857bbc21846779cf0eb37af3281',
    'src/protocol_identity_limit_fields.rs': '3d362ab19342ddae26005024230f25646b60df3ed611b9cb7dc806051796d9ed',
    'src/protocol_read_pagination.rs': 'f467d2875373d2643483ff1580d7726da066ca73055115eb84bd36c0510be91a',
    'src/read_page.rs': 'd386b2f8a310788e2bfe116878f47892382b5360ea89ca5a713754430c6e9a54',
    'src/read_retention.rs': 'c1a069d41f3ff8d133b7929680d2b3f094e4d07a4331e6784427c64d9cc6a811',
    'src/supplied_outer.rs': '7844aab69b622ffcabe6693930248ec0c94b6fc5c022f151a55e2230bfbf3e91',
    'src/worker/acceptance.rs': 'edb4a355552c0caeadd5226c9db006649b31e7857eae993041b0831679af1465',
    'src/worker/charter.rs': '700374cb2f81b3f1c17ffa37d3f8cb7bf3915bf9569843354f6c13d794320516',
    'src/worker/charter_output.rs': 'b4231b3e24e7933a1e142a187bf6f07ff6d316889aaf2b14354bec2d6fef1900',
    'src/worker/cleanup.rs': '54f15913412870da6984ade05477d09004a28ab07cece96931759d2381bba898',
    'src/worker/cleanup_copyout.rs': '4181eeac29c4869d07fa11ca282e5963259a566240e0cc389d4e4ef2fc0aaa80',
    'src/worker/cleanup_plugins.rs': '247e6241889e1f58d52172f20ee4359a9f9804af73171e8e42c5e3a7c07a42cd',
    'src/worker/cleanup_reads.rs': '54940748108c99b7f496ca49f81c21fb5f8eca5f957d1b9d71d50edfc527bdf0',
    'src/worker/codex.rs': 'd7e0e6923561c698c2ef68c8b57838f40430cbd802a57eb03d23df03f2fee0e5',
    'src/worker/codex_charter.rs': '81c409316aa2cb4b651942c51688bd6c1719588ababed672d0f5b6e7dbef6e58',
    'src/worker/codex_charter_turns.rs': 'db9798de3c855716f3b6d65df0df2c66ec9dadf3c75e700b4d548aefc85c43c3',
    'src/worker/codex_mcp.rs': 'cadc88711a330d24b6b2257b6ee1ac8b32b004551700ec3d0a65348d7701a532',
    'src/worker/codex_retry.rs': 'ca3e0f8d26a57d6ab8a073b9b90573f781ee521db886446ccd6935820847e5a0',
    'src/worker/collaboration_notice.rs': 'b6924a898af2dc8990512421819f5d58f1b1cee7eea7d00d899455fa3e3fdc19',
    'src/worker/completion.rs': '9fde728db6228fe7377137c817390c224d4782ae4c99e070f8eb1cb9a72159a5',
    'src/worker/git_cleanup.rs': 'c291bc2926daf930e66522e52ea6c0f53199269beced4bce1124f26c08e9fd8c',
    'src/worker/handoff.rs': 'd9c2feda72c95868aec915fd19842b918d58272e0e479e5931d9cfa048dbe60e',
    'src/worker/identity_limit_fields.rs': '898745126398665a1a1885d4670e2e214533fa74996e2f7bd5fd1d87b949552c',
    'src/worker/machine_resources.rs': '7e34c7ec3bc87298a88cb42b216584cb16b70e82a14c96b1786e9048aea45775',
    'src/worker/mod.rs': '847f3a0fbff683524798034bf019a2a47452366ecab97367d0469ce68e00cfbf',
    'src/worker/mutations.rs': 'f3d1c510541f125d531d908624cdad4238fa094e8abe3c8dbbdc7f59b0fd3486',
    'src/worker/native_loop.rs': '78108dbb1d06b545d9684d35fff6e7d3e2ebaa237170a44d3abcdc2caf360657',
    'src/worker/process.rs': 'c13c470c5b414e0df7793e5140095344df09f10f1f6888c0431533bfbf93a6bb',
    'src/worker/publication.rs': '972cda207bf324f5393dbee390b559089ad8798a6c920e00457e09a2dc0e7dc9',
    'src/worker/pull_request.rs': 'f0308a668abbf229851cf0a454570371a3315a32a55a3e60bed8b145eff2ac1b',
    'src/worker/read_pagination.rs': 'fa638c1d13b8d1c4585b221711ccf6602be9da6b2f4697f8cf81172f17735c6c',
    'src/worker/read_retention.rs': '1c54c8a127c88859d9cf799884cc9c42e5f303a9e3edfd648c8c8888948ca381',
}



def cleanup_record(raw, name, selected):
    value=c.strict(raw)
    c.fields(value,'format_revision command_id attempt_id policy policy_revision status checkpoint first_safe_ms last_observed_ms next_due_ms disposition reason worktrees cache cache_started cache_safe_ms cache_done','cleanup record')
    def number(n):
        if type(n) is not int or not 0<=n<=2**64-1:raise ValueError('cleanup unsigned value')
    def text(s):
        if not isinstance(s,str) or not s or any(ord(x)<32 or ord(x)==127 for x in s):raise ValueError('cleanup text')
    def identity(v):
        c.fields(v,'device inode','cleanup directory identity')
        for n in v.values():number(n)
    def registration(v):
        c.fields(v,'repository_id registration_revision','cleanup registration')
        for s in v.values():text(s)
    if type(value['format_revision']) is not int or value['format_revision']!=1:raise ValueError('cleanup format differs')
    for key in ('command_id','attempt_id','policy_revision'):text(value[key])
    if name!='cleanup/'+value['attempt_id'].encode().hex()+'.json':raise ValueError('cleanup path/attempt differs')
    if c.canonical(value['policy'])!=c.canonical(selected['retention']['worker']) or value['policy_revision']!=selected['revision']:raise ValueError('cleanup selected policy differs')
    if value['disposition'] not in ('retained','removing','reclaimed','uncertain'):raise ValueError('cleanup disposition')
    if value['reason'] not in ('startup','active','local_recovery','canonical_unavailable','canonical_binding','not_terminal','not_stopped','uncertain_effect','checkpoint','grace','clock','source_or_path','filesystem','complete','cache_dependency','cache_grace'):raise ValueError('cleanup reason')
    if value['status'] not in (None,'succeeded','failed','cancelled'):raise ValueError('cleanup terminal status')
    for key in ('first_safe_ms','next_due_ms','cache_safe_ms'):
        if value[key] is not None:number(value[key])
    number(value['last_observed_ms'])
    for key in ('cache_started','cache_done'):
        if type(value[key]) is not bool:raise ValueError('cleanup cache flag')
    if value['checkpoint'] is not None:
        checkpoint=value['checkpoint'];c.fields(checkpoint,'registration repository commit remote_ref operation_id','cleanup checkpoint')
        registration(checkpoint['registration'])
        for key in ('repository','commit','remote_ref','operation_id'):text(checkpoint[key])
    if not isinstance(value['worktrees'],list) or len(value['worktrees'])!=2:raise ValueError('cleanup worktree slots')
    for slot in value['worktrees']:
        c.fields(slot,'proof started done','cleanup worktree slot')
        if type(slot['started']) is not bool or type(slot['done']) is not bool:raise ValueError('cleanup worktree flag')
        proof=slot['proof']
        if proof is not None:
            c.fields(proof,'path root directory cache head discard_source','cleanup worktree proof')
            for key in ('path','head'):text(proof[key])
            for key in ('root','directory','cache'):identity(proof[key])
            if type(proof['discard_source']) is not bool:raise ValueError('cleanup discard flag')
        elif slot['started']:raise ValueError('cleanup started without proof')
    if value['cache'] is not None:
        proof=value['cache'];c.fields(proof,'identity root directory last_use_ms last_use_revision','cleanup cache proof')
        for key in ('root','directory'):identity(proof[key])
        for key in ('last_use_ms','last_use_revision'):number(proof[key])
        ci=proof['identity'];c.fields(ci,'version registration source_endpoint checkpoint_endpoint','cleanup cache identity')
        number(ci['version']);registration(ci['registration'])
        for key in ('source_endpoint','checkpoint_endpoint'):text(ci[key])
    elif value['cache_started']:raise ValueError('cleanup cache started without proof')
    return value


def cleanup_clocks(original,observed,*,retained_holds=False):
    before=dict(original);after=dict(observed)
    start=before.pop('last_observed_ms');end=after.pop('last_observed_ms')
    old_due=before.pop('next_due_ms');new_due=after.pop('next_due_ms')
    if c.canonical(before)!=c.canonical(after) or end<start:raise ValueError('cleanup change exceeds observation clocks')
    def add(a,b):
        if type(a) is not int or type(b) is not int or not 0<=a<=2**64-1-b:raise ValueError('cleanup clock overflow')
        return a+b
    if before['reason']=='grace':
        status=before['status'];key={'succeeded':'success_grace_ms','failed':'failed_grace_ms','cancelled':'cancelled_grace_ms'}.get(status)
        if key is None:raise ValueError('cleanup grace status')
        due=add(before['first_safe_ms'],before['policy'][key])
        if old_due!=due or new_due!=due or end>=due:raise ValueError('cleanup grace deadline differs')
    elif before['reason']=='local_recovery':
        if before['cache_safe_ms'] is not None or before['first_safe_ms'] is not None:raise ValueError('cleanup retry safe interval differs')
        delay=before['policy']['retry_delay_ms']
        if old_due!=add(start,delay) or new_due!=add(end,delay):raise ValueError('cleanup retry deadline differs')
    elif retained_holds and before['reason'] in ('uncertain_effect','canonical_binding'):
        # These exact producer branches return before status/checkpoint updates.
        # A hold tick does not settle the task or authorize any cleanup effect.
        if (before['first_safe_ms'] is not None or before['disposition']!='retained'
                or before['cache_started'] or before['cache_done']
                or any(slot['started'] or slot['done'] for slot in before['worktrees'])):
            raise ValueError('cleanup retained hold scope differs')
        delay=before['policy']['retry_delay_ms']
        if old_due!=add(start,delay) or new_due!=add(end,delay):
            raise ValueError('cleanup retained hold deadline differs')
    elif before['reason']=='cache_dependency':
        # cleanup.rs saves the updated observation at796 before its dependency
        # await. That intermediate record still carries the preceding deadline.
        # Multiple ticks may occur before inspect: a retained deadline may come
        # from any completed tick between the two observed times.
        if before['cache_safe_ms'] is not None or before['disposition']!='retained' or before['cache_started'] or before['cache_done'] or not all(slot['done'] for slot in before['worktrees']):raise ValueError('cleanup cache observation scope differs')
        delay=before['policy']['retry_delay_ms'];old_max=add(start,delay);new_max=add(end,delay)
        if type(old_due) is not int or type(new_due) is not int or not delay<=old_due<=old_max or not old_due<=new_due<=new_max:raise ValueError('cleanup inherited retry deadline differs')
        if new_due!=old_due and not start<=new_due-delay<=end:raise ValueError('cleanup retry tick outside observation interval')
    else:raise ValueError('cleanup reason outside observation-only scope')
    return {'last_observed_ms_before':start,'last_observed_ms_after':end,'next_due_ms_before':old_due,'next_due_ms_after':new_due,'unchanged_record_sha256':c.digest(c.canonical(before))}


CLEANUP_ATTEMPT_FIELDS = ('id','task_id','session_id','generation','worker_id','worker_incarnation','deadline_unix_ms')

def _add(a,b):
    if type(a) is not int or type(b) is not int or not 0<=b<=2**64-1 or not 0<=a<=2**64-1-b:
        raise ValueError('cleanup_transition_clock_overflow')
    return a+b

def _same(c,a,b):
    return c.canonical(a)==c.canonical(b)

def verify_task(c,command,authority,worker):
    c.fields(authority,'id session_id assigned_agent_id status input_sha256 attempt selected_checkpoint result_binding task_sha256','cleanup task authority')
    if command['kind']!='run_attempt' or command['task']['definition']['executor']!='coding':
        raise ValueError('cleanup_transition_command_kind')
    attempt=command['attempt'];task=command['task'];current=authority['attempt']
    if (authority['id']!=attempt['task_id'] or authority['session_id']!=attempt['session_id']
            or authority['assigned_agent_id']!=task['definition']['id']
            or authority['input_sha256']!=c.digest(c.canonical(task))
            or current is None or any(current.get(k)!=attempt[k] for k in CLEANUP_ATTEMPT_FIELDS)
            or current['worker_id']!=worker or current.get('stopped') is not True
            or current.get('phase')!='stopped' or current.get('uncertain') is not False
            or authority['status'] not in ('succeeded','failed','cancelled')):
        raise ValueError('cleanup_transition_canonical_binding')
    if re.fullmatch('[0-9a-f]{64}',authority['task_sha256']) is None:
        raise ValueError('cleanup_transition_task_hash')
    if authority['status']=='succeeded':
        result=authority['result_binding']
        if (authority['selected_checkpoint'] is None or result is None
                or set(result)!= {'attempt_id','checkpoint'} or result['attempt_id']!=attempt['id']
                or not _same(c,result['checkpoint'],authority['selected_checkpoint'])):
            raise ValueError('cleanup_transition_canonical_result')
    return authority

def validate_transition(original,observed,command,task,common,validate_new_proof):
    """Validate only the two selected transitions; record parsers run beforehand."""
    c=common;worker=command['attempt']['worker_id'];verify_task(c,command,task,worker)
    if (original['command_id']!=command['command_id'] or observed['command_id']!=command['command_id']
            or original['attempt_id']!=command['attempt']['id'] or observed['attempt_id']!=command['attempt']['id']
            or original['disposition']!='retained' or observed['disposition']!='retained'
            or original['cache_started'] or original['cache_done'] or observed['cache_started'] or observed['cache_done']
            or original['cache_safe_ms'] is not None or observed['cache_safe_ms'] is not None
            or not _same(c,original['cache'],observed['cache'])
            or observed['last_observed_ms']<original['last_observed_ms']):
        raise ValueError('cleanup_transition_material_or_cache_scope')
    start=original['last_observed_ms'];end=observed['last_observed_ms'];policy=original['policy']
    before=copy.deepcopy(original);after=copy.deepcopy(observed)
    for value in (before,after):
        value.pop('last_observed_ms');value.pop('next_due_ms');value.pop('reason')
    pair=(original['reason'],observed['reason'])
    if pair==('grace','cache_dependency'):
        if (original['status']!=task['status'] or observed['status']!=task['status']
                or not _same(c,original['checkpoint'],task['selected_checkpoint'])
                or not _same(c,observed['checkpoint'],task['selected_checkpoint'])):
            raise ValueError('cleanup_transition_terminal_checkpoint')
        grace=policy[{'succeeded':'success_grace_ms','failed':'failed_grace_ms','cancelled':'cancelled_grace_ms'}[task['status']]]
        due=_add(original['first_safe_ms'],grace)
        if original['next_due_ms']!=due or not start<due<=end:
            raise ValueError('cleanup_transition_grace_expiry')
        for old,new in zip(original['worktrees'],observed['worktrees']):
            if (old['started'] is not False or old['done'] is not False or new['done'] is not True
                    or new['started'] is not (old['proof'] is not None) or not _same(c,old['proof'],new['proof'])):
                raise ValueError('cleanup_transition_worktree_completion')
        # A final retry, or a later intermediate sample retaining a prior retry,
        # must originate after the original grace deadline and no later than end.
        retry=policy['retry_delay_ms'];new_due=observed['next_due_ms'];latest=_add(end,retry)
        if type(new_due) is not int or not retry<=new_due<=latest or not due<=new_due-retry<=end:
            raise ValueError('cleanup_transition_retry_deadline')
        before.pop('worktrees');after.pop('worktrees')
        if not _same(c,before,after):raise ValueError('cleanup_transition_other_material_changed')
        return {'kind':'grace_to_cache_dependency','task_sha256':task['task_sha256'],
                'completed_worktree_slots':2,'existing_worktree_proofs_exact':True,'cache_disposal':False}
    if pair==('local_recovery','grace'):
        if (original['status'] is not None or observed['status']!='cancelled' or task['status']!='cancelled'
                or original['checkpoint'] is not None or observed['checkpoint'] is not None
                or task['selected_checkpoint'] is not None or original['first_safe_ms'] is not None
                or original['cache'] is not None):
            raise ValueError('cleanup_transition_cancelled_scope')
        if original['next_due_ms']!=_add(start,policy['retry_delay_ms']):
            raise ValueError('cleanup_transition_original_retry')
        first=observed['first_safe_ms'];due=_add(first,policy['cancelled_grace_ms'])
        if type(first) is not int or not start<=first<=end<due or observed['next_due_ms']!=due:
            raise ValueError('cleanup_transition_new_grace')
        for old,new in zip(original['worktrees'],observed['worktrees']):
            if old['started'] or old['done'] or new['started'] or new['done'] or old['proof'] is not None:
                raise ValueError('cleanup_transition_new_proof_flags')
        proof=observed['worktrees'][0]['proof']
        if proof is None or observed['worktrees'][1]['proof'] is not None:
            raise ValueError('cleanup_transition_new_proof_slots')
        proof_evidence=validate_new_proof(proof,command)
        for value in (before,after):
            value.pop('first_safe_ms');value.pop('status');value.pop('worktrees')
        if not _same(c,before,after):raise ValueError('cleanup_transition_other_material_changed')
        return {'kind':'local_recovery_to_cancelled_grace','task_sha256':task['task_sha256'],
                'new_worktree_proof':proof_evidence,'cache_disposal':False,'worktree_removal_started':False}
    raise ValueError('cleanup_transition_not_selected')

def _full(m):
    return (m.st_dev,m.st_ino,m.st_uid,m.st_gid,m.st_mode,m.st_nlink,m.st_size,m.st_mtime_ns,m.st_ctime_ns)

def _directory(path,uid):
    path=Path(path)
    if not path.is_absolute() or '..' in path.parts or str(path)!=os.path.normpath(str(path)):
        raise ValueError('cleanup_transition_absolute_path')
    fd=os.open('/',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        for part in path.parts[1:]:
            new=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd);os.close(fd);fd=new
        m=os.fstat(fd)
        if m.st_uid!=uid or m.st_mode&0o022:raise ValueError('cleanup_transition_directory_owner')
        return fd,m
    except BaseException:os.close(fd);raise

def _file(path,bound,uid):
    path=Path(path);parent,pm=_directory(path.parent,uid)
    try:
        before=os.stat(path.name,dir_fd=parent,follow_symlinks=False)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid!=uid or before.st_nlink!=1
                or before.st_mode&0o022 or not 0<=before.st_size<=bound):
            raise ValueError('cleanup_transition_file_metadata')
        fd=os.open(path.name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=parent)
        try:
            if _full(os.fstat(fd))!=_full(before):raise ValueError('cleanup_transition_file_race')
            with os.fdopen(fd,'rb',closefd=False) as stream:raw=stream.read(before.st_size+1)
            if (len(raw)!=before.st_size or _full(os.fstat(fd))!=_full(before)
                    or _full(os.stat(path.name,dir_fd=parent,follow_symlinks=False))!=_full(before)):
                raise ValueError('cleanup_transition_file_race')
        finally:os.close(fd)
        check,after=_directory(path.parent,uid)
        try:
            if _full(os.fstat(parent))!=_full(pm) or _full(after)!=_full(pm):
                raise ValueError('cleanup_transition_parent_race')
        finally:os.close(check)
        return raw,[before.st_dev,before.st_ino,before.st_uid,before.st_gid,stat.S_IMODE(before.st_mode)]
    finally:os.close(parent)

def _cache_config(raw,source):
    """Closed clone-generated subset, not a general Git config interpreter."""
    try:text=raw.decode('utf-8')
    except UnicodeError:raise ValueError('cleanup_transition_git_config_encoding') from None
    section=None;values={}
    for line in text.split('\n'):
        line=line.strip(' \t')
        if not line or line.startswith(('#',';')):continue
        if line in ('[core]','[remote "origin"]'):
            section='core' if line=='[core]' else 'origin';continue
        if (section is None or '=' not in line or any(ch in line for ch in '\x00\r\\"#;')
                or any(ord(ch)<32 and ch!='\t' for ch in line)):
            raise ValueError('cleanup_transition_git_config_unsupported')
        key,value=(part.strip(' \t') for part in line.split('=',1));item=(section,key)
        if (item in values or item not in {('core','repositoryformatversion'),('core','filemode'),
                ('core','bare'),('origin','url')} or not value):
            raise ValueError('cleanup_transition_git_config_unsupported')
        values[item]=value
    if (values.get(('core','repositoryformatversion'))!='0' or values.get(('core','filemode')) not in ('true','false')
            or values.get(('core','bare'))!='true' or values.get(('origin','url'))!=source):
        raise ValueError('cleanup_transition_git_config_binding')

# The active preflight runs the installed predecessor, not the candidate.
# This closed family describes its fixed idle journal/cleanup atomic writers.
_ACTIVE_PREFLIGHT_WRITERS = {
    'src/worker/process.rs': 'c13c470c5b414e0df7793e5140095344df09f10f1f6888c0431533bfbf93a6bb',
    'src/worker/mutations.rs': 'e510c3020ca1476ac3d2bda9762b2cee11d2f29d1ac552472d9218b9237e0553',
    'src/worker/cleanup.rs': '5cbe54319ba54f53064519c72240e5ea75070cc9a5879be548fd20c4971a87b9',
    'src/worker/mod.rs': 'cdc4900fd348d3a7c19a9fdc7abb5cb3e1d9e8537843bee95e627bd4da868b21',
}


# Installed 0516 has the same atomic process/mutation writers. Its cleanup and
# caller source changed; admit that exact predecessor family independently of
# the newer candidate. This remains a live sample, never stopped preservation.
_ACTIVE_PREFLIGHT_CURRENT_FORMATS = {'wire': 24, 'controller_schema': 25, 'worker_format': 19}
_ACTIVE_PREFLIGHT_CURRENT_WRITERS = {
    'src/worker/process.rs': 'c13c470c5b414e0df7793e5140095344df09f10f1f6888c0431533bfbf93a6bb',
    'src/worker/mutations.rs': 'e510c3020ca1476ac3d2bda9762b2cee11d2f29d1ac552472d9218b9237e0553',
    'src/worker/cleanup.rs': '4b0e33c95ae3f0d1c37a4a8f6af8dab9838ec7661c162824d7983b857242d3ea',
    'src/worker/mod.rs': 'bced916dd3dbb0572f4955d0f65641f491b90ab80cf2ac84f76d89fe68da621a',
}


def active_preflight_writers(host):
    files = getattr(host, '_verified_before_manifest', {}).get('source', {}).get('files', {})
    families = [_ACTIVE_PREFLIGHT_WRITERS]
    if _same(c, host.p.get('formats', {}).get('before'), _ACTIVE_PREFLIGHT_CURRENT_FORMATS):
        families.append(_ACTIVE_PREFLIGHT_CURRENT_WRITERS)
    for family in families:
        if all(isinstance(files.get(name), dict) and files[name].get('sha256') == sha
               for name, sha in family.items()):
            return family
    return None


class ActiveWorkerSample:
    """Bounded pre-stop observations; never a stopped preservation baseline."""
    slots = frozenset(('mutations/registration.json', 'mutations/await.json'))

    def __init__(self, host):
        self.host = host
        self.writer_sources = active_preflight_writers(host)
        if self.writer_sources is None:
            raise ValueError('active sample installed writer differs')
        policy = host.cleanup_policy()
        if policy is None:
            raise ValueError('active sample cleanup policy unselected')
        self.cleanup_bound = min(host.l['state_bytes'], host.l['receipt_bytes'],
                                 policy['retention']['worker']['record_bytes'])
        self.mutation_bound = min(host.l['state_bytes'], host.l['receipt_bytes'],
                                  host.retained_policy()['worker']['mutation_record_bytes'])
        self.root = _full(Path(host.h['state']['root']).lstat())
        self.captured = {}; self.captured_bytes = 0; self.transients = set()
        self.vanished = set(); self.vanished_reserved_bytes = 0
        self.directory_changes = 0; self.replacements = 0

    def mutable_bound(self, key):
        if key in self.slots:
            return self.mutation_bound
        if re.fullmatch(r'cleanup/(?:[0-9a-f]{2})+\.json', key):
            return self.cleanup_bound
        return None

    def transient_target(self, key):
        if key.endswith('.pending'):
            target = key[:-8] + '.json'
            if self.mutable_bound(target) is not None:
                return target
        return None

    def metadata(self, key, meta):
        bound = self.mutable_bound(key)
        if (bound is None or not stat.S_ISREG(meta.st_mode) or meta.st_nlink != 1
                or meta.st_dev != self.root[0] or meta.st_uid != self.host.h['service_uid']
                or meta.st_gid != self.root[3] or stat.S_IMODE(meta.st_mode) != 0o600
                or not 0 <= meta.st_size <= bound):
            raise ValueError('active sample mutable file metadata')

    def vanished_temporary(self, key):
        target = self.transient_target(key)
        if target is None:
            return None
        if key in self.vanished:
            raise ValueError('active sample repeated vanished temporary')
        self.vanished.add(key); self.transients.add(target)
        bound = self.mutable_bound(target)
        self.vanished_reserved_bytes += bound
        # The name was enumerated but its type/bytes were not observed. Reserve
        # the entire selected record bound; do not infer zero bytes or emptiness.
        return bound

    def transient(self, key, meta):
        target = self.transient_target(key)
        if target is None:
            return False
        self.metadata(target, meta)
        self.transients.add(target)
        return True

    def entry(self, key, meta):
        if key in ('cleanup', 'mutations'):
            if (not stat.S_ISDIR(meta.st_mode) or meta.st_uid != self.host.h['service_uid']
                    or meta.st_gid != self.root[3] or stat.S_IMODE(meta.st_mode) != 0o700):
                raise ValueError('active sample directory ownership')
        if self.mutable_bound(key) is None:
            return False
        self.metadata(key, meta)
        return True

    def directory(self, relative, expected, observed):
        if relative not in ('cleanup', 'mutations'):
            return expected == observed
        if (tuple(expected[:7]) != tuple(observed[:7])
                or expected[2] != self.host.h['service_uid'] or expected[3] != self.root[3]
                or not stat.S_ISDIR(expected[4]) or stat.S_IMODE(expected[4]) != 0o700):
            return False
        self.directory_changes += expected[7:] != observed[7:]
        return True

    def opened(self, key, before, opened):
        if _full(before) == _full(opened):
            return opened
        if self.mutable_bound(key) is None or before.st_ino == opened.st_ino:
            raise ValueError('state file changed before read')
        self.metadata(key, before); self.metadata(key, opened)
        self.replacements += 1
        return opened

    def path(self, key, expected, observed):
        if expected == observed:
            return True
        bound = self.mutable_bound(key)
        if (bound is None or expected[1] == observed[1] or expected[0] != observed[0]
                or expected[2:5] != observed[2:5] or observed[5] != 1
                or not stat.S_ISREG(observed[4]) or not 0 <= observed[6] <= bound):
            return False
        self.replacements += 1
        return True

    def capture_bytes(self, key, raw, identity, digest):
        if (key in self.captured or self.mutable_bound(key) is None
                or len(raw) > self.mutable_bound(key) or c.digest(raw) != digest):
            raise ValueError('active sample body binding')
        self.captured_bytes += len(raw)
        if self.captured_bytes > self.host.l['receipt_bytes']:
            raise ValueError('active sample aggregate bound')
        self.captured[key] = (raw, {'sha256': digest, 'identity': identity})

    def cleanup_bytes(self, name, entry, bound):
        pair = self.captured.get(name)
        if (pair is None or not name.startswith('cleanup/')
                or c.canonical(pair[1]) != c.canonical(entry) or len(pair[0]) > bound):
            raise ValueError('active cleanup exact sample required')
        return pair[0]

    def finish(self, state):
        if not self.transients <= set(self.captured):
            raise ValueError('active sample temporary without sampled target')
        if any(c.canonical(pair[1]) != c.canonical(state.get(key))
               for key, pair in self.captured.items()):
            raise ValueError('active sample inventory binding')
        return {'simultaneous_snapshot': False, 'owned_snapshot': False,
                'complete_historical_membership': False, 'temporary_bodies_validated': False,
                'temporary_targets_observed': len(self.transients),
                'vanished_temporary_names': len(self.vanished),
                'vanished_temporary_metadata_validated': False,
                'vanished_temporary_reserved_bytes': self.vanished_reserved_bytes,
                'directory_time_changes': self.directory_changes,
                'atomic_path_replacements': self.replacements,
                'installed_writer_sources': self.writer_sources}


class WorkerStateCapture:
    def __init__(self, host, common, stop):
        self.host, self.c, self.stop = host, common, stop
        if host.h['role'] != 'worker':
            raise ValueError('cleanup_capture_worker_only')
        policy = host.cleanup_policy()
        if policy is None:
            raise ValueError('cleanup_capture_policy_unselected')
        self.record = cleanup_record
        self.policy = policy
        self.bound = min(host.l['state_bytes'], host.l['receipt_bytes'], policy['retention']['worker']['record_bytes'])
        self.original = stop.get('cleanup_records', {})
        self.expected = {name for name in stop['state'] if name.startswith('cleanup/')}
        if not self.expected or self.expected != set(self.original) or len(self.expected) > host.l['state_entries']:
            raise ValueError('cleanup_capture_original_membership')
        self.registry_ctime_enabled = host.selected_writer_sources({'src/worker/process.rs': _PROCESS_SOURCE_SHA})
        self.process_roots = set()
        for name in self.expected:
            if (len(Path(name).parts) != 2 or not name.endswith('.json')
                    or not name[8:-5] or any(c not in '0123456789abcdef' for c in name[8:-5])):
                raise ValueError('cleanup_capture_original_name')
            raw = self.original[name].encode('utf-8')
            if len(raw) > self.bound or common.digest(raw) != stop['state'][name]['sha256']:
                raise ValueError('cleanup_capture_original_hash')
            record = self.record(raw, name, policy)
            self.process_roots.add('processes/cleanup-' + record['attempt_id'].encode('utf-8').hex())
            command = 'commands/' + record['command_id'].encode().hex() + '.json'
            if command not in stop['state']:
                raise ValueError('cleanup_capture_original_command')
        self.process_files = {name for name in stop['state'] if self.process_root(name) is not None}
        self.process_seen = {}
        self.process_directory_after = {}
        self.process_ctime_changes = 0
        self.slots = frozenset(('mutations/registration.json', 'mutations/await.json'))
        self.mutation_files = {name for name in stop['state'] if name.startswith('mutations/')}
        if not self.slots <= self.mutation_files:
            raise ValueError('live_capture_original_slots')
        self.mutation_dirs = {'mutations'}
        for name in self.mutation_files:
            parts = Path(name).parts
            if any(part in ('.', '..') for part in parts) or str(Path(name)) != name:
                raise ValueError('live_capture_original_path')
            self.mutation_dirs.update('/'.join(parts[:n]) for n in range(1, len(parts)))
        self.mutation_nodes = self.mutation_files | self.mutation_dirs
        if len(self.mutation_nodes) > host.l['state_entries']:
            raise ValueError('live_capture_original_bound')
        retained = host.retained_policy()
        limit = retained['worker']['mutation_record_bytes']
        if type(limit) is not int or limit <= 0:
            raise ValueError('live_capture_record_bound')
        self.mutation_bound = min(host.l['state_bytes'], host.l['receipt_bytes'], limit)
        self.mutation_seen = {}
        self.mutation_captured = {}
        self.mutation_directory_before = None
        self.mutation_directory_after = None
        self.mutation_time_changes = 0
        self.slot_path_replacements = 0
        self.captured = {}
        self.captured_bytes = 0
        self.directory_before = None
        self.directory_after = None
        self.time_changes = 0
        self.finished = False
        self.started = False

    def deadline(self):
        if self.c.now_ms() >= self.host.r['deadline_ms']:
            raise ValueError('cleanup_capture_deadline')

    def entry(self, key, meta):
        if self.process_root(key) is not None:
            return self.process_entry(key, meta)
        if key == 'mutations' or key.startswith('mutations/'):
            return self.mutation_entry(key, meta)
        if not key.startswith('cleanup/'):
            return False
        self.deadline()
        if key not in self.expected:
            raise ValueError('cleanup_capture_unknown_child')
        original = self.stop['state'][key]['identity']
        if (not stat.S_ISREG(meta.st_mode) or meta.st_nlink != 1
                or meta.st_dev != original[0] or meta.st_uid != self.host.h['service_uid']
                or meta.st_uid != original[2] or meta.st_gid != original[3]
                or stat.S_IMODE(meta.st_mode) != 0o600 or original[4] != 0o600
                or meta.st_size > self.bound):
            raise ValueError('cleanup_capture_child_metadata')
        return True

    def capture_bytes(self, key, raw, identity, digest):
        self.deadline()
        if key in self.slots:
            if key in self.mutation_captured or len(raw) > self.mutation_bound or self.c.digest(raw) != digest:
                raise ValueError('live_capture_body_binding')
            expected = self.mutation_seen[key]
            public = [expected[0], expected[1], expected[2], expected[3], stat.S_IMODE(expected[4])]
            if identity != public or len(raw) != expected[6]:
                raise ValueError('live_capture_descriptor_binding')
            self.mutation_captured[key] = (raw, {'sha256': digest, 'identity': identity})
            self.captured_bytes += len(raw)
            if self.captured_bytes > self.host.l['receipt_bytes']:
                raise ValueError('cleanup_capture_aggregate_bound')
            return
        if key not in self.expected or key in self.captured or len(raw) > self.bound or self.c.digest(raw) != digest:
            raise ValueError('cleanup_capture_body_binding')
        row = {'sha256': digest, 'identity': identity}
        self.captured[key] = (raw, row)
        self.captured_bytes += len(raw)
        if self.captured_bytes > self.host.l['receipt_bytes']:
            raise ValueError('cleanup_capture_aggregate_bound')

    def directory(self, relative, expected, observed):
        if self.process_clock_path(relative):
            if not self.registry_ctime_enabled:
                return expected == observed
            self.deadline()
            baseline = self.process_seen.get(relative)
            previous = self.process_directory_after.get(relative, baseline)
            if (baseline is None or tuple(expected) != baseline
                    or tuple(observed[:8]) != baseline[:8]
                    or observed[8] < baseline[8] or observed[8] < previous[8]
                    or baseline[2] != self.host.h['service_uid']
                    or not stat.S_ISDIR(baseline[4]) or stat.S_IMODE(baseline[4]) != 0o700):
                return False
            self.process_ctime_changes += observed[8] != previous[8]
            self.process_directory_after[relative] = tuple(observed)
            return True
        if relative == 'mutations':
            self.deadline()
            if self.mutation_directory_before is None:
                self.mutation_directory_before = tuple(expected)
            if tuple(expected) != self.mutation_directory_before or tuple(expected[:7]) != tuple(observed[:7]):
                return False
            if expected[2] != self.host.h['service_uid'] or not stat.S_ISDIR(expected[4]) or stat.S_IMODE(expected[4]) != 0o700:
                return False
            self.mutation_time_changes += tuple(expected[7:]) != tuple(observed[7:])
            self.mutation_directory_after = tuple(observed)
            return True
        if relative != 'cleanup':
            return expected == observed
        self.deadline()
        if self.directory_before is None:
            self.directory_before = tuple(expected)
        if tuple(expected) != self.directory_before or tuple(expected[:7]) != tuple(observed[:7]):
            return False
        if expected[2] != self.host.h['service_uid'] or not stat.S_ISDIR(expected[4]) or stat.S_IMODE(expected[4]) != 0o700:
            return False
        self.time_changes += tuple(expected[7:]) != tuple(observed[7:])
        self.directory_after = tuple(observed)
        return True

    def process_root(self, key):
        parts = Path(key).parts
        candidate = '/'.join(parts[:2])
        return candidate if candidate in self.process_roots else None

    def process_clock_path(self, key):
        root = self.process_root(key)
        return root is not None and key in (root, root + '/home', root + '/home/code')

    def process_entry(self, key, meta):
        self.deadline()
        if key in self.process_seen or len(self.process_seen) >= self.host.l['state_entries']:
            raise ValueError('registry_capture_repeated_or_bound')
        if stat.S_ISDIR(meta.st_mode):
            if self.process_clock_path(key) and (meta.st_uid != self.host.h['service_uid']
                    or stat.S_IMODE(meta.st_mode) != 0o700):
                raise ValueError('registry_capture_directory_metadata')
        else:
            if key not in self.process_files or not stat.S_ISREG(meta.st_mode) or meta.st_nlink != 1:
                raise ValueError('registry_capture_unknown_or_nonregular_child')
            public = [meta.st_dev, meta.st_ino, meta.st_uid, meta.st_gid, stat.S_IMODE(meta.st_mode)]
            if public != self.stop['state'][key]['identity']:
                raise ValueError('registry_capture_retained_identity')
        self.process_seen[key] = _full(meta)
        # Original inventory hashes the full body with its unchanged descriptor
        # and post-path checks. Process records need no additional body buffer.
        return False

    def process_membership(self, current):
        self.deadline()
        directories = {key for key, meta in self.process_seen.items() if stat.S_ISDIR(meta[4])}
        files = set(self.process_seen) - directories
        if (files != self.process_files
                or {key for key in current if self.process_root(key) is not None} != files
                or {key for key in self.host.inventory_directories if self.process_root(key) is not None} != directories):
            raise ValueError('registry_capture_complete_membership')
        for key in files:
            if self.c.canonical(current[key]) != self.c.canonical(self.stop['state'][key]):
                raise ValueError('registry_capture_retained_bytes')
        # Stop contains file rows, not directory rows. Only historical files are
        # compared to Stop. Directory membership is a complete current sample,
        # including empty directories, and is rechecked below without body reads.
        encountered = self.process_roots & directories
        if not encountered:
            return
        children = {key: set() for key in directories}
        for key in self.process_seen:
            parent = str(Path(key).parent)
            if parent in children:
                children[parent].add(Path(key).name)
        root = Path(self.host.h['state']['root'])
        fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for part in root.parts[1:]:
                self.deadline()
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = child
            if _full(os.fstat(fd)) != self.host.inventory_directories['.']:
                raise ValueError('registry_capture_root_changed')
            parent = os.open('processes', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            try:
                parent_expected = self.host.inventory_directories['processes']
                if _full(os.fstat(parent)) != parent_expected:
                    raise ValueError('registry_capture_parent_changed')
                def visit(parent_fd, name, relative):
                    self.deadline()
                    directory = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
                    try:
                        expected = self.process_seen[relative]
                        if not self.directory(relative, expected, _full(os.fstat(directory))):
                            raise ValueError('registry_capture_directory_changed')
                        seen = set()
                        with os.scandir(directory) as entries:
                            for child in entries:
                                self.deadline()
                                if child.name in seen or child.name not in children[relative]:
                                    raise ValueError('registry_capture_membership_changed')
                                seen.add(child.name)
                                key = relative + '/' + child.name
                                observed = _full(os.stat(child.name, dir_fd=directory, follow_symlinks=False))
                                expected_child = self.process_seen[key]
                                if key in directories:
                                    if not self.directory(key, expected_child, observed):
                                        raise ValueError('registry_capture_directory_changed')
                                    visit(directory, child.name, key)
                                elif observed != expected_child:
                                    raise ValueError('registry_capture_child_changed')
                        if seen != children[relative]:
                            raise ValueError('registry_capture_membership_changed')
                        if (not self.directory(relative, expected, _full(os.fstat(directory)))
                                or not self.directory(relative, expected, _full(os.stat(name, dir_fd=parent_fd, follow_symlinks=False)))):
                            raise ValueError('registry_capture_directory_changed')
                    finally:
                        os.close(directory)
                for relative in sorted(encountered):
                    visit(parent, Path(relative).name, relative)
                if (_full(os.fstat(parent)) != parent_expected
                        or _full(os.stat('processes', dir_fd=fd, follow_symlinks=False)) != parent_expected):
                    raise ValueError('registry_capture_parent_changed')
            finally:
                os.close(parent)
            if (_full(os.fstat(fd)) != self.host.inventory_directories['.']
                    or _full(root.lstat()) != self.host.inventory_directories['.']):
                raise ValueError('registry_capture_root_changed')
        finally:
            os.close(fd)

    def mutation_entry(self, key, meta):
        self.deadline()
        if key not in self.mutation_nodes or key in self.mutation_seen:
            raise ValueError('live_capture_unknown_or_repeated_child')
        if key in self.mutation_dirs:
            if not stat.S_ISDIR(meta.st_mode) or meta.st_uid != self.host.h['service_uid'] or stat.S_IMODE(meta.st_mode) != 0o700:
                raise ValueError('live_capture_directory_metadata')
        else:
            old = self.stop['state'][key]['identity']
            current = [meta.st_dev, meta.st_ino, meta.st_uid, meta.st_gid, stat.S_IMODE(meta.st_mode)]
            if not stat.S_ISREG(meta.st_mode) or meta.st_nlink != 1:
                raise ValueError('live_capture_file_metadata')
            if key in self.slots:
                if (current[0:1] + current[2:] != old[0:1] + old[2:]
                        or current[2] != self.host.h['service_uid'] or current[4] != 0o600
                        or meta.st_size > self.mutation_bound):
                    raise ValueError('live_capture_slot_metadata')
            elif current != old:
                raise ValueError('live_capture_retained_identity')
        self.mutation_seen[key] = _full(meta)
        return key in self.slots

    def path(self, key, expected, observed):
        if key not in self.slots:
            return expected == observed
        self.deadline()
        if tuple(expected) != self.mutation_seen.get(key):
            raise ValueError('live_capture_path_sample')
        if expected == observed:
            return True
        # Only an atomic replacement after the stable descriptor read is allowed.
        # The replacement metadata is checked; its body is not claimed as sampled.
        if (expected[1] == observed[1] or observed[0] != expected[0]
                or observed[2:5] != expected[2:5] or observed[5] != 1
                or not stat.S_ISREG(observed[4]) or not 0 <= observed[6] <= self.mutation_bound):
            return False
        self.slot_path_replacements += 1
        return True

    def mutation_membership(self, current):
        self.deadline()
        if (set(self.mutation_seen) != self.mutation_nodes
                or {k for k in current if k.startswith('mutations/')} != self.mutation_files
                or {k for k in self.host.inventory_directories if k == 'mutations' or k.startswith('mutations/')} != self.mutation_dirs
                or set(self.mutation_captured) != self.slots):
            raise ValueError('live_capture_complete_membership')
        for key in self.mutation_files:
            if key in self.slots:
                if self.c.canonical(self.mutation_captured[key][1]) != self.c.canonical(current[key]):
                    raise ValueError('live_capture_inventory_binding')
            elif self.c.canonical(current[key]) != self.c.canonical(self.stop['state'][key]):
                raise ValueError('live_capture_retained_bytes')
        # No bodies are read again. Reopen only directory descriptors and compare
        # complete membership plus each child's observed metadata to its sample.
        root = Path(self.host.h['state']['root'])
        fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for part in root.parts[1:]:
                self.deadline()
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = child
            if _full(os.fstat(fd)) != self.host.inventory_directories['.']:
                raise ValueError('live_capture_root_changed')
            def visit(parent, name, relative):
                self.deadline()
                directory = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
                try:
                    expected = self.mutation_seen[relative]
                    if not self.directory(relative, expected, _full(os.fstat(directory))):
                        raise ValueError('live_capture_directory_changed')
                    expected_names = {Path(k).name for k in self.mutation_nodes if str(Path(k).parent) == relative}
                    seen = set()
                    with os.scandir(directory) as entries:
                        for child in entries:
                            self.deadline()
                            if child.name in seen or child.name not in expected_names:
                                raise ValueError('live_capture_membership_changed')
                            seen.add(child.name)
                            key = relative + '/' + child.name
                            observed = _full(os.stat(child.name, dir_fd=directory, follow_symlinks=False))
                            expected_child = self.mutation_seen[key]
                            if key in self.mutation_dirs:
                                if observed != expected_child:
                                    raise ValueError('live_capture_nested_directory_changed')
                                visit(directory, child.name, key)
                            elif not self.path(key, expected_child, observed):
                                raise ValueError('live_capture_child_changed')
                    if seen != expected_names:
                        raise ValueError('live_capture_membership_changed')
                    if (not self.directory(relative, expected, _full(os.fstat(directory)))
                            or not self.directory(relative, expected, _full(os.stat(name, dir_fd=parent, follow_symlinks=False)))):
                        raise ValueError('live_capture_directory_changed')
                finally:
                    os.close(directory)
            visit(fd, 'mutations', 'mutations')
            if (_full(os.fstat(fd)) != self.host.inventory_directories['.']
                    or _full(root.lstat()) != self.host.inventory_directories['.']):
                raise ValueError('live_capture_root_changed')
        finally:
            os.close(fd)

    def cleanup_bytes(self, name, entry, bound):
        self.deadline()
        pair = self.captured.get(name)
        if pair is None or self.c.canonical(pair[1]) != self.c.canonical(entry) or len(pair[0]) > bound:
            raise ValueError('cleanup_capture_exact_sample_required')
        # These bytes already passed the original descriptor, post-read entry,
        # single-link, owner/mode and hash checks. Never fetch a later version.
        return pair[0]

    def membership(self):
        self.deadline()
        root = Path(self.host.h['state']['root'])
        fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for part in root.parts[1:]:
                self.deadline()
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = child
            if _full(os.fstat(fd)) != self.host.inventory_directories['.']:
                raise ValueError('cleanup_capture_root_changed')
            directory = os.open('cleanup', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            try:
                if not self.directory('cleanup', self.directory_before, _full(os.fstat(directory))):
                    raise ValueError('cleanup_capture_directory_identity')
                names = set()
                with os.scandir(directory) as entries:
                    for child in entries:
                        self.deadline()
                        key = 'cleanup/' + child.name
                        if key in names or len(names) >= len(self.expected):
                            raise ValueError('cleanup_capture_membership_bound')
                        self.entry(key, os.stat(child.name, dir_fd=directory, follow_symlinks=False))
                        names.add(key)
                if names != self.expected:
                    raise ValueError('cleanup_capture_membership_changed')
                if (not self.directory('cleanup', self.directory_before, _full(os.fstat(directory)))
                        or not self.directory('cleanup', self.directory_before,
                                              _full(os.stat('cleanup', dir_fd=fd, follow_symlinks=False)))):
                    raise ValueError('cleanup_capture_directory_identity')
            finally:
                os.close(directory)
            if (_full(os.fstat(fd)) != self.host.inventory_directories['.']
                    or _full(root.lstat()) != self.host.inventory_directories['.']):
                raise ValueError('cleanup_capture_root_changed')
        finally:
            os.close(fd)

    def inventory(self):
        if self.started:
            raise ValueError('cleanup_capture_no_repeat')
        self.started = True
        current = self.host._state_files(self)
        if (self.directory_before is None or set(self.captured) != self.expected
                or {name for name in current if name.startswith('cleanup/')} != self.expected):
            raise ValueError('cleanup_capture_complete_membership')
        self.membership()
        self.mutation_membership(current)
        self.process_membership(current)
        for name in sorted(self.expected):
            raw, entry = self.captured[name]
            if self.c.canonical(entry) != self.c.canonical(current[name]):
                raise ValueError('cleanup_capture_inventory_binding')
            record = self.record(raw, name, self.policy)
            command = 'commands/' + record['command_id'].encode().hex() + '.json'
            if (command not in self.stop['state'] or command not in current
                    or self.c.canonical(current[command]) != self.c.canonical(self.stop['state'][command])):
                raise ValueError('cleanup_capture_original_command_changed')
            old = self.stop['state'][name]
            if self.c.canonical(old) != self.c.canonical(entry):
                # Unchanged original method; only its byte provider is captured.
                self.host.cleanup_observation(name, old, entry, self.original)
            elif raw != self.original[name].encode('utf-8'):
                raise ValueError('cleanup_capture_original_bytes')
        self.finished = True
        return current

    def proof(self):
        if not self.finished:
            raise ValueError('cleanup_capture_incomplete')
        rows = {name: {'sha256': pair[1]['sha256'], 'identity': pair[1]['identity'], 'bytes': len(pair[0])}
                for name, pair in self.captured.items()}
        return {'status': 'PASS_CAPTURED_CLEANUP_CLOCK_PRESERVATION', 'implementation': 'direct_scanner_hooks',
                'records': len(rows), 'captured_bytes': sum(row['bytes'] for row in rows.values()),
                'captured_inventory_sha256': self.c.digest(self.c.canonical(rows)),
                'directory_before': self.directory_before, 'directory_after': self.directory_after,
                'directory_time_comparisons_changed': self.time_changes,
                'complete_original_membership': True, 'later_record_reread': False,
                'simultaneous_snapshot': False, 'host_mutations': False,
                'mutation_slots': {name: {'sha256': pair[1]['sha256'], 'identity': pair[1]['identity'], 'bytes': len(pair[0])}
                                   for name, pair in sorted(self.mutation_captured.items())},
                'mutation_record_bound': self.mutation_bound,
                'mutation_records': len(self.mutation_files), 'mutation_directories': len(self.mutation_dirs),
                'mutation_immutable_records': len(self.mutation_files - self.slots),
                'mutation_directory_before': self.mutation_directory_before,
                'mutation_directory_after': self.mutation_directory_after,
                'mutation_directory_time_comparisons_changed': self.mutation_time_changes,
                'slot_post_read_path_replacements': self.slot_path_replacements,
                'complete_mutation_membership': True, 'mutable_bytes_semantically_decoded': False,
                'original_registration_and_worker_history_guards_unchanged': True,
                'cleanup_process_registry': {
                    'verified_writer_ctime_enabled': self.registry_ctime_enabled,
                    'eligible_roots': len(self.process_roots),
                    'encountered_roots': len(self.process_roots & set(self.process_seen)),
                    'sampled_nodes': len(self.process_seen),
                    'retained_file_rows': len(self.process_files),
                    'sampled_nodes_sha256': self.c.digest(self.c.canonical(self.process_seen)),
                    'ctime_comparisons_changed': self.process_ctime_changes,
                    'allowed_relative_suffixes': ['', 'home', 'home/code'],
                    'first_eight_metadata_fields_exact': True, 'ctime_nondecreasing': True,
                    'historical_file_rows_exact': True, 'complete_current_membership_rechecked': True,
                    'historical_directory_inventory_available': False,
                    'configured_home_branch_observed': False, 'writer_attributed': False}}

class CleanupTransitions:
    def __init__(self,host,common,stop,authority):
        self.host,self.c,self.stop,self.authority=host,common,stop,authority;self.proofs={}
        c=common
        c.fields(authority,'worker_id workspace_root tasks binding','cleanup authority')
        c.fields(authority['binding'],'original_preflight_sha256 current_controller_proof_sha256','cleanup authority binding')
        if (authority['worker_id']!=host.h['api']['worker_id']
                or any(not isinstance(v,str) or re.fullmatch('[0-9a-f]{64}',v) is None for v in authority['binding'].values())
                or (not host.codex_home_scope() and host.p['formats']['source_files'].get('src/worker/cleanup.rs') not in _CLEANUP_SOURCE_SHAS)):
            raise ValueError('cleanup_transition_authority_binding')
        unit=c.pin(host.h['new_unit'],host.l['module_bytes']).decode('utf-8')
        lines=[line.split('=',1)[1] for line in unit.splitlines() if line.startswith('ExecStart=')]
        if len(lines)!=1:raise ValueError('cleanup_transition_unit_exec')
        argv=shlex.split(lines[0]);positions=[i for i,x in enumerate(argv) if x=='--workspace-root']
        if len(positions)!=1 or positions[0]+1>=len(argv) or argv[positions[0]+1]!=authority['workspace_root']:
            raise ValueError('cleanup_transition_workspace_binding')
        self.workspace=Path(authority['workspace_root'])
        if not self.workspace.is_absolute() or '..' in self.workspace.parts or str(self.workspace)!=authority['workspace_root']:
            raise ValueError('cleanup_transition_workspace_path')
        self.original=host.cleanup_clock_observation
        self.record=cleanup_record
        self.selected=host.cleanup_policy()
        if self.selected is None:raise ValueError('cleanup_transition_policy')
        self.retained=host.retained_policy()
        self.bound=min(host.l['state_bytes'],host.l['receipt_bytes'],self.selected['retention']['worker']['record_bytes'])

    def deadline(self):
        if self.c.now_ms()>=self.host.r['deadline_ms']:raise ValueError('cleanup_transition_deadline')

    def new_proof(self,proof,command):
        self.deadline();uid=self.host.h['service_uid'];attempt=command['attempt']['id']
        path=self.workspace/'workspaces'/attempt.encode().hex();root=path.parent
        if proof['path']!=str(path) or proof['discard_source'] is not True:
            raise ValueError('cleanup_transition_proof_path')
        fds=[];identities=[];read=[]
        def directory(p,expected=None):
            fd,m=_directory(p,uid);fds.append(fd);identities.append((p,_full(m)))
            if expected is not None and expected!={'device':m.st_dev,'inode':m.st_ino}:
                raise ValueError('cleanup_transition_proof_identity')
        bound=min(self.host.l['module_bytes'],self.retained['worker']['git_output_bytes'])
        def file(p):
            self.deadline();raw,identity=_file(p,bound,uid);read.append((p,raw,identity));return raw
        try:
            directory(root,proof['root']);directory(path,proof['directory'])
            pointer=file(path/'.git').decode('utf-8')
            if not pointer.startswith('gitdir: ') or pointer.count('\n')>1 or '\r' in pointer:
                raise ValueError('cleanup_transition_git_pointer')
            gitdir=Path(pointer[8:].removesuffix('\n'))
            try:parts=gitdir.relative_to(self.workspace/'repositories').parts
            except ValueError:raise ValueError('cleanup_transition_git_association') from None
            if (len(parts)!=4 or re.fullmatch('repository-[0-9a-f]{16}',parts[0]) is None
                    or parts[1:3]!=('repository.git','worktrees')
                    or re.fullmatch('[A-Za-z0-9_-]{1,256}',parts[3]) is None):
                raise ValueError('cleanup_transition_git_association')
            cache=self.workspace/'repositories'/parts[0];bare=cache/'repository.git'
            directory(cache,proof['cache']);directory(bare);directory(gitdir)
            marker=self.c.strict(file(cache/'identity.json'))
            repository=command['task']['repository']
            expected={'version':2,'registration':repository['registration'],
                      'source_endpoint':repository['url'],'checkpoint_endpoint':repository['checkpoint_remote']}
            if not _same(self.c,marker,expected):raise ValueError('cleanup_transition_cache_registration')
            _cache_config(file(bare/'config'),repository['url'])
            if file(gitdir/'commondir') not in (b'../..',b'../..\n'):
                raise ValueError('cleanup_transition_git_common')
            if file(gitdir/'gitdir') not in (str(path/'.git').encode(),str(path/'.git').encode()+b'\n'):
                raise ValueError('cleanup_transition_git_backreference')
            head=file(gitdir/'HEAD').decode('ascii').removesuffix('\n')
            if re.fullmatch('[0-9a-fA-F]{40}|[0-9a-fA-F]{64}',head) is None or head!=proof['head']:
                raise ValueError('cleanup_transition_git_head')
            for p,raw,identity in read:
                check,newid=_file(p,bound,uid)
                if check!=raw or newid!=identity:raise ValueError('cleanup_transition_git_sample_race')
            for (p,expected),fd in zip(identities,fds):
                check,after=_directory(p,uid)
                try:
                    if _full(os.fstat(fd))!=expected or _full(after)!=expected:
                        raise ValueError('cleanup_transition_proof_directory_race')
                finally:os.close(check)
            return {'proof_sha256':self.c.digest(self.c.canonical(proof)),
                    'owned_root_directory_cache_verified':True,'accepted_repository_marker_exact':True,'bare_and_origin_config_exact':True,
                    'git_backreference_common_and_head_verified':True,'git_process_executed':False}
        finally:
            for fd in fds:os.close(fd)

    def observe(self,name,old,new,saved):
        self.deadline()
        try:return self.original(name,old,new,saved)
        except ValueError as exc:
            if str(exc)!='cleanup change exceeds observation clocks':raise
        # Original observation already validated both exact captured records,
        # selected policy, old hash and owner/mode/device before the clock guard.
        oldraw=saved[name].encode();newraw=self.host.cleanup_bytes(name,new,self.bound)
        before=self.record(oldraw,name,self.selected);after=self.record(newraw,name,self.selected)
        command_name='commands/'+before['command_id'].encode().hex()+'.json'
        if command_name not in self.stop['state']:raise ValueError('cleanup_transition_original_command')
        bound=min(self.host.l['state_bytes'],self.host.l['receipt_bytes'],self.retained['worker']['command_record_bytes'])
        raw,identity=_file(Path(self.host.h['state']['root'])/command_name,bound,self.host.h['service_uid'])
        command_entry={'sha256':self.c.digest(raw),'identity':identity}
        if not _same(self.c,command_entry,self.stop['state'][command_name]):raise ValueError('cleanup_transition_command_changed')
        receipt=self.c.strict(raw)
        if receipt['completed'] is not True:raise ValueError('cleanup_transition_command_incomplete')
        command=receipt['command'];task=self.authority['tasks'].get(command['attempt']['task_id'])
        if task is None:raise ValueError('cleanup_transition_task_absent')
        verify_task(self.c,command,task,self.authority['worker_id'])
        proof=validate_transition(before,after,command,task,self.c,self.new_proof)
        value={'original':old,'observed':new,'transition':proof,
               'authority_binding':self.authority['binding'],'captured_record_bytes_only':True}
        key=self.c.digest(name.encode());self.proofs[key]=value
        return value

    def proof(self):
        return {'status':'SCOPED_CLEANUP_TRANSITION_PROOFS','records':len(self.proofs),
                'proofs_sha256':self.c.digest(self.c.canonical(self.proofs)),
                'kinds':sorted({p['transition']['kind'] for p in self.proofs.values()}),
                'authority_binding':self.authority['binding'],'host_effects':False,
                'other_durable_inventory_guards_unchanged':True}

def _codex_deadline(host):
    if c.now_ms() >= host.r['deadline_ms']:raise ValueError('codex_copyout_deadline')


def _codex_read(host, relative, inventory, record_bound=None):
    _codex_deadline(host)
    entry=inventory.get(relative)
    if entry is None:raise ValueError('codex_copyout_original_file_absent')
    raw,identity=_file(Path(host.h['state']['root'])/relative,
                       min(host.l['receipt_bytes'],host.l['state_bytes'],record_bound or host.l['state_bytes']),host.h['service_uid'])
    if entry!={'sha256':c.digest(raw),'identity':identity}:raise ValueError('codex_copyout_file_changed')
    return raw


def _codex_id(meta):
    return {'device':meta[0],'inode':meta[1]}


def _codex_signature(host, base, name, inventory, directories):
    """Exact cleanup_copyout::signature encoding, streamed from no-follow FDs.

    The source is the stopped sample or the current scanner's actual rows, never
    reconstructed from a later manifest. Empty directories participate in SHA.
    """
    root=Path(host.h['state']['root']);digest=hashlib.sha256();files=0;size=0
    key=base+'/'+name;present=key in inventory or key in directories
    def walk(relative, display):
        nonlocal files,size
        _codex_deadline(host);digest.update(display.encode());digest.update(b'\0')
        if relative in directories:
            fd,m=_directory(root/relative,host.h['service_uid'])
            try:
                if _full(m)!=tuple(directories[relative]):raise ValueError('codex_copyout_directory_changed')
                digest.update(b'directory\0')
                names=sorted(os.listdir(fd))
                if len(names)>host.l['state_entries']:raise ValueError('codex_copyout_entry_bound')
                for child in names:walk(relative+'/'+child,display+'/'+child)
                if _full(os.fstat(fd))!=_full(m):raise ValueError('codex_copyout_directory_changed')
                check,after=_directory(root/relative,host.h['service_uid'])
                try:
                    if _full(after)!=_full(m):raise ValueError('codex_copyout_directory_changed')
                finally:os.close(check)
            finally:os.close(fd)
        else:
            expected=inventory.get(relative)
            if expected is None or set(expected)!={'sha256','identity'}:raise ValueError('codex_copyout_transcript_not_regular')
            parent,pm=_directory((root/relative).parent,host.h['service_uid'])
            try:
                m=os.stat(Path(relative).name,dir_fd=parent,follow_symlinks=False)
                if (not stat.S_ISREG(m.st_mode) or m.st_nlink!=1 or m.st_uid!=host.h['service_uid']
                        or m.st_dev!=pm.st_dev or m.st_mode&0o022):raise ValueError('codex_copyout_file_metadata')
                identity=[m.st_dev,m.st_ino,m.st_uid,m.st_gid,stat.S_IMODE(m.st_mode)]
                if identity!=expected['identity']:raise ValueError('codex_copyout_file_identity')
                fd=os.open(Path(relative).name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=parent)
                try:
                    if _full(os.fstat(fd))!=_full(m):raise ValueError('codex_copyout_file_race')
                    digest.update(b'file\0');digest.update(m.st_size.to_bytes(8,'big'));leaf=hashlib.sha256();used=0
                    while True:
                        _codex_deadline(host);raw=os.read(fd,min(host.l['io_chunk_bytes'],m.st_size-used+1))
                        if not raw:break
                        used+=len(raw)
                        if used>m.st_size or size+used>host.l['state_bytes']:raise ValueError('codex_copyout_byte_bound')
                        digest.update(raw);leaf.update(raw)
                    if (used!=m.st_size or leaf.hexdigest()!=expected['sha256'] or _full(os.fstat(fd))!=_full(m)
                            or _full(os.stat(Path(relative).name,dir_fd=parent,follow_symlinks=False))!=_full(m)):
                        raise ValueError('codex_copyout_file_race')
                finally:os.close(fd)
                files+=1;size+=m.st_size
            finally:os.close(parent)
    if present:walk(key,name)
    return {'name':name,'present':present,'sha256':digest.hexdigest(),'files':files,'bytes':size}


def codex_home_snapshot(host,state):
    """Called only while stopped and holding the existing worker owner lock."""
    out={};dirs=host.inventory_directories
    for relative in sorted(state):
        if not re.fullmatch(r'commands/(?:[0-9a-f]{2})+\.json',relative):continue
        receipt=c.strict(_codex_read(host,relative,state));command=receipt.get('command',{})
        if (command.get('kind')!='run_attempt' or command.get('task',{}).get('definition',{}).get('executor')!='coding'
                or receipt.get('completed') is not True):continue
        attempt=command['attempt'];key=attempt['id'].encode().hex();base='codex/'+key
        if base not in dirs:continue
        if (relative!='commands/'+command['command_id'].encode().hex()+'.json'
                or attempt['worker_id']!=host.h['api']['worker_id'] or key in out):
            raise ValueError('codex_copyout_original_command_binding')
        if not any(n.startswith('cleanup/') for n in state):raise ValueError('codex_copyout_workspace_record_absent')
        sources=[]
        for name in _CODEX_TRANSCRIPTS:
            rel=base+'/'+name;row=state.get(rel);directory=dirs.get(rel)
            entry=None
            if directory is not None:entry={'name':name,**_codex_id(directory),'kind':'directory'}
            elif row is not None:entry={'name':name,'device':row['identity'][0],'inode':row['identity'][1],'kind':'file'}
            sources.append({'name':name,'entry':entry,'signature':_codex_signature(host,base,name,state,dirs)})
        thread=base+'/borg-thread.json';expected=thread in state
        if expected:
            value=c.strict(_codex_read(host,thread,state))
            if not isinstance(value.get('thread_id'),str) or not value['thread_id']:raise ValueError('codex_copyout_thread_binding')
        out[key]={'command':relative,'home':_codex_id(dirs[base]),'parents':[_codex_id(dirs['.']),_codex_id(dirs['codex'])],
                  'transcript_sources':sources,'transcript_expected':expected,
                  'home_metadata':list(dirs[base][:5]),'parent_metadata':[list(dirs[n][:5]) for n in ('.','codex')],
                  'journal_directories':{n:list(m) for n,m in dirs.items() if n.startswith(base+'/') and n[len(base)+1:].split('/')[0] in _CODEX_JOURNALS}}
        if len(out)>host.l['state_entries'] or len(c.canonical(out))>host.l['receipt_bytes']:raise ValueError('codex_copyout_snapshot_bound')
    return out


_CODEX_RETAINED_SOURCES = {
    'src/worker/cleanup_plugins.rs':'c1b9a658179748c196f3b855b26e08a1528162240820015b562f8e3ce1223be0',
    'src/worker/cleanup.rs':'4b0e33c95ae3f0d1c37a4a8f6af8dab9838ec7661c162824d7983b857242d3ea',
    'src/worker/cleanup_copyout.rs':'d1925748bfcf7bbc5415f8e0c1d1bbd05c86681d62afdb5b2d8efa1581648b69'}


_RECOVERED_CODEX_RETAINED_SOURCES = {
    'src/worker/cleanup.rs': '54f15913412870da6984ade05477d09004a28ab07cece96931759d2381bba898',
    'src/worker/cleanup_copyout.rs': '4181eeac29c4869d07fa11ca282e5963259a566240e0cc389d4e4ef2fc0aaa80',
    'src/worker/cleanup_plugins.rs': '247e6241889e1f58d52172f20ee4359a9f9804af73171e8e42c5e3a7c07a42cd',
}


class CodexHomePreservation:
    """Closed proof of original vendor disappearance; no mutable path wildcard.

    The selected producer enforces its local journals and canonical checkpoint
    guards before intent and each removal. This reader independently joins the
    completed claim to original stopped bytes, current authority and archive.
    Racing/incomplete progress refuses; no live directory relaxation is added.
    """
    def __init__(self,host,stop,current):
        self.host,self.stop,self.current=host,stop,current;self.checked={};self.observations={};self.retained={}
        self.saved=stop.get('codex_homes',{})

    def validate(self,key):
        if key in self.checked:return self.checked[key]
        h=self.host;_codex_deadline(h)
        if key not in self.saved:raise ValueError('codex_copyout_unbound_original_home')
        original=self.saved[key];base='codex/'+key;dirs=h.inventory_directories;root=Path(h.h['state']['root'])
        command_path=original['command'];old=self.stop['state'].get(command_path)
        if old is None or self.current.get(command_path)!=old:raise ValueError('codex_copyout_original_command_changed')
        receipt=c.strict(_codex_read(h,command_path,self.current))
        c.fields(receipt,'command completed effects_started','codex command receipt')
        if receipt['completed'] is not True or type(receipt['effects_started']) is not bool:raise ValueError('codex_copyout_command_incomplete')
        command=receipt['command'];attempt=command['attempt']
        if key!=attempt['id'].encode().hex():raise ValueError('codex_copyout_attempt_path')
        authority=h.r.get('cleanup_authority')
        if not isinstance(authority,dict):raise ValueError('codex_copyout_authority_missing')
        c.fields(authority,'worker_id workspace_root tasks binding','codex authority')
        c.fields(authority['binding'],'original_preflight_sha256 current_controller_proof_sha256','codex authority binding')
        if (authority['worker_id']!=h.h['api']['worker_id'] or any(re.fullmatch('[0-9a-f]{64}',v or '') is None for v in authority['binding'].values())):
            raise ValueError('codex_copyout_authority_binding')
        task=authority['tasks'].get(attempt['task_id'])
        if task is None:raise ValueError('codex_copyout_task_absent')
        verify_task(c,command,task,authority['worker_id'])
        name='cleanup-codex/'+key+'.json'
        policy=h.cleanup_policy()
        if policy is None:raise ValueError('codex_copyout_policy_unselected')
        record=c.strict(_codex_read(h,name,self.current,policy['retention']['worker']['record_bytes']))
        c.fields(record,'format_revision command_id attempt_id policy policy_revision status checkpoint first_safe_ms last_observed_ms next_due_ms proof archive exports transcript_expected transcript_sources removing started done reason','codex cleanup progress')
        if (type(record['format_revision']) is not int or record['format_revision']!=1
                or record['command_id']!=command['command_id'] or record['attempt_id']!=attempt['id']
                or not _same(c,record['policy'],policy['retention']['worker']) or record['policy_revision']!=policy['revision']
                or record['status']!=task['status'] or not _same(c,record['checkpoint'],task['selected_checkpoint'])
                or record['started'] is not True or record['done'] is not True or record['reason']!='complete'
                or record['removing'] is not None or record['next_due_ms'] is not None):
            raise ValueError('codex_copyout_completed_binding')
        first,last=record['first_safe_ms'],record['last_observed_ms']
        grace=record['policy'][{'succeeded':'success_grace_ms','failed':'failed_grace_ms','cancelled':'cancelled_grace_ms'}[task['status']]]
        if type(first) is not int or type(last) is not int or not 0<=first<=last<=c.now_ms() or _add(first,grace)>last:
            raise ValueError('codex_copyout_grace_binding')
        proof={'path':str(root/base),'parents':original['parents'],'directory':original['home']}
        if not _same(c,record['proof'],proof):raise ValueError('codex_copyout_home_proof')
        for rel,expected,metadata in zip(('.', 'codex', base),original['parents']+[original['home']],original['parent_metadata']+[original['home_metadata']]):
            meta=dirs.get(rel)
            if meta is None or _codex_id(meta)!=expected or list(meta[:5])!=metadata or meta[2]!=h.h['service_uid'] or stat.S_IMODE(meta[4])&0o022:
                raise ValueError('codex_copyout_home_replaced')
        expected_dirs=original['journal_directories']
        current_dirs={n:list(m) for n,m in dirs.items() if n.startswith(base+'/') and n[len(base)+1:].split('/')[0] in _CODEX_JOURNALS}
        if current_dirs!=expected_dirs:raise ValueError('codex_copyout_journal_directories_changed')
        old_journals={n:v for n,v in self.stop['state'].items() if n.startswith(base+'/') and n[len(base)+1:].split('/')[0] in _CODEX_JOURNALS}
        current_journals={n:v for n,v in self.current.items() if n.startswith(base+'/') and n[len(base)+1:].split('/')[0] in _CODEX_JOURNALS}
        if current_journals!=old_journals:raise ValueError('codex_copyout_journals_changed')
        if any(n.startswith(base+'/') and n[len(base)+1:].split('/')[0] not in _CODEX_JOURNALS for n in set(dirs)|set(self.current)):
            raise ValueError('codex_copyout_vendor_remainder')
        if (record['transcript_expected'] is not original['transcript_expected']
                or not _same(c,record['transcript_sources'],original['transcript_sources'])
                or not _same(c,record['exports'],[s['signature'] for s in original['transcript_sources']])):
            raise ValueError('codex_copyout_original_transcripts_changed')
        if record['transcript_expected'] and not any(e['files']>0 for e in record['exports'] if e['name']!='history.jsonl'):
            raise ValueError('codex_copyout_expected_transcript_absent')
        archive='codex-transcripts/'+key
        if ('codex-transcripts' not in dirs or archive not in dirs or not _same(c,record['archive'],{
                'parent':_codex_id(dirs['codex-transcripts']),'directory':_codex_id(dirs[archive])})):
            raise ValueError('codex_copyout_archive_identity')
        device=original['parents'][0]['device']
        if (any(v['device']!=device for v in original['parents']+[original['home']])
                or any(dirs[n][0]!=device or dirs[n][2]!=h.h['service_uid'] for n in ('codex-transcripts',archive))
                or self.current[name]['identity'][0]!=device or self.current[name]['identity'][2]!=h.h['service_uid']
                or self.current[name]['identity'][4]!=0o600):
            raise ValueError('codex_copyout_foreign_proof_owner_or_device')
        expected={e['name'] for e in record['exports'] if e['present']}
        actual={n[len(archive)+1:].split('/')[0] for n in set(dirs)|set(self.current) if n.startswith(archive+'/')}
        if expected!=actual:raise ValueError('codex_copyout_archive_membership')
        for e in record['exports']:
            if _codex_signature(h,archive,e['name'],self.current,dirs)!=e:raise ValueError('codex_copyout_archive_content')
        workspace='cleanup/'+key+'.json'
        raw=h.cleanup_bytes(workspace,self.current[workspace],min(h.l['receipt_bytes'],record['policy']['record_bytes']))
        w=cleanup_record(raw,workspace,policy)
        if (w['command_id']!=command['command_id'] or w['attempt_id']!=attempt['id'] or w['status']!=task['status']
                or w['checkpoint']!=task['selected_checkpoint'] or not all(s['done'] for s in w['worktrees'])):
            raise ValueError('codex_copyout_workspace_incomplete')
        # Pending reports/acceptance, delegation and live process registries
        # cannot be justified by a completed copyout sidecar.
        for rel in self.current:
            if re.fullmatch(r'outbox/(?:acceptance/)?[^/]+\.json',rel):
                value=c.strict(_codex_read(h,rel,self.current))
                report=value.get('original') if rel.startswith('outbox/acceptance/') else value
                if not isinstance(report,dict) or any(not isinstance(report.get(k),str) or not report[k] for k in ('task_id','attempt_id','worker_id','incarnation')):
                    raise ValueError('codex_copyout_unknown_report_association')
                if report['task_id']==attempt['task_id'] or report['attempt_id']==attempt['id']:
                    raise ValueError('codex_copyout_unacknowledged_report')
            if rel.startswith('delegations/'+key) or (rel.startswith('processes/'+key+'/') and not rel.startswith('processes/'+key+'/home/')):
                raise ValueError('codex_copyout_local_recovery_obligation')
        process_root='processes/'+key
        if any(n.startswith(process_root+'/') and n[len(process_root)+1:].split('/')[0]!='home' for n in dirs):
            raise ValueError('codex_copyout_unknown_process_entry')
        checkpoint=task['selected_checkpoint']
        if checkpoint is not None:
            reply=h.api('get_operation',{'operation_id':checkpoint['operation_id']})
            operation=reply.get('data',{}).get('value',{})
            if (reply.get('data',{}).get('kind')!='operation' or operation.get('id')!=checkpoint['operation_id']
                    or operation.get('task_id')!=attempt['task_id'] or operation.get('attempt_id')!=attempt['id']
                    or operation.get('generation')!=attempt['generation'] or operation.get('kind')!='checkpoint'
                    or operation.get('state')!='settled' or not isinstance(operation.get('outcome'),dict)
                    or operation['outcome'].get('acknowledged') is not True
                    or operation.get('intended')!={k:checkpoint[k] for k in ('registration','repository','commit','remote_ref')}):
                raise ValueError('codex_copyout_checkpoint_unacknowledged')
        self.checked[key]={'progress_sha256':self.current[name]['sha256'],'task_sha256':task['task_sha256'],
                           'archive_exports':record['exports'],'original_stopped_home':original,
                           'authority_binding':authority['binding'],'current_content_verified':True}
        return self.checked[key]

    def observe_retained(self,name,old,new):
        """Current no-removal proof; the old progress body was not captured."""
        if re.fullmatch(r'cleanup-codex/(?:[0-9a-f]{2})+\.json',name) is None:return False
        h=self.host
        if not h.selected_writer_sources(h.retained_writer_sources()):return False
        _codex_deadline(h)
        key=Path(name).stem;original=self.saved.get(key);dirs=h.inventory_directories
        if original is None:raise ValueError('codex_retained_unbound_original_home')
        if (not isinstance(old,dict) or not isinstance(new,dict) or set(old)!={'sha256','identity'} or set(new)!={'sha256','identity'}):
            raise ValueError('codex_retained_original_row_missing')
        a,b=old['identity'],new['identity']
        if (not isinstance(a,list) or not isinstance(b,list) or len(a)!=5 or len(b)!=5
                or any(type(v) is not int or v<0 for v in a+b) or a[:1]+a[2:]!=b[:1]+b[2:]
                or a[2]!=h.h['service_uid'] or a[4]!=0o600):raise ValueError('codex_retained_owner_mode_device')
        policy=h.cleanup_policy()
        if policy is None:raise ValueError('codex_retained_policy_unselected')
        record=c.strict(_codex_read(h,name,self.current,policy['retention']['worker']['record_bytes']))
        c.fields(record,'format_revision command_id attempt_id policy policy_revision status checkpoint first_safe_ms last_observed_ms next_due_ms proof archive exports transcript_expected transcript_sources removing started done reason','codex retained progress')
        # Preserve the original completed-copyout reader for all actual removal
        # states. No failed proof is converted to an observational allowance.
        if record['started'] is not False or record['done'] is not False:return False
        if any(record[k] is not None for k in ('archive','exports','transcript_expected','transcript_sources','removing')):
            raise ValueError('codex_retained_copyout_material_present')
        if record['status'] not in (None,'succeeded','failed','cancelled'):raise ValueError('codex_retained_status_shape')
        command_path=original['command'];command_row=self.stop['state'].get(command_path)
        if command_row is None or self.current.get(command_path)!=command_row:raise ValueError('codex_retained_original_command_changed')
        receipt=c.strict(_codex_read(h,command_path,self.current));c.fields(receipt,'command completed effects_started','codex retained command')
        command=receipt['command'];attempt=command.get('attempt',{})
        if (receipt['completed'] is not True or type(receipt['effects_started']) is not bool
                or command.get('kind')!='run_attempt' or command.get('task',{}).get('definition',{}).get('executor')!='coding'
                or command_path!='commands/'+command['command_id'].encode().hex()+'.json'
                or attempt.get('worker_id')!=h.h['api']['worker_id'] or key!=attempt['id'].encode().hex()
                or type(record['format_revision']) is not int or record['format_revision']!=1
                or record['command_id']!=command['command_id'] or record['attempt_id']!=attempt['id']
                or not _same(c,record['policy'],policy['retention']['worker']) or record['policy_revision']!=policy['revision']):
            raise ValueError('codex_retained_command_policy_binding')
        if record['checkpoint'] is not None:
            # The original workspace sidecar was captured in full, unlike this
            # progress sidecar. Bind a retained checkpoint to those exact old
            # bytes; this does not assert current canonical task settlement.
            workspace='cleanup/'+key+'.json';saved=self.stop.get('cleanup_records',{}).get(workspace)
            if not isinstance(saved,str):raise ValueError('codex_retained_checkpoint_source_absent')
            workspace_raw=saved.encode();workspace_row=self.stop['state'].get(workspace)
            if (workspace_row is None or len(workspace_raw)>policy['retention']['worker']['record_bytes']
                    or c.digest(workspace_raw)!=workspace_row['sha256']):raise ValueError('codex_retained_checkpoint_source_changed')
            workspace_record=cleanup_record(workspace_raw,workspace,policy)
            if (workspace_record['command_id']!=command['command_id'] or workspace_record['attempt_id']!=attempt['id']
                    or not _same(c,record['checkpoint'],workspace_record['checkpoint'])):
                raise ValueError('codex_retained_checkpoint_binding')
        base='codex/'+key;root=Path(h.h['state']['root'])
        proof={'path':str(root/base),'parents':original['parents'],'directory':original['home']}
        if record['proof'] is not None and not _same(c,record['proof'],proof):raise ValueError('codex_retained_home_proof')
        for rel,expected,metadata in zip(('.', 'codex', base),original['parents']+[original['home']],original['parent_metadata']+[original['home_metadata']]):
            meta=dirs.get(rel)
            if meta is None or _codex_id(meta)!=expected or list(meta[:5])!=metadata or meta[2]!=h.h['service_uid'] or stat.S_IMODE(meta[4])&0o022:
                raise ValueError('codex_retained_home_replaced')
        old_home={n:v for n,v in self.stop['state'].items() if n.startswith(base+'/')}
        current_home={n:v for n,v in self.current.items() if n.startswith(base+'/')}
        if current_home!=old_home:raise ValueError('codex_retained_original_home_rows_changed')
        journal_dirs={n:list(m) for n,m in dirs.items() if n.startswith(base+'/') and n[len(base)+1:].split('/')[0] in _CODEX_JOURNALS}
        if journal_dirs!=original['journal_directories']:raise ValueError('codex_retained_journal_directories_changed')
        for source in original['transcript_sources']:
            rel=base+'/'+source['name'];row=self.current.get(rel);directory=dirs.get(rel);entry=None
            if directory is not None:entry={'name':source['name'],**_codex_id(directory),'kind':'directory'}
            elif row is not None:entry={'name':source['name'],'device':row['identity'][0],'inode':row['identity'][1],'kind':'file'}
            if entry!=source['entry'] or _codex_signature(h,base,source['name'],self.current,dirs)!=source['signature']:
                raise ValueError('codex_retained_original_transcripts_changed')
        first,last,due=record['first_safe_ms'],record['last_observed_ms'],record['next_due_ms']
        if (type(last) is not int or not 0<=last<=min(c.now_ms(),2**64-1)
                or type(due) is not int or not 0<=due<=2**64-1):raise ValueError('codex_retained_clock_shape')
        reason=record['reason']
        if reason=='grace':
            grace_key={'succeeded':'success_grace_ms','failed':'failed_grace_ms','cancelled':'cancelled_grace_ms'}.get(record['status'])
            if (grace_key is None or type(first) is not int or not 0<=first<=last<due or record['proof'] is None
                    or due!=_add(first,record['policy'][grace_key])):raise ValueError('codex_retained_grace_binding')
        elif reason in ('uncertain_effect','canonical_binding'):
            if first is not None or due!=_add(last,record['policy']['retry_delay_ms']):raise ValueError('codex_retained_hold_binding')
        else:raise ValueError('codex_retained_reason_outside_scope')
        evidence={'progress_sha256':new['sha256'],'original_progress_row':copy.deepcopy(old),
            'sampled_progress_row':copy.deepcopy(new),'original_command_row':copy.deepcopy(command_row),
            'original_home_rows':len(old_home),'original_home_rows_sha256':c.digest(c.canonical(old_home)),
            'original_stopped_home':original,'current_reason':reason,'last_observed_ms':last,'next_due_ms':due,
            'current_not_started':True,'current_not_done':True,'original_home_file_rows_exact':True,
            'old_progress_body_available':False,'unchanged_progress_fields_claimed':False,
            'canonical_settlement_claimed':False,'reclamation_claimed':False,'simultaneous_snapshot':False}
        self.retained[key]=evidence
        if len(self.retained)>h.l['state_entries'] or len(c.canonical(self.retained))>h.l['receipt_bytes']:raise ValueError('codex_retained_proof_bound')
        self.observations[name]={'original':old,'observed':new,'retained_home_proof_sha256':c.digest(c.canonical(evidence)),
                                 'original_home_file_rows_exact':True,'reclamation_claimed':False}
        return True

    def observe(self,name,old,new):
        parts=Path(name).parts
        if len(parts)>=3 and parts[0]=='codex' and re.fullmatch(r'(?:[0-9a-f]{2})+',parts[1]) and parts[2] not in _CODEX_JOURNALS:
            if new is not None:raise ValueError('codex_copyout_vendor_modified_not_removed')
            key=parts[1]
        elif len(parts)==2 and parts[0]=='cleanup-codex' and re.fullmatch(r'(?:[0-9a-f]{2})+\.json',parts[1]):
            if new is None:raise ValueError('codex_copyout_progress_removed')
            if self.observe_retained(name,old,new):return True
            key=parts[1][:-5]
        else:return False
        proof=self.validate(key)
        self.observations[name]={'original':old,'observed':new,'copyout_proof_sha256':c.digest(c.canonical(proof)),
                                 'original_bytes_preserved':name.startswith('codex/'+key+'/') and Path(name).parts[2] in _CODEX_TRANSCRIPTS}
        return True

    def finish(self):
        for name,row in self.current.items():
            if name.startswith('cleanup-codex/') and self.stop['state'].get(name)!=row:
                if re.fullmatch(r'cleanup-codex/(?:[0-9a-f]{2})+\.json',name) is None:raise ValueError('codex_copyout_progress_path')
                key=Path(name).stem
                if key not in self.retained:self.validate(key)
        for key in self.retained:
            _codex_read(self.host,'cleanup-codex/'+key+'.json',self.current)
        # A readback claims a stable sample, never synchronization with a writer.
        # Recheck all validated proof bodies against the sampled inventory.
        for key in self.checked:
            _codex_read(self.host,'cleanup-codex/'+key+'.json',self.current)
            for e in self.checked[key]['archive_exports']:
                if _codex_signature(self.host,'codex-transcripts/'+key,e['name'],self.current,self.host.inventory_directories)!=e:
                    raise ValueError('codex_copyout_archive_changed_after_read')

    def proof(self):
        return {'status':'SCOPED_CODEX_HOME_PRESERVATION' if self.retained else 'SCOPED_COMPLETED_CODEX_HOME_PRESERVATION','attempts':self.checked,
                **({'retained_attempts':self.retained,'retained_source_files':self.host.retained_writer_sources()} if self.retained else {}),
                'observed_changes':len(self.observations),'same_format_contract_changed':False,
                'other_original_rows_exact':True,'host_effects':False,'simultaneous_snapshot':False}


# This current writer deliberately re-establishes manifest durability during
# recovery. Its immutable chunks and original command remain separate records.
_CHARTER_MANIFEST_SOURCES = {
    'src/worker/charter_output.rs': '77ee76cbb97a69fdd08b510ff3b9404afc64caa02a9fd6db350eba7a4403635b',
    'src/worker/process.rs': 'c13c470c5b414e0df7793e5140095344df09f10f1f6888c0431533bfbf93a6bb',
    'src/worker/charter.rs': '700374cb2f81b3f1c17ffa37d3f8cb7bf3915bf9569843354f6c13d794320516',
}


_CHARTER_JOURNAL_SOURCES = {**_CHARTER_MANIFEST_SOURCES,
    'src/worker/mutations.rs':'e510c3020ca1476ac3d2bda9762b2cee11d2f29d1ac552472d9218b9237e0553'}


_RECOVERED_CHARTER_JOURNAL_SOURCES = {
    'src/worker/charter.rs': '700374cb2f81b3f1c17ffa37d3f8cb7bf3915bf9569843354f6c13d794320516',
    'src/worker/charter_output.rs': '77ee76cbb97a69fdd08b510ff3b9404afc64caa02a9fd6db350eba7a4403635b',
    'src/worker/mutations.rs': 'f3d1c510541f125d531d908624cdad4238fa094e8abe3c8dbbdc7f59b0fd3486',
    'src/worker/process.rs': 'c13c470c5b414e0df7793e5140095344df09f10f1f6888c0431533bfbf93a6bb',
}


class CharterManifestPreservation:
    """Same-byte atomic manifest replacement; no new output or task authority."""
    def __init__(self,host,stop,current):
        self.host,self.stop,self.current=host,stop,current
        self.observations={};self.journals={};self.commands=None
        retained=host.retained_policy()
        self.bound=min(host.l['state_bytes'],host.l['receipt_bytes'],retained['worker']['command_record_bytes'])

    def deadline(self):
        if c.now_ms()>=self.host.r['deadline_ms']:raise ValueError('charter_manifest_deadline')

    def original_commands(self):
        if self.commands is not None:return self.commands
        values=[];total=0
        for name,expected in self.stop['state'].items():
            self.deadline()
            if not name.startswith('commands/'):continue
            if re.fullmatch(r'commands/(?:[0-9a-f]{2})+\.json',name) is None:raise ValueError('charter_manifest_command_path')
            if self.current.get(name)!=expected:raise ValueError('charter_manifest_original_command_changed')
            raw,identity=_file(Path(self.host.h['state']['root'])/name,self.bound,self.host.h['service_uid'])
            if {'sha256':c.digest(raw),'identity':identity}!=expected:raise ValueError('charter_manifest_command_sample_changed')
            total+=len(raw)
            if total>self.host.l['state_bytes']:raise ValueError('charter_manifest_command_bytes')
            record=c.strict(raw);command=record.get('command')
            if not isinstance(command,dict):raise ValueError('charter_manifest_command_shape')
            if command.get('kind')!='run_attempt':continue
            if record.get('completed') is not True or name!='commands/'+command['command_id'].encode().hex()+'.json':raise ValueError('charter_manifest_command_identity')
            attempt=command['attempt']
            caller={'task_id':attempt['task_id'],'attempt_id':attempt['id'],'generation':attempt['generation'],
                    'worker_id':attempt['worker_id'],'incarnation':attempt['worker_incarnation']}
            if caller['worker_id']!=self.host.h['api']['worker_id']:raise ValueError('charter_manifest_command_worker')
            values.append((caller,command,name,expected))
            if len(values)>self.host.l['state_entries']:raise ValueError('charter_manifest_command_count')
        self.commands=values
        return values

    def observe(self,name,old,new):
        if not isinstance(name,str) or re.fullmatch(r'charter-output/[0-9a-f]{64}/manifest\.json',name) is None:return False
        self.deadline()
        if (not isinstance(old,dict) or not isinstance(new,dict) or set(old)!={'sha256','identity'}
                or set(new)!={'sha256','identity'} or old['sha256']!=new['sha256']):
            raise ValueError('charter_manifest_content_or_presence_changed')
        a,b=old['identity'],new['identity']
        if (not isinstance(a,list) or not isinstance(b,list) or len(a)!=5 or len(b)!=5
                or any(type(v) is not int or v<0 for v in a+b)
                or a[:1]+a[2:]!=b[:1]+b[2:] or a[2]!=self.host.h['service_uid'] or a[4]!=0o600):
            raise ValueError('charter_manifest_identity_changed')
        raw,identity=_file(Path(self.host.h['state']['root'])/name,self.bound,self.host.h['service_uid'])
        if {'sha256':c.digest(raw),'identity':identity}!=new:raise ValueError('charter_manifest_sample_changed')
        manifest=c.strict(raw);c.fields(manifest,'revision caller operation_id observation','charter manifest')
        caller=manifest['caller'];c.fields(caller,'task_id attempt_id generation worker_id incarnation','charter manifest caller')
        if (type(manifest['revision']) is not int or manifest['revision']!=1
                or type(caller['generation']) is not int or not 0<=caller['generation']<=2**64-1
                or any(not isinstance(caller[k],str) or not caller[k] for k in ('task_id','attempt_id','worker_id','incarnation'))):
            raise ValueError('charter_manifest_revision_or_caller')
        observation=manifest['observation'];kind=observation.get('kind') if isinstance(observation,dict) else None
        fields={'reply':'kind turn_id reply','supplied_reply':'kind turn_id terminal reply','compaction_reply':'kind run_id reply summary'}
        if kind not in fields:raise ValueError('charter_manifest_observation_kind')
        c.fields(observation,fields[kind],'charter manifest observation')
        occurrence=observation['run_id' if kind=='compaction_reply' else 'turn_id']
        if not isinstance(occurrence,str) or not occurrence or manifest['operation_id']!=occurrence+'-reply':raise ValueError('charter_manifest_operation_identity')
        directory='charter-output/'+c.digest(manifest['operation_id'].encode())
        if name!=directory+'/manifest.json':raise ValueError('charter_manifest_directory_identity')
        reply=observation['reply']
        if not isinstance(reply,dict) or 'output' in reply:raise ValueError('charter_manifest_inline_output')
        matches=[row for row in self.original_commands() if c.canonical(row[0])==c.canonical(caller)]
        if len(matches)!=1:raise ValueError('charter_manifest_unique_original_command')
        _,command,command_name,command_row=matches[0]
        accepted=command['task']['resolved_limits']['global_metadata']
        policy=accepted.get('charter')
        if not isinstance(policy,dict) or type(policy.get('record_bytes')) is not int or not 0<len(raw)<=policy['record_bytes']:
            raise ValueError('charter_manifest_accepted_record_bound')
        reference=reply.get('output_reference');expected_chunks=set()
        if reference is not None:
            c.fields(reference,'request_id bytes sha256 chunk_bytes chunks','charter output reference')
            if (reference['request_id']!=occurrence or any(type(reference[k]) is not int or not 0<reference[k]<=2**32-1 for k in ('chunk_bytes','chunks'))
                    or type(reference['bytes']) is not int or reference['bytes']<=0
                    or type(policy.get('records')) is not int or not 0<reference['chunks']<=min(policy['records'],self.host.l['state_entries'])
                    or reference['chunks']!=(reference['bytes']+reference['chunk_bytes']-1)//reference['chunk_bytes']
                    or not isinstance(reference['sha256'],str) or re.fullmatch('[0-9a-f]{64}',reference['sha256']) is None):
                raise ValueError('charter_manifest_output_reference')
            expected_chunks={directory+'/'+format(i,'010d')+'.chunk' for i in range(reference['chunks'])}
        original_names={n for n in self.stop['state'] if n.startswith(directory+'/')}
        current_names={n for n in self.current if n.startswith(directory+'/')}
        if original_names!={name}|expected_chunks or current_names!=original_names:raise ValueError('charter_manifest_original_chunk_membership')
        if any(self.current[n]!=self.stop['state'][n] for n in expected_chunks):raise ValueError('charter_manifest_original_chunk_changed')
        key=c.digest(name.encode())
        proof={'relative_sha256':key,'original':copy.deepcopy(old),'sampled':copy.deepcopy(new),
               'manifest_sha256':c.digest(raw),'operation_id_sha256':c.digest(manifest['operation_id'].encode()),
               'caller_sha256':c.digest(c.canonical(caller)),'observation_kind':kind,
               'command_relative_sha256':c.digest(command_name.encode()),'original_command_row':copy.deepcopy(command_row),
               'original_chunk_rows':len(expected_chunks),'original_chunk_rows_sha256':c.digest(c.canonical({n:self.stop['state'][n] for n in sorted(expected_chunks)})),
               'original_content_exact':True,'inode_only':True,'inode_changed':a[1]!=b[1],'original_command_caller_join':True,
               'source_writer_may_replace_inode':True,'actual_writer_attributed':False,'new_output_authority':False}
        self.observations[key]=proof
        if len(self.observations)>self.host.l['state_entries'] or len(c.canonical(self.observations))>self.host.l['receipt_bytes']:raise ValueError('charter_manifest_observation_bound')
        return True

    def observe_journal(self,name,old,new):
        if not isinstance(name,str) or re.fullmatch(r'charter/(?:[0-9a-f]{2})+\.json',name) is None:return False
        self.deadline()
        if (not isinstance(old,dict) or not isinstance(new,dict) or set(old)!={'sha256','identity'}
                or set(new)!={'sha256','identity'} or old['sha256']!=new['sha256']):
            raise ValueError('charter_journal_content_or_presence_changed')
        a,b=old['identity'],new['identity']
        if (not isinstance(a,list) or not isinstance(b,list) or len(a)!=5 or len(b)!=5
                or any(type(v) is not int or v<0 for v in a+b)
                or a[:1]+a[2:]!=b[:1]+b[2:] or a[2]!=self.host.h['service_uid'] or a[4]!=0o600):
            raise ValueError('charter_journal_identity_changed')
        bound=min(self.host.l['state_bytes'],self.host.l['receipt_bytes'],self.host.retained_policy()['worker']['mutation_record_bytes'])
        raw,identity=_file(Path(self.host.h['state']['root'])/name,bound,self.host.h['service_uid'])
        if {'sha256':c.digest(raw),'identity':identity}!=new:raise ValueError('charter_journal_sample_changed')
        journal=c.strict(raw);c.fields(journal,'limits revision request outcome outcome_context consumed recovery_epoch','charter journal')
        if (type(journal['revision']) is not int or journal['revision']!=3 or journal['consumed'] is not True
                or type(journal['recovery_epoch']) is not int or journal['recovery_epoch']!=0):
            raise ValueError('charter_journal_original_settled_scope')
        request=journal['request'];c.fields(request,'operation input','charter journal request')
        body=request['input'];c.fields(body,'operation_id caller observation','charter journal input')
        operation=body['operation_id']
        if (request['operation']!='observe_charter' or not isinstance(operation,str) or not operation
                or name!='charter/'+operation.encode().hex()+'.json'):
            raise ValueError('charter_journal_request_identity')
        observation=body['observation'];kind=observation.get('kind') if isinstance(observation,dict) else None
        if kind not in ('reply','supplied_reply','compaction_reply','output_chunk'):raise ValueError('charter_journal_observation_kind')
        if kind=='output_chunk':
            c.fields(observation,'kind chunk','charter chunk observation')
            chunk=observation['chunk'];c.fields(chunk,'reference index data','charter chunk')
            reference=chunk['reference'];occurrence=reference.get('request_id') if isinstance(reference,dict) else None
        else:occurrence=observation.get('run_id' if kind=='compaction_reply' else 'turn_id')
        if not isinstance(occurrence,str) or not occurrence:raise ValueError('charter_journal_occurrence')
        manifest_name='charter-output/'+c.digest((occurrence+'-reply').encode())+'/manifest.json'
        manifest_old=self.stop['state'].get(manifest_name);manifest_new=self.current.get(manifest_name)
        # The paired original manifest proves caller, operation, accepted command
        # and unchanged complete chunk membership before a journal is admitted.
        self.observe(manifest_name,manifest_old,manifest_new)
        manifest_raw,manifest_identity=_file(Path(self.host.h['state']['root'])/manifest_name,self.bound,self.host.h['service_uid'])
        if {'sha256':c.digest(manifest_raw),'identity':manifest_identity}!=manifest_new:raise ValueError('charter_journal_manifest_changed')
        manifest=c.strict(manifest_raw)
        if not _same(c,body['caller'],manifest['caller']):raise ValueError('charter_journal_caller_changed')
        matches=[row for row in self.original_commands() if _same(c,row[0],body['caller'])]
        if len(matches)!=1:raise ValueError('charter_journal_unique_original_command')
        _,command,command_name,command_row=matches[0];accepted=command['task']['resolved_limits']['global_metadata']
        if (not _same(c,journal['limits'],accepted) or len(raw)>accepted['worker']['mutation_record_bytes']
                or len(c.canonical(request))>accepted['transport']['body_bytes']):
            raise ValueError('charter_journal_accepted_limits')
        outcome=journal['outcome'];c.fields(outcome,'status data','charter journal outcome')
        data=outcome['data'];c.fields(data,'kind value','charter journal data')
        result=data['value'];c.fields(result,'kind value','charter journal result')
        if outcome['status']!='ok' or data['kind']!='charter' or len(c.canonical(outcome))>accepted['transport']['body_bytes']:
            raise ValueError('charter_journal_original_outcome')
        if kind=='output_chunk':
            expected_ref=manifest['observation']['reply'].get('output_reference');index=chunk['index'];encoded=chunk['data']
            if (expected_ref is None or not _same(c,reference,expected_ref) or type(index) is not int or not 0<=index<reference['chunks']
                    or operation!='charter-output-'+c.digest(occurrence.encode())+'-'+str(index)
                    or not isinstance(encoded,str) or re.fullmatch(r'(?:[0-9a-f]{2})+',encoded) is None):
                raise ValueError('charter_journal_chunk_identity')
            chunk_raw=bytes.fromhex(encoded);chunk_name=str(Path(manifest_name).parent)+'/'+format(index,'010d')+'.chunk'
            expected_size=min(reference['chunk_bytes'],reference['bytes']-index*reference['chunk_bytes'])
            expected={'reference':reference,'index':index,'sha256':c.digest(chunk_raw),'bytes':len(chunk_raw)}
            if (len(chunk_raw)!=expected_size or self.stop['state'][chunk_name]['sha256']!=expected['sha256']
                    or result['kind']!='output_chunk' or not _same(c,result['value'],expected)):
                raise ValueError('charter_journal_chunk_receipt')
        else:
            if operation!=manifest['operation_id'] or not _same(c,observation,manifest['observation']):raise ValueError('charter_journal_manifest_request')
            value=result['value']
            if kind=='compaction_reply':
                if result['kind']!='compaction' or not isinstance(value,dict) or value.get('run_id')!=occurrence or not _same(c,value.get('caller'),body['caller']):raise ValueError('charter_journal_compaction_result')
                value=value.get('reply')
            elif result['kind']!='reply':raise ValueError('charter_journal_reply_result')
            c.fields(value,'reply_id turn_id effective observation audit','charter journal reply')
            if value['reply_id']!=operation or value['turn_id']!=occurrence or not _same(c,value['observation'],observation['reply']):raise ValueError('charter_journal_reply_identity')
        context=journal['outcome_context'];c.fields(context,'commit observed_controller_revision','charter journal context')
        commit=context['commit'];c.fields(commit,'operation_id controller_revision','charter journal commit')
        revision,observed=commit['controller_revision'],context['observed_controller_revision']
        if (commit['operation_id']!=operation or type(revision) is not int or type(observed) is not int
                or not 0<revision<=observed<=2**63-1):raise ValueError('charter_journal_original_commit')
        key=c.digest(name.encode())
        self.journals[key]={'relative_sha256':key,'original':copy.deepcopy(old),'sampled':copy.deepcopy(new),
            'original_content_exact':True,'inode_only':True,'inode_changed':a[1]!=b[1],
            'manifest_relative_sha256':c.digest(manifest_name.encode()),'manifest_sha256':manifest_old['sha256'],
            'command_relative_sha256':c.digest(command_name.encode()),'original_command_row':copy.deepcopy(command_row),
            'observation_kind':kind,'original_context_sha256':c.digest(c.canonical(context)),
            'original_request_sha256':c.digest(c.canonical(request)),'original_outcome_sha256':c.digest(c.canonical(outcome)),
            'source_writer_may_replace_inode':True,'actual_writer_attributed':False,'new_operation_authority':False}
        if len(self.journals)>self.host.l['state_entries'] or len(c.canonical(self.journals))>self.host.l['receipt_bytes']:raise ValueError('charter_journal_observation_bound')
        return True

    def proof(self):
        return {'status':'SCOPED_ORIGINAL_CHARTER_MANIFEST_OBSERVATIONS','records':len(self.observations),
                'observations':copy.deepcopy(self.observations),'source_files':_CHARTER_MANIFEST_SOURCES,
                'journal_observations':copy.deepcopy(self.journals),'journal_source_files':self.host.charter_journal_sources(),
                'other_history_guards_unchanged':True,'host_effects':False,'directory_allowances_added':False,
                'simultaneous_snapshot':False,'original_content_and_noninode_identity_preserved':True}


class StopMarkers:
    def __init__(self,host,c,stop):
        self.host,self.c,self.stop=host,c,stop;self.observations={}
        retained=host.retained_policy()
        self.bound=min(host.l['state_bytes'],host.l['receipt_bytes'],retained['worker']['command_record_bytes'])

    def observe(self,name,old,new,stop,current):
        if not isinstance(name,str) or re.fullmatch(r'stops/(?:[0-9a-f]{2})+',name) is None:return False
        c=self.c;host=self.host
        if c.now_ms()>=host.r['deadline_ms']:raise ValueError('stop_marker_deadline')
        if stop!=self.stop:raise ValueError('stop_marker_original_snapshot')
        if (not isinstance(old,dict) or not isinstance(new,dict) or set(old)!={'sha256','identity'}
                or set(new)!={'sha256','identity'} or old['sha256']!=new['sha256']):
            raise ValueError('stop_marker_content_or_presence_changed')
        a,b=old['identity'],new['identity']
        if (not isinstance(a,list) or not isinstance(b,list) or len(a)!=5 or len(b)!=5
                or any(type(v) is not int or v<0 for v in a+b)
                or a[:1]+a[2:]!=b[:1]+b[2:] or a[2]!=host.h['service_uid'] or a[4]!=0o600):
            raise ValueError('stop_marker_identity_changed')
        raw,identity=_file(Path(host.h['state']['root'])/name,self.bound,host.h['service_uid'])
        if identity!=new['identity'] or c.digest(raw)!=new['sha256']:
            raise ValueError('stop_marker_sample_changed')
        command=c.strict(raw)
        c.fields(command,'kind command_id task_id attempt_id generation incarnation','stop marker command')
        if (command['kind']!='stop_attempt'
                or any(not isinstance(command[k],str) or not command[k] for k in ('command_id','task_id','attempt_id','incarnation'))
                or type(command['generation']) is not int or not 0<=command['generation']<=2**64-1
                or name!='stops/'+command['attempt_id'].encode().hex()):
            raise ValueError('stop_marker_typed_path')
        command_name='commands/'+command['command_id'].encode().hex()+'.json'
        expected=stop['state'].get(command_name)
        if expected is None or current.get(command_name)!=expected:
            raise ValueError('stop_marker_original_command_changed')
        command_raw,command_identity=_file(Path(host.h['state']['root'])/command_name,self.bound,host.h['service_uid'])
        if {'sha256':c.digest(command_raw),'identity':command_identity}!=expected:
            raise ValueError('stop_marker_original_command_sample')
        receipt=c.strict(command_raw)
        if not isinstance(receipt,dict) or c.canonical(receipt.get('command'))!=c.canonical(command):
            raise ValueError('stop_marker_original_command_join')
        key=c.digest(name.encode())
        self.observations[key]={'relative_sha256':key,'original':copy.deepcopy(old),'sampled':copy.deepcopy(new),
            'current_read_matched_captured_row':True,'original_content_exact':True,'inode_only':True,
            'command_relative_sha256':c.digest(command_name.encode()),'original_command_row':copy.deepcopy(expected),
            'command_sha256':c.digest(c.canonical(command)),'typed_stop_attempt_join':True,
            'source_writer_may_replace_inode':True,'actual_writer_attributed':False}
        if len(self.observations)>host.l['state_entries'] or len(c.canonical(self.observations))>host.l['receipt_bytes']:
            raise ValueError('stop_marker_observation_bound')
        return True

    def proof(self):
        return {'status':'SCOPED_ORIGINAL_STOP_MARKER_OBSERVATIONS','records':len(self.observations),
            'observations':copy.deepcopy(self.observations),'implementation':'direct_history_branch',
            'original_state_rows_preserved':True,'sampled_rows_explicit':True,'other_history_guards_unchanged':True,
            'host_effects':False,'directory_allowances_added':False}

def _snapshot_identity(value):
    return [value.st_dev, value.st_ino, value.st_uid, value.st_gid,
            value.st_mode, value.st_nlink, value.st_size,
            value.st_mtime_ns, value.st_ctime_ns]

def controller_snapshot_read(host, relative, pin=None, retain=True):
    common=c
    """Fixed protected file, stable descriptor/entry, streaming hash, no writes."""
    def deadline():
        if common.now_ms() >= host.r['deadline_ms']:
            raise ValueError('receipt_proof_deadline')
    deadline()
    path = common.protected(host.work / relative)
    before = path.lstat()
    bound = min(host.l['state_bytes'], host.l['receipt_bytes'])
    if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
            or before.st_uid != host.h['helper_uid'] or before.st_gid != os.getegid()
            or stat.S_IMODE(before.st_mode) != 0o600 or before.st_size > bound):
        raise ValueError('receipt_proof_file_metadata')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    chunks = [] if retain else None
    total = 0
    digest = hashlib.sha256()
    try:
        if _snapshot_identity(os.fstat(fd)) != _snapshot_identity(before):
            raise ValueError('receipt_proof_open_race')
        while True:
            deadline()
            chunk = os.read(fd, min(host.l['io_chunk_bytes'], bound + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > bound:
                raise ValueError('receipt_proof_byte_bound')
            digest.update(chunk)
            if retain:
                chunks.append(chunk)
        if (_snapshot_identity(os.fstat(fd)) != _snapshot_identity(before)
                or _snapshot_identity(path.lstat()) != _snapshot_identity(before) or total != before.st_size):
            raise ValueError('receipt_proof_file_changed')
    finally:
        os.close(fd)
    proof = {'sha256': digest.hexdigest(), 'bytes': total, 'identity': _snapshot_identity(before)}
    if pin is not None and (pin['sha256'] != proof['sha256'] or pin['bytes'] != total):
        raise ValueError('original_phase_pin_changed')
    deadline()
    return (common.strict(b''.join(chunks)) if retain else None), proof

def verify_controller_receipts(host):
    common=c
    """Use only saved JSON and GetOperation; return IDs and non-body proof."""
    names = {phase + suffix for phase in ('stop', 'install', 'start')
             for suffix in ('.intent.json', '.result.json')}
    if host.h['role'] != 'controller':
        raise ValueError('controller_receipt_scope')
    values, proof = {}, {}
    for name in sorted(names):
        values[name], proof[name] = controller_snapshot_read(host, name)
    for phase in ('stop', 'install', 'start'):
        intent = values[phase + '.intent.json']
        if intent.get('phase') != phase:
            raise ValueError('original_phase_identity')
        for key in ('revision', 'action_id', 'subject', 'profile_path', 'profile_sha256', 'profile_bytes','dispatch_until_ms'):
            if key not in intent or key not in host.r or not _same(common, intent[key], host.r[key]):
                raise ValueError('original_phase_identity')
    before = values['stop.result.json']['snapshot']
    stopped, proof['stopped-copy/snapshot.json'] = controller_snapshot_read(host, 'stopped-copy/snapshot.json')
    if not _same(common, before, stopped):
        raise ValueError('original_stopped_snapshot_changed')
    del stopped
    restart, proof['restart-copy/snapshot.json'] = controller_snapshot_read(host, 'restart-copy/snapshot.json')
    plan = common.controller_receipt_plan(host.p,before,restart,manifest=getattr(host,'_verified_manifest',None))
    ids = [row['id'] for row in common.controller_receipt_rows(before,'receipts',host.l['sqlite_rows']).values()]
    external_count = 0
    compact_count = 0
    for expected in plan:
        if common.now_ms() >= host.r['deadline_ms']:
            raise ValueError('receipt_proof_deadline')
        common.verify_controller_lookup(expected,host.api('get_operation',{'operation_id':expected['id']}))
        external_count += expected['namespace'] == 'external'
        compact_count += expected['namespace'] == 'committed_read_reference'
    for name in sorted(proof):
        _, after = controller_snapshot_read(host, name, retain=False)
        if not _same(common, proof[name], after):
            raise ValueError('receipt_proof_sample_changed')
    return {'preserved_controller_receipts': ids, 'proof': {
        'status': 'PASS_ORIGINAL_RAW_SNAPSHOTS_AND_LIVE_NAMESPACES',
        'files': proof, 'original_receipts': len(ids), 'live_lookups': len(plan),
        'application_mutation_lookups': len(plan) - external_count - compact_count,
        'external_operation_lookups': external_count,
        **({'compact_read_lookups':compact_count,
            'compact_read_source_files':common.compact_receipt_sources(host.p,getattr(host,'_verified_manifest',None)),
            'compact_scope':'already_compact_original_rows_only',
            'original_body_reconstructed':False} if compact_count else {}),
        'raw_receipts_scope': 'original_stopped_and_original_restart_snapshots',
        'current_raw_receipts_read': False, 'fabricated_commit_stamp': False,
        'live_database_opened': False, 'host_mutations': False}}


_GUARD_CATEGORIES = {'cleanup','mutations','processes','commands','stops','codex',
                     'publications','pull-requests','launches','reports','checkpoints'}
_GUARD_METADATA = ('device','inode','uid','gid','mode','nlink','bytes','mtime_ns','ctime_ns')


def _guard_location(name):
    parts=Path(name).parts
    return {'category':'root' if name=='.' else parts[0] if parts and parts[0] in _GUARD_CATEGORIES else 'other',
            'relative_sha256':c.digest(os.fsencode(name)),'depth':len(parts)}


def directory_refusal(site,name,expected,observed):
    # Inputs are the exact metadata tuples used by the failed comparison.
    return c.GuardFailure('state_directory_changed',{
        'kind':'directory','site':site,**_guard_location(name),
        'changed_fields':[key for key,a,b in zip(_GUARD_METADATA,expected,observed) if a!=b],
        'expected':list(expected),'observed':list(observed)})


def history_refusal(site,name,expected,observed):
    # Forward only the closed inventory row, never the retained body or path.
    fields=('sha256','identity','kind','bytes','mtime_ns','ctime_ns')
    def row(value):
        return None if value is None else {key:value[key] for key in fields if key in value}
    before,after=row(expected),row(observed)
    changes=['present'] if before is None or after is None else [key for key in fields if before.get(key)!=after.get(key)]
    return c.GuardFailure('original_worker_history_changed',{
        'kind':'history','site':site,**_guard_location(name),
        'changed_fields':changes,'expected':before,'observed':after})

class Host:
    def __init__(self,profile,host,subject,request):
        c.fields(subject,'release_id artifact repository target_ref expected_base policy deployment_target target_revision installed_package_sha256','subject')
        for key in ('policy','deployment_target','target_revision'):
            if subject[key]!=profile[key]:raise ValueError('host subject/profile differs')
        controller=next(h for h in profile['hosts'] if h['role']=='controller')
        if subject['installed_package_sha256']!=controller['old_artifact']['package_sha256']:raise ValueError('host controller baseline differs')
        # Only the two explicitly selected current boundaries qualify exact baseline
        # hosts. Historical forward kinds still require one before-package.
        if c.per_host_forward(profile):c.format_contract(profile)
        if c.forward(profile) and not c.per_host_forward(profile) and subject['installed_package_sha256']!=host['old_artifact']['package_sha256']:raise ValueError('host installed baseline differs')
        self.p=profile;self.h=host;self.s=subject;self.r=request;self.l=profile['limits']
        self.after=False
        self.root=Path(host['state_root']);c.protected(self.root,directory=True)
        if os.geteuid()!=host['helper_uid']: raise ValueError('host helper UID differs')
        self.work=self.root/('action-'+c.digest(c.canonical({'action':request['action_id'],'subject':subject})))
    def command(self,args):
        c.pin(self.h['systemctl'],self.l['module_bytes'])
        deadline=self.r['deadline_ms']
        if args[0] in ('stop','start','daemon-reload'):deadline=min(deadline,self.r['dispatch_until_ms']-self.l['clock_uncertainty_ms'])
        return c.run([self.h['systemctl']['path'],*args],self.l,deadline)
    def service(self):
        names=['ActiveState','SubState','MainPID','ControlGroup','FragmentPath','DropInPaths','NeedDaemonReload','InvocationID','User']
        raw=self.command(['show',self.h['service'],'--property='+','.join(names)]).decode()
        values={}
        for line in raw.splitlines():
            k,sep,v=line.partition('=')
            if not sep or k not in names or k in values: raise ValueError('service observation shape')
            values[k]=v
        if set(values)!=set(names) or values['FragmentPath']!=self.h['unit']['path'] or values['DropInPaths'] or values['NeedDaemonReload']!='no': raise ValueError('service identity/drop-ins/reload changed')
        return values
    def pids(self,row):
        group=row['ControlGroup']
        if not group: return []
        if not group.startswith('/') or '..' in Path(group).parts: raise ValueError('service cgroup identity')
        root=Path('/sys/fs/cgroup')/group.lstrip('/');out=set();count=0
        for path,dirs,files in os.walk(root,followlinks=False,onerror=_walk_error):
            count+=len(dirs)+len(files)
            if count>self.l['state_entries']: raise ValueError('cgroup entry bound')
            if any((Path(path)/n).is_symlink() for n in dirs+files): raise ValueError('cgroup alias')
            if 'cgroup.procs' in files:
                raw=c.read(Path(path)/'cgroup.procs',self.l['command_output_bytes'])
                out.update(int(x) for x in raw.split())
        return sorted(out)
    def pid_binary(self,pid):
        path=Path('/proc')/str(pid)/'exe'
        with path.open('rb') as f:
            raw=f.read(self.l['package_bytes']+1)
        if len(raw)>self.l['package_bytes']: raise ValueError('running executable bound')
        return c.digest(raw),str(path.resolve())
    def check_running(self,new):
        row=self.service();a=self.s['artifact'] if new else self.h['old_artifact']
        unit=self.h['new_unit'] if new else self.h['unit']
        if c.digest(c.read(self.h['unit']['path'],self.l['module_bytes'],True))!=unit['sha256']: raise ValueError('current unit differs')
        if row['ActiveState']!='active' or row['SubState']!='running' or int(row['MainPID'])<=0: raise ValueError('service not running')
        installed=Path(self.h['release_root'])/a['package_sha256']
        manifest=c.read(installed/'manifest.json',self.l['module_bytes'],True)
        if c.digest(manifest)!=a['manifest_sha256']:raise ValueError('installed manifest differs')
        if not new:c.verify_before_manifest(self.p,self.h,manifest)
        sha,path=self.pid_binary(int(row['MainPID']))
        if sha!=a['binary_sha256'] or path!=str(Path(self.h['release_root'])/a['package_sha256']/'borg-runtime'): raise ValueError('running package/binary differs')
        status=c.read(Path('/proc')/row['MainPID']/'status',self.l['command_output_bytes']).decode()
        uids=next(line.split()[1:] for line in status.splitlines() if line.startswith('Uid:'))
        if len(uids)!=4 or any(int(x)!=self.h['service_uid'] for x in uids): raise ValueError('service process UID differs')
        if ('--limits '+self.h['state']['policy']['path']).encode() not in c.read(self.h['unit']['path'],self.l['module_bytes'],True):raise ValueError('service runtime policy path differs')
        if self.h['role']=='worker' and b'--incarnation ${INVOCATION_ID}' not in c.read(self.h['unit']['path'],self.l['module_bytes'],True): raise ValueError('worker incarnation convention not present in pinned unit')
        if self.h['role']=='controller':
            flag=('--protected-release-config '+self.p['controller_config']['path']).encode()
            if flag not in c.read(self.h['unit']['path'],self.l['module_bytes'],True): raise ValueError('service protected configuration binding differs')
        if int(row['MainPID']) not in self.pids(row): raise ValueError('main process outside service group')
        if not new:self._verified_before_manifest=c.strict(manifest)
        return row
    def check_stopped_predecessor(self):
        # Caller holds the existing worker owner lock. The profile is a precise
        # observation of an already stopped predecessor, never stop authority
        # inferred from a missing process or permission failure.
        proof=self.h['stopped_predecessor'];row=self.stopped()
        if row!=proof['service']:raise ValueError('stopped predecessor service changed')
        unit=c.read(self.h['unit']['path'],self.l['module_bytes'],True)
        if c.digest(unit)!=self.h['unit']['sha256']:raise ValueError('stopped predecessor unit changed')
        old=self.h['old_artifact'];installed=Path(self.h['release_root'])/old['package_sha256']
        manifest=c.read(installed/'manifest.json',self.l['module_bytes'],True)
        if c.digest(manifest)!=old['manifest_sha256']:raise ValueError('stopped predecessor manifest changed')
        c.verify_before_manifest(self.p,self.h,manifest)
        binary=c.read(installed/'borg-runtime',self.l['package_bytes'],True)
        if len(binary)!=old['binary_bytes'] or c.digest(binary)!=old['binary_sha256']:raise ValueError('stopped predecessor binary changed')
        if ('--limits '+self.h['state']['policy']['path']).encode() not in unit or b'--incarnation ${INVOCATION_ID}' not in unit:raise ValueError('stopped predecessor unit binding differs')
        import pwd
        if pwd.getpwnam(row['User']).pw_uid!=self.h['service_uid']:raise ValueError('stopped predecessor service UID differs')
        m=Path(self.h['owner_lock']).lstat()
        if [m.st_dev,m.st_ino,m.st_uid,m.st_gid,stat.S_IMODE(m.st_mode)]!=proof['owner_identity']:raise ValueError('stopped predecessor owner changed')
        self.compatibility()
        if c.digest(c.canonical(self.state_files()))!=proof['state_sha256']:raise ValueError('stopped predecessor retained state changed')
        # Reject a start/replacement race while we examined the bounded state.
        if self.stopped()!=row:raise ValueError('stopped predecessor service raced')
        return row
    def metadata(self):
        out={'pinned_file_hashes':{item['path']:c.digest(c.pin(item,self.l['module_bytes'])) for item in self.h['preserve_files']}}
        for path in self.h['preserve_metadata']:
            p=Path(path);m=p.lstat()
            if stat.S_ISLNK(m.st_mode): raise ValueError('preserved path alias')
            out[path]=[m.st_dev,m.st_ino,m.st_uid,m.st_gid,stat.S_IMODE(m.st_mode),m.st_size,m.st_mtime_ns]
        return out
    def state_size(self):
        # A metadata sample rejects an already oversized active worker before
        # Start/Stop. It is not an owned snapshot: full content/command/link
        # validation remains mandatory under the owner lock after stopping.
        root=Path(self.h['state']['root']);nodes={};pending=['.'];count=0;files=0;total=0
        full=lambda m:(m.st_dev,m.st_ino,m.st_uid,m.st_gid,m.st_mode,m.st_nlink,m.st_size,m.st_mtime_ns,m.st_ctime_ns)
        def deadline():
            if c.now_ms()>=self.r['deadline_ms']:raise ValueError('state inventory deadline')
        transition=c.forward(self.p)
        link_scope=self.h['role']=='worker' and ((transition is not None and transition['kind'] in (c.PR_PUBLICATION_KIND,c.INSTRUCTION_SOURCE_KIND)) or self.codex_home_scope())
        pattern=r'codex/((?:[0-9a-f]{2})+)/tmp/arg0-linux/codex-arg0[A-Za-z0-9]{6}/(?:apply_patch|applypatch|codex-linux-sandbox)'
        fd=os.open('/',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:
            for part in root.parts[1:]:
                deadline();new=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd)
                os.close(fd);fd=new
            nodes['.']=full(os.fstat(fd))
            def directory(relative):
                deadline();current=os.dup(fd)
                try:
                    for part in Path(relative).parts:
                        deadline();new=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=current)
                        os.close(current);current=new
                    if full(os.fstat(current))!=nodes[relative]:raise ValueError('state directory changed')
                    return current
                except BaseException:
                    os.close(current);raise
            while pending:
                relative=pending.pop();current=directory(relative)
                try:
                    with os.scandir(current) as entries:
                        for entry in entries:
                            deadline();count+=1
                            if count>self.l['state_entries']:raise ValueError('state inventory entry bound')
                            key=str(Path(relative)/entry.name);m=os.stat(entry.name,dir_fd=current,follow_symlinks=False)
                            if stat.S_ISDIR(m.st_mode):nodes[key]=full(m);pending.append(key);continue
                            if stat.S_ISLNK(m.st_mode):
                                if not link_scope or re.fullmatch(pattern,key) is None or m.st_uid!=self.h['service_uid'] or m.st_gid!=nodes['.'][3] or m.st_nlink!=1:raise ValueError('state nonregular/aliased file')
                                parent=Path(key).parent
                                while str(parent)!='.':
                                    meta=nodes[str(parent)]
                                    if meta[2]!=self.h['service_uid'] or meta[3]!=nodes['.'][3] or stat.S_IMODE(meta[4])&0o022:raise ValueError('Codex link parent ownership differs')
                                    parent=parent.parent
                            elif not stat.S_ISREG(m.st_mode) or m.st_nlink!=1:raise ValueError('state nonregular/aliased file')
                            if m.st_size>self.l['state_bytes']-total:raise ValueError('state inventory byte bound')
                            total+=m.st_size;files+=1
                    if full(os.fstat(current))!=nodes[relative]:raise ValueError('state directory changed during read')
                finally:os.close(current)
            for relative in nodes:
                current=directory(relative);os.close(current)
            if full(root.lstat())!=nodes['.']:raise ValueError('state root changed')
            deadline()
            return {'entries':count,'files':files,'bytes':total,'content_hashed':False,'owned_snapshot':False}
        finally:os.close(fd)
    def state_files(self):
        return self._state_files()
    def _state_files(self,capture=None,*,preflight=None):
        if preflight is not None:
            if capture is not None:raise ValueError('conflicting inventory scopes')
            capture=preflight
        root=Path(self.h['state']['root']);out={};total=0;count=0;commands={};links=[]
        full=lambda m:(m.st_dev,m.st_ino,m.st_uid,m.st_gid,m.st_mode,m.st_nlink,m.st_size,m.st_mtime_ns,m.st_ctime_ns)
        public=lambda m:[m.st_dev,m.st_ino,m.st_uid,m.st_gid,stat.S_IMODE(m.st_mode)]
        def deadline():
            if c.now_ms()>=self.r['deadline_ms']:raise ValueError('state inventory deadline')
        transition=c.forward(self.p)
        link_scope=self.h['role']=='worker' and ((transition is not None and transition['kind'] in (c.PR_PUBLICATION_KIND,c.INSTRUCTION_SOURCE_KIND)) or self.codex_home_scope())
        # This is a closed preservation rule, not trust in a link target or
        # attribution of who created it. Retained Codex's arg0 helper names are
        # inert here; every other alias and every special file still refuses.
        pattern=r'codex/((?:[0-9a-f]{2})+)/tmp/arg0-linux/codex-arg0[A-Za-z0-9]{6}/(?:apply_patch|applypatch|codex-linux-sandbox)'
        fd=os.open('/',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:
            for part in root.parts[1:]:
                new=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd)
                os.close(fd);fd=new
            nodes={'.':full(os.fstat(fd))};pending=['.']
            def directory(relative,site='state_files.initial_directory'):
                deadline()
                current=os.dup(fd)
                try:
                    for part in Path(relative).parts:
                        new=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=current)
                        os.close(current);current=new
                    observed=full(os.fstat(current))
                    if not (capture.directory(relative,nodes[relative],observed) if capture else observed==nodes[relative]):
                        raise directory_refusal(site,relative,nodes[relative],observed)
                    return current
                except BaseException:
                    os.close(current);raise
            while pending:
                relative=pending.pop();current=directory(relative)
                try:
                    with os.scandir(current) as entries:
                        for entry in entries:
                            deadline()
                            count+=1
                            if count>self.l['state_entries']:raise ValueError('state inventory entry bound')
                            name=entry.name;key=str(Path(relative)/name)
                            try:m=os.stat(name,dir_fd=current,follow_symlinks=False)
                            except FileNotFoundError:
                                reserved=preflight.vanished_temporary(key) if preflight is not None else None
                                if reserved is None:raise
                                if reserved>self.l['state_bytes']-total:raise ValueError('state inventory byte bound')
                                total+=reserved
                                continue
                            if preflight is not None and preflight.transient(key,m):
                                if m.st_size>self.l['state_bytes']-total:raise ValueError('state inventory byte bound')
                                total+=m.st_size
                                continue
                            sampled=capture.entry(key,m) if capture else False
                            if stat.S_ISDIR(m.st_mode):
                                nodes[key]=full(m);pending.append(key);continue
                            if stat.S_ISLNK(m.st_mode):
                                match=re.fullmatch(pattern,key) if link_scope else None
                                if match is None or m.st_uid!=self.h['service_uid'] or m.st_gid!=nodes['.'][3] or m.st_nlink!=1:
                                    raise ValueError('state nonregular/aliased file')
                                # Check every private parent as a real, owned
                                # directory. No link target is opened/resolved.
                                parent=Path(key).parent
                                while str(parent)!='.':
                                    meta=nodes[str(parent)]
                                    if meta[2]!=self.h['service_uid'] or meta[3]!=nodes['.'][3] or stat.S_IMODE(meta[4])&0o022:
                                        raise ValueError('Codex link parent ownership differs')
                                    parent=parent.parent
                                if m.st_size>self.l['state_bytes']-total:raise ValueError('state inventory byte bound')
                                raw=os.fsencode(os.readlink(name,dir_fd=current))
                                if len(raw)!=m.st_size or full(os.stat(name,dir_fd=current,follow_symlinks=False))!=full(m):
                                    raise ValueError('Codex link changed during read')
                                links.append((key,match.group(1),full(m)))
                                row={'kind':'codex_arg0_symlink','bytes':len(raw),'mtime_ns':m.st_mtime_ns,'ctime_ns':m.st_ctime_ns}
                                size=len(raw);file_sha=c.digest(raw)
                            elif stat.S_ISREG(m.st_mode) and m.st_nlink==1:
                                if m.st_size>self.l['state_bytes']-total:raise ValueError('state inventory byte bound')
                                command_file=len(Path(key).parts)==2 and key.startswith('commands/') and key.endswith('.json')
                                opened=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=current)
                                try:
                                    if preflight is not None:m=preflight.opened(key,m,os.fstat(opened))
                                    elif full(os.fstat(opened))!=full(m):raise ValueError('state file changed before read')
                                    if m.st_size>self.l['state_bytes']-total:raise ValueError('state inventory byte bound')
                                    with os.fdopen(opened,'rb',closefd=False) as stream:
                                        if command_file or sampled:
                                            # Keep original command bytes for the conditional
                                            # Coding association below; the aggregate bound
                                            # still includes every retained command byte.
                                            raw=stream.read(m.st_size+1);size=len(raw);file_sha=c.digest(raw)
                                        else:
                                            # Opaque retained payloads need only an exact
                                            # digest, never a whole-file allocation.
                                            digest=hashlib.sha256();size=0
                                            while True:
                                                deadline()
                                                chunk=stream.read(min(self.l['io_chunk_bytes'],m.st_size-size+1))
                                                if not chunk:break
                                                size+=len(chunk)
                                                if size>m.st_size:raise ValueError('state file changed during read')
                                                digest.update(chunk)
                                            file_sha=digest.hexdigest()
                                    if size!=m.st_size or full(os.fstat(opened))!=full(m):raise ValueError('state file changed during read')
                                finally:os.close(opened)
                                if not (capture.path(key,full(m),full(os.stat(name,dir_fd=current,follow_symlinks=False))) if capture else full(os.stat(name,dir_fd=current,follow_symlinks=False))==full(m)):raise ValueError('state file replaced during read')
                                deadline()
                                if command_file:commands[key]=raw
                                if sampled:capture.capture_bytes(key,raw,public(m),file_sha)
                                row={}
                            else:raise ValueError('state nonregular/aliased file')
                            total+=size
                            if total>self.l['state_bytes']:raise ValueError('state inventory byte bound')
                            out[key]={'sha256':file_sha,'identity':public(m),**row}
                    observed=full(os.fstat(current))
                    if not (capture.directory(relative,nodes[relative],observed) if capture else observed==nodes[relative]):
                        raise directory_refusal('state_files.during_read',relative,nodes[relative],observed)
                finally:os.close(current)
            if links:
                attempts=set()
                for name,raw in commands.items():
                    deadline()
                    record=c.strict(raw);command=record['command']
                    if command['kind']!='run_attempt':continue
                    if name!='commands/'+command['command_id'].encode().hex()+'.json':raise ValueError('Codex original command filename differs')
                    if record['completed'] is True and command['task']['definition']['executor']=='coding':
                        attempts.add(command['attempt']['id'].encode().hex())
                for key,attempt,expected in links:
                    if attempt not in attempts:raise ValueError('Codex link has no completed original Coding command')
                    parent=directory(str(Path(key).parent))
                    try:
                        if full(os.stat(Path(key).name,dir_fd=parent,follow_symlinks=False))!=expected:raise ValueError('Codex link changed after read')
                    finally:os.close(parent)
            for relative in nodes:
                current=directory(relative,'state_files.final_directory');os.close(current)
            observed=full(root.lstat())
            if observed!=nodes['.']:raise directory_refusal('state_files.final_root','.',nodes['.'],observed)
            self.inventory_stats={'entries':count,'files':len(out),'bytes':total}
            self.inventory_directories=nodes
            return out
        finally:os.close(fd)
    def retained_policy(self):
        state=self.h['state'];selected=c.strict(c.pin(state['policy'],self.l['state_bytes']))
        path=Path(state['root'])/'limits.json';before=path.lstat()
        raw=c.read(path,self.l['state_bytes']);after=path.lstat()
        identity=lambda m:(m.st_dev,m.st_ino,m.st_uid,m.st_gid,m.st_mode,m.st_size,m.st_mtime_ns,m.st_ctime_ns)
        if identity(before)!=identity(after) or before.st_uid!=self.h['service_uid'] or before.st_mode&0o077:raise ValueError('worker retained policy ownership/bytes changed')
        retained=c.strict(raw)
        pin=state.get('retained_policy')
        if pin is None:
            if retained!=selected:raise ValueError('worker retained policy differs')
        else:
            if pin['path']!=str(path) or c.digest(raw)!=pin['sha256']:raise ValueError('worker retained policy pin changed')
            # Match worker::same_execution_policy: only a changed current cleanup
            # block and its revision label may differ on this retained root.
            if retained!=selected:
                if retained.get('retention')==selected.get('retention'):raise ValueError('worker execution policy differs')
                execution=dict(selected);execution['revision']=retained['revision']
                if 'retention' in retained:execution['retention']=retained['retention']
                else:execution.pop('retention',None)
                if execution!=retained:raise ValueError('worker execution policy differs')
        return retained
    def codex_home_scope(self):
        if self.h['role']!='worker':return False
        files=getattr(self,'_verified_manifest',{}).get('source',{}).get('files',{})
        exact=lambda family:all(isinstance(files.get(n),dict) and files[n].get('sha256')==sha for n,sha in family.items())
        if _same(c,self.p.get('formats',{}).get('after'),_RECOVERED_CODEX_HOME_FORMATS):
            return exact(_RECOVERED_CODEX_HOME_SOURCES)
        if any(exact(family) for family in (_CODEX_HOME_SOURCES,_SUPPLIED_CODEX_HOME_SOURCES,_DIRECTORY_CODEX_HOME_SOURCES,_DEFAULT_ASSIGNMENT_CODEX_HOME_SOURCES)):
            return True
        return (_same(c,self.p.get('formats',{}).get('after'),_COMPOSED_CODEX_HOME_FORMATS)
                and exact(_COMPOSED_CODEX_HOME_SOURCES))
    def retained_writer_sources(self):
        if _same(c,self.p.get('formats',{}).get('after'),_RECOVERED_CODEX_HOME_FORMATS):
            return _RECOVERED_CODEX_RETAINED_SOURCES
        return _CODEX_RETAINED_SOURCES
    def charter_journal_sources(self):
        if _same(c,self.p.get('formats',{}).get('after'),_RECOVERED_CODEX_HOME_FORMATS):
            return _RECOVERED_CHARTER_JOURNAL_SOURCES
        return _CHARTER_JOURNAL_SOURCES
    def cleanup_policy(self):
        if self.codex_home_scope():
            # The existing protected profile pins the selected metadata bytes.
            # No historical policy digest or new numerical default is substituted.
            return c.strict(c.pin(self.h['state']['policy'],self.l['state_bytes']))
        if self.h['role']!='worker' or not c.forward(self.p) or c.forward(self.p)['kind'] not in (c.PR_PUBLICATION_KIND,c.INSTRUCTION_SOURCE_KIND):return None
        if self.p['formats']['source_files'].get('src/worker/cleanup.rs') not in _CLEANUP_SOURCE_SHAS or self.h['state']['policy']['sha256']!=_CLEANUP_POLICY_SHA:return None
        return c.strict(c.pin(self.h['state']['policy'],self.l['state_bytes']))
    def cleanup_bytes(self,name,entry,bound):
        capture=getattr(self,'_worker_capture',None)
        if capture is not None:return capture.cleanup_bytes(name,entry,bound)
        if len(Path(name).parts)!=2 or Path(name).parts[0]!='cleanup' or not name.endswith('.json') or not name[8:-5] or any(x not in '0123456789abcdef' for x in name[8:-5]):raise ValueError('cleanup relative path differs')
        path=Path(self.h['state']['root'])/name
        before=path.lstat();raw=c.read(path,bound);after=path.lstat()
        full=lambda m:(m.st_dev,m.st_ino,m.st_uid,m.st_gid,m.st_mode,m.st_size,m.st_mtime_ns,m.st_ctime_ns)
        identity=[after.st_dev,after.st_ino,after.st_uid,after.st_gid,stat.S_IMODE(after.st_mode)]
        if full(before)!=full(after) or identity!=entry['identity'] or not stat.S_ISREG(after.st_mode) or after.st_nlink!=1 or after.st_uid!=self.h['service_uid'] or stat.S_IMODE(after.st_mode)!=0o600 or c.digest(raw)!=entry['sha256']:raise ValueError('cleanup exact file changed')
        return raw
    def cleanup_snapshot(self,state):
        selected=self.cleanup_policy();out={}
        if selected is None:return out
        bound=min(self.l['state_bytes'],self.l['receipt_bytes'],selected['retention']['worker']['record_bytes'])
        for name,entry in state.items():
            if not name.startswith('cleanup/'):continue
            raw=self.cleanup_bytes(name,entry,bound);record=cleanup_record(raw,name,selected)
            command='commands/'+record['command_id'].encode().hex()+'.json'
            if command not in state:raise ValueError('cleanup original command absent')
            command_raw=c.read(Path(self.h['state']['root'])/command,self.l['state_bytes'])
            if c.digest(command_raw)!=state[command]['sha256']:raise ValueError('cleanup original command changed')
            original=c.strict(command_raw)['command']
            if original['kind']!='run_attempt' or original['command_id']!=record['command_id'] or original['attempt']['id']!=record['attempt_id'] or original['attempt']['worker_id']!=self.h['api']['worker_id']:raise ValueError('cleanup original command binding differs')
            out[name]=raw.decode('utf-8')
            if len(out)>self.l['state_entries'] or len(c.canonical(out))>self.l['receipt_bytes']:raise ValueError('cleanup snapshot bound')
        return out
    def cleanup_observation(self,name,old,new,saved):
        transitions=getattr(self,'_cleanup_transitions',None)
        if transitions is not None:return transitions.observe(name,old,new,saved)
        return self.cleanup_clock_observation(name,old,new,saved)
    def cleanup_clock_observation(self,name,old,new,saved):
        selected=self.cleanup_policy()
        if selected is None or name not in saved or new is None:
            raise history_refusal('cleanup_observation.required_inputs',name,old,new)
        bound=min(self.l['state_bytes'],self.l['receipt_bytes'],selected['retention']['worker']['record_bytes'])
        raw=saved[name].encode('utf-8')
        if len(raw)>bound or c.digest(raw)!=old['sha256']:raise ValueError('cleanup original snapshot differs')
        original=cleanup_record(raw,name,selected)
        observed=cleanup_record(self.cleanup_bytes(name,new,bound),name,selected)
        # atomic_json replaces this one inode; device, owner, group and mode stay exact.
        if old['identity'][0:1]+old['identity'][2:]!=new['identity'][0:1]+new['identity'][2:] or old['identity'][2]!=self.h['service_uid'] or old['identity'][4]!=0o600:raise ValueError('cleanup owner/mode/device changed')
        sources=self.p['formats']['source_files']
        retained_holds=(sources.get('src/worker/cleanup.rs')==_RETAINED_HOLD_SOURCE_SHA
                        and sources.get('src/worker/process.rs')==_PROCESS_SOURCE_SHA)
        if _same(c,self.p.get('formats',{}).get('after'),_RECOVERED_CODEX_HOME_FORMATS):
            # The current20 hold producer is a separate authenticated family.
            # Do not inherit the historical profile-only allowance for this tuple.
            required={name:_RECOVERED_CODEX_HOME_SOURCES[name]
                      for name in ('src/worker/cleanup.rs','src/worker/process.rs')}
            retained_holds=self.selected_writer_sources(required)
        return {'original':old,'observed':new,**cleanup_clocks(original,observed,retained_holds=retained_holds)}
    def worker_history(self,stop,current):
        preserved={};observations={};mutable={'mutations/registration.json','mutations/await.json','worker-owner.json'}
        if c.forward(self.p):
            old=stop['state'].get('format.json');new=current.get('format.json')
            if old is None or new is None or old['identity'][0:1]+old['identity'][2:]!=new['identity'][0:1]+new['identity'][2:]:raise ValueError('worker marker ownership/mode changed')
            mutable.add('format.json')
        home_proof=CodexHomePreservation(self,stop,current) if self.codex_home_scope() else None
        for name,value in stop['state'].items():
            if name in mutable:continue
            if current.get(name)==value:preserved[name]=value
            elif home_proof is not None and home_proof.observe(name,value,current.get(name)):
                observations[name]=home_proof.observations[name]
            elif name.startswith('cleanup/'):
                observations[name]=self.cleanup_observation(name,value,current.get(name),stop.get('cleanup_records',{}))
            elif self.charter_manifest_observation(name,value,current.get(name),stop,current):
                observations[name]=self._charter_manifests.observations[c.digest(name.encode())]
            elif self.charter_journal_observation(name,value,current.get(name),stop,current):
                observations[name]=self._charter_manifests.journals[c.digest(name.encode())]
            elif self.current_format_observation(name,value,current.get(name)):
                observations[name]=self._current_format_observation
            elif self.stop_marker_observation(name,value,current.get(name),stop,current):preserved[name]=value
            else:raise history_refusal('worker_history.original_row',name,value,current.get(name))
        if home_proof is not None:
            home_proof.finish()
            self._codex_home_proof=home_proof.proof()
        return preserved,observations
    def current_format_observation(self,name,old,new):
        # finish_state_upgrade re-persists the selected current marker after owned
        # admissions. Same-format observation preserves bytes; only its inode moves.
        if name!='format.json':return False
        formats=self.p.get('formats',{})
        required={'src/worker/handoff.rs':'2d7be98666db4fe56debc5d91e90553050dbe195f4b6ec2a4592cdf88e505c75',
                  'src/worker/process.rs':_PROCESS_SOURCE_SHA,
                  'src/worker/mod.rs':'bced916dd3dbb0572f4955d0f65641f491b90ab80cf2ac84f76d89fe68da621a'}
        selected=_COMPOSED_CODEX_HOME_FORMATS
        if _same(c,formats.get('after'),_RECOVERED_CODEX_HOME_FORMATS):
            selected=_RECOVERED_CODEX_HOME_FORMATS
            required={name:_RECOVERED_CODEX_HOME_SOURCES[name] for name in required}
        if (self.h['role']!='worker' or c.forward(self.p)
                or not _same(c,formats.get('before'),selected)
                or not _same(c,formats.get('after'),selected)
                or not self.selected_writer_sources(required)):return False
        revision=selected['worker_format']
        if c.now_ms()>=self.r['deadline_ms']:raise ValueError('current_format_observation_deadline')
        if (not isinstance(old,dict) or not isinstance(new,dict)
                or set(old)!={'sha256','identity'} or set(new)!={'sha256','identity'}
                or old['sha256']!=new['sha256']):raise ValueError('current_format_original_bytes_changed')
        a,b=old['identity'],new['identity']
        if (not isinstance(a,list) or not isinstance(b,list) or len(a)!=5 or len(b)!=5
                or any(type(v) is not int or v<0 for v in a+b)
                or a[:1]+a[2:]!=b[:1]+b[2:] or a[2]!=self.h['service_uid'] or a[4]!=0o600):
            raise ValueError('current_format_noninode_identity_changed')
        bound=min(self.l['state_bytes'],self.l['receipt_bytes'],self.retained_policy()['worker']['state_format_bytes'])
        raw,identity=_file(Path(self.h['state']['root'])/name,bound,self.h['service_uid'])
        if identity!=b or c.digest(raw)!=new['sha256']:raise ValueError('current_format_sample_changed')
        marker=c.strict(raw)
        c.fields(marker,'format_revision','current worker marker')
        if type(marker['format_revision']) is not int or marker['format_revision']!=revision:
            raise ValueError('current_format_marker_value')
        if c.now_ms()>=self.r['deadline_ms']:raise ValueError('current_format_observation_deadline')
        proof={'kind':'same_current_worker_format_atomic_observation','relative_sha256':c.digest(name.encode()),
               'original':copy.deepcopy(old),'observed':copy.deepcopy(new),'format_revision':revision,
               'writer_sources':required,'original_bytes_preserved':True,'noninode_identity_preserved':True,
               'current_read_matched_captured_row':True,'actual_writer_attributed':False,
               'simultaneous_snapshot':False,'service_effects':False}
        if len(c.canonical(proof))>self.l['receipt_bytes']:raise ValueError('current_format_proof_bound')
        self._current_format_observation=proof
        return True

    def selected_writer_sources(self,required):
        # These source-specific housekeeping rules are not generic permissions
        # for an arbitrary retained format. execute() supplies the manifest
        # already authenticated by the selected package descriptor.
        transition=c.forward(self.p)
        if not self.codex_home_scope() and (transition is None or transition['kind'] not in (c.PR_PUBLICATION_KIND,c.INSTRUCTION_SOURCE_KIND)):return False
        manifest=getattr(self,'_verified_manifest',None)
        if not isinstance(manifest,dict):return False
        files=manifest.get('source',{}).get('files',{})
        return all(isinstance(files.get(name),dict) and files[name].get('sha256')==sha for name,sha in required.items())
    def charter_manifest_observation(self,name,old,new,stop,current):
        if not isinstance(name,str) or re.fullmatch(r'charter-output/[0-9a-f]{64}/manifest\.json',name) is None:return False
        if not self.selected_writer_sources(_CHARTER_MANIFEST_SOURCES):return False
        hook=getattr(self,'_charter_manifests',None)
        if hook is None:self._charter_manifests=hook=CharterManifestPreservation(self,stop,current)
        return hook.observe(name,old,new)
    def charter_journal_observation(self,name,old,new,stop,current):
        if not isinstance(name,str) or re.fullmatch(r'charter/(?:[0-9a-f]{2})+\.json',name) is None:return False
        if not self.selected_writer_sources(self.charter_journal_sources()):return False
        hook=getattr(self,'_charter_manifests',None)
        if hook is None:self._charter_manifests=hook=CharterManifestPreservation(self,stop,current)
        return hook.observe_journal(name,old,new)
    def stop_marker_observation(self,name,old,new,stop,current):
        if not (self.codex_home_scope() or self.selected_writer_sources({'src/worker/mod.rs':_STOP_SOURCE_SHA,'src/worker/process.rs':_PROCESS_SOURCE_SHA})):return False
        if not isinstance(name,str) or re.fullmatch(r'stops/(?:[0-9a-f]{2})+',name) is None:return False
        hook=getattr(self,'_stop_markers',None)
        if hook is None:self._stop_markers=hook=StopMarkers(self,c,stop)
        return hook.observe(name,old,new,stop,current)
    def compatibility(self,pending_forward=False):
        state=self.h['state'];expected=self.p['formats']['after' if self.after else 'before']
        c.pin(state['policy'],self.l['state_bytes'])
        pending=False
        # The live main-file header can be stale while WAL is authoritative.
        # Controller startup + expected-wire reads prove the live format; a
        # stopped, locked full-sidecar copy is inspected before any install.
        if self.h['role']=='worker':
            if not self.after and c.forward(self.p) and c.forward(self.p)['kind'] in (c.PR_PUBLICATION_KIND,c.INSTRUCTION_SOURCE_KIND):
                # This explicit transition starts from the captured installed9
                # namespace. New PR phases need the selected worker reader evidence, not
                # an inherited claim from the older retained-state readset.
                phase_root=Path(state['root'])/'pull-requests'
                if phase_root.exists() or phase_root.is_symlink():
                    raise ValueError('installed worker9 pull-requests namespace is not absent')
            raw=c.read(state['format_file'],self.l['receipt_bytes']);marker=c.strict(raw)
            if marker!={'format_revision':expected['worker_format']}:
                if not (pending_forward and self.after and c.forward(self.p) and marker=={'format_revision':self.p['formats']['before']['worker_format']}):
                    raise ValueError('worker format differs from exact phase')
                # A running new process may still be doing its original admission
                # checks. Only this exact retained marker permits read-only waiting.
                old=c.strict(c.read(self.work/'stop.result.json',self.l['receipt_bytes'],True))['state']['format.json']
                m=Path(state['format_file']).lstat();identity=[m.st_dev,m.st_uid,m.st_gid,stat.S_IMODE(m.st_mode)]
                if not stat.S_ISREG(m.st_mode) or m.st_nlink!=1 or old['sha256']!=c.digest(raw) or identity!=old['identity'][0:1]+old['identity'][2:]:
                    raise ValueError('pending worker marker ownership/bytes changed')
                pending=True
            self.retained_policy()
        return not pending
    def sqlite_snapshot(self,label):
        # Caller holds the existing owner lock after verified service shutdown.
        # Preserve exact DB/WAL/SHM bytes separately from the disposable reader.
        c.pin(self.p['sqlite_reader']['extension'],self.l['module_bytes'])
        c.pin(self.p['sqlite_reader']['library'],self.l['module_bytes'])
        import _sqlite3, sqlite3
        if Path(_sqlite3.__file__).resolve()!=Path(self.p['sqlite_reader']['extension']['path']): raise ValueError('SQLite reader extension differs')
        maps=c.read('/proc/self/maps',self.l['module_bytes']).decode()
        libraries={line.split()[-1] for line in maps.splitlines() if 'libsqlite3.so' in line and line.split()[-1].startswith('/')}
        if libraries!={self.p['sqlite_reader']['library']['path']}: raise ValueError('SQLite reader library differs')
        before=self.state_files();out=self.work/label;out.mkdir(mode=0o700);c.sync(self.work)
        evidence=out/'bytes';reader=out/'reader';evidence.mkdir(mode=0o700);reader.mkdir(mode=0o700)
        db=Path(self.h['state']['database'])
        if db.with_name(db.name+'-journal').exists(): raise ValueError('rollback-journal state outside WAL procedure')
        for suffix in ('','-wal','-shm'):
            path=Path(str(db)+suffix)
            if path.exists() or path.is_symlink():
                raw=c.read(path,self.l['state_bytes']);c.write_new(evidence/path.name,raw);c.write_new(reader/path.name,raw)
        end=min(self.r['deadline_ms'],c.now_ms()+self.l['command_timeout_ms'])
        conn=sqlite3.connect((reader/db.name).as_uri()+'?mode=ro',uri=True,timeout=max(0,end-c.now_ms())/1000)
        try:
            conn.set_progress_handler(lambda:int(c.now_ms()>=end),self.l['sqlite_steps'])
            conn.execute('PRAGMA query_only=ON')
            if conn.execute('PRAGMA quick_check').fetchall()!=[('ok',)]: raise ValueError('copied SQLite integrity failed')
            schema=conn.execute('PRAGMA user_version').fetchone()[0]
            if schema!=self.p['formats']['after' if self.after else 'before']['controller_schema']: raise ValueError('stopped schema differs from exact phase')
            names=[r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchmany(self.l['sqlite_tables']+1)]
            if len(names)>self.l['sqlite_tables']: raise ValueError('copied SQLite table bound')
            tables={};total=0
            for name in names:
                quoted='"'+name.replace('"','""')+'"';columns=[r[1] for r in conn.execute('PRAGMA table_info('+quoted+')')]
                rows=conn.execute('SELECT * FROM '+quoted).fetchmany(self.l['sqlite_rows']+1)
                if len(rows)>self.l['sqlite_rows']: raise ValueError('copied SQLite row bound')
                rows=[[{'blob_hex':v.hex()} if isinstance(v,bytes) else v for v in row] for row in rows]
                tables[name]={'columns':columns,'rows':sorted(rows,key=c.canonical)}
                total+=len(c.canonical(tables[name]))
                if total>self.l['state_bytes']: raise ValueError('copied SQLite logical byte bound')
        finally: conn.close()
        if self.state_files()!=before: raise ValueError('stopped original state changed during copy')
        value={'schema':schema,'tables':tables,'physical':before};c.save(out/'snapshot.json',value,self.l['state_bytes']);return value
    def clean(self):
        for registry in self.h['helpers']:
            root=Path(registry);count=0
            if not root.exists(): raise ValueError('helper registry missing')
            for path,dirs,files in os.walk(root,followlinks=False,onerror=_walk_error):
                count+=len(dirs)+len(files)
                if count>self.l['state_entries']: raise ValueError('helper registry bound')
                if any((Path(path)/name).is_symlink() for name in dirs+files): raise ValueError('helper registry alias')
                if any(name.endswith('.json') and name[:-5].isdigit() for name in files): raise ValueError('unfinished helper ownership remains')
        if self.h['role']=='worker':
            root=Path(self.h['state']['root'])
            commands=list((root/'commands').glob('*.json'))
            if len(commands)>self.l['state_entries']: raise ValueError('worker command inventory bound')
            for p in commands:
                row=c.strict(c.read(p,self.l['state_bytes']))
                if row['command']['kind']=='run_attempt' and row['completed'] is not True: raise ValueError('unfinished worker command')
            for path in (root/'outbox',root/'outbox/acceptance'):
                if any(path.glob('*.json')): raise ValueError('pending worker report')
    @contextlib.contextmanager
    def owner(self):
        path=Path(self.h['owner_lock']);fd=os.open(path,os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK)
        try:
            m=os.fstat(fd)
            if not stat.S_ISREG(m.st_mode) or m.st_nlink!=1: raise ValueError('owner lock type')
            fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            if (m.st_dev,m.st_ino)!=(path.stat().st_dev,path.stat().st_ino): raise ValueError('owner lock replaced')
            yield
        finally: os.close(fd)
    def stopped(self):
        row=self.service()
        if row['ActiveState']!='inactive' or row['MainPID']!='0' or self.pids(row): raise ValueError('service/process group not verified stopped')
        self.clean();return row
    def api(self,operation,inp):
        a=self.h['api']
        if a['host'] not in ('127.0.0.1','::1'): raise ValueError('controller API must use existing loopback route')
        q={'protocol_revision':self.p['formats']['after' if self.after else 'before']['wire'],'request_id':'rollout-read','operation':operation,'input':inp}
        reply=c.strict(c.http_read(a['host'],a['port'],c.canonical(q),self.l,self.r['deadline_ms']))
        if reply['request_id']!=q['request_id'] or reply['status']!='ok' or type(reply.get('observed_controller_revision')) is not int: raise ValueError('controller read refused')
        receipt_kind=reply['data']['value']['kind'] if operation=='get_operation' and reply['data']['kind']=='operation' else None
        if receipt_kind=='committed_read_reference':
            c.compact_receipt_sources(self.p,getattr(self,'_verified_manifest',None))
        if receipt_kind in ('application_mutation','committed_read_reference'):
            stamp=reply['commit'];c.fields(stamp,'operation_id controller_revision','original commit')
            if stamp['operation_id']!=inp['operation_id'] or type(stamp['controller_revision']) is not int or not 0<stamp['controller_revision']<=reply['observed_controller_revision']:raise ValueError('original mutation commit stamp differs')
        elif reply['commit'] is not None:raise ValueError('unexpected commit on pure read')
        return reply
    def tasks(self):
        after=None;tasks={}
        for _ in range(self.l['api_pages']):
            reply=self.api('list_tasks',{'after':after,'limit':self.l['api_page_items']});data=reply['data']
            if data['kind']!='tasks': raise ValueError('task projection differs')
            rows=data['value']
            if not rows: return tasks
            for t in rows:
                if t['id'] in tasks or t['status'] not in ('succeeded','failed','cancelled') or (t['attempt'] is not None and not t['attempt']['stopped']): raise ValueError('task not quiesced/unique')
                tasks[t['id']]=t
            after=rows[-1]['id']
        raise ValueError('task inventory page bound')
    def ready(self):
        end=min(self.r['deadline_ms'],c.now_ms()+self.l['command_timeout_ms'])
        while True:
            try:
                row=self.check_running(True)
                if not self.compatibility(pending_forward=True):raise ValueError('forward worker migration pending')
                if self.h['role']=='controller': self.tasks()
                else: self.registration(row)
                return row
            except (ConnectionError,FileNotFoundError) as error: pass
            except ValueError as error:
                if str(error) not in ('service not running','new worker registration not committed','forward worker migration pending'): raise
            if c.now_ms()>=end: raise ValueError('service readiness deadline; state requires reconciliation')
            time.sleep(min(self.l['readiness_poll_ms'],end-c.now_ms())/1000)
    def registration(self,row):
        path=Path(self.h['state']['root'])/'mutations/registration.json';j=c.strict(c.read(path,self.l['state_bytes']))
        c.fields(j,'limits revision request outcome outcome_context consumed recovery_epoch','registration journal')
        q=j['request'];w=q['input']['worker'];context=j['outcome_context']
        policy=self.retained_policy()
        if j['limits']!=policy or type(j['consumed']) is not bool or type(j['recovery_epoch']) is not int:raise ValueError('registration retained policy/shape differs')
        if w['coding_capacity']!=policy['worker']['coding_capacity'] or w['native_capacity']!=policy['worker']['native_capacity']:raise ValueError('registration capacity differs from retained policy')

        if j['revision']!=3 or q['operation']!='register_worker' or w['id']!=self.h['api']['worker_id'] or w['incarnation']!=row['InvocationID'] or j['outcome'] is None or j['outcome'].get('status')!='ok' or j['outcome_context'] is None: raise ValueError('new worker registration not committed')
        reply=self.api('get_operation',{'operation_id':q['input']['operation_id']});r=reply['data']
        c.fields(context,'commit observed_controller_revision','original registration context')
        if context['commit']!=reply['commit'] or not 0<context['commit']['controller_revision']<=context['observed_controller_revision']<=reply['observed_controller_revision']:raise ValueError('canonical registration commit differs')
        if j['outcome']!={'status':'ok','data':{'kind':'worker','value':w}}:raise ValueError('registered worker outcome differs')
        if r['kind']!='operation' or r['value']['intended']!=q or r['value']['outcome']!=j['outcome'] or r['value']['state']!='settled': raise ValueError('canonical registration differs')
        return {'request':q,'outcome':j['outcome'],'context':j['outcome_context']}
    def clock_now(self):
        if c.now_ms()+self.l['clock_uncertainty_ms']>=min(self.r['deadline_ms'],self.r['dispatch_until_ms']): raise ValueError('dispatch authorization expired before effect')
    def bound_now(self):
        # Every blocking observation precedes this last check at each effect.
        raw=c.read(self.r['profile_path'],self.r['profile_bytes'],True)
        if c.digest(raw)!=self.r['profile_sha256']: raise ValueError('target/profile changed before effect')
        for item in self.p['modules'].values():c.pin(item,self.l['module_bytes'])
        c.forward_evidence(self.p)
        self.compatibility()
        for item in self.h['preserve_files']:c.pin(item,self.l['module_bytes'])
        if self.h['role']=='controller':c.pin(self.p['controller_config'],self.r['profile_bytes'])
        self.clock_now()
    def install(self):
        self.stopped()
        with self.owner():
            if 'stopped_predecessor' in self.h:self.check_stopped_predecessor()
            before=self.state_files();meta=self.metadata()
            stop=c.strict(c.read(self.work/'stop.result.json',self.l['receipt_bytes'],True))
            if before!=stop['state']: raise ValueError('runtime state changed since owned stop')
            if self.h['role']=='controller' and stop['snapshot']['schema']!=self.p['formats']['before']['controller_schema']: raise ValueError('no stopped schema proof')
            unit_stat=Path(self.h['unit']['path']).lstat();unit_identity=(unit_stat.st_dev,unit_stat.st_ino,unit_stat.st_uid,unit_stat.st_gid,stat.S_IMODE(unit_stat.st_mode))
            old=c.read(self.h['unit']['path'],self.l['module_bytes'],True);new=c.pin(self.h['new_unit'],self.l['module_bytes'])
            oldpath=str(Path(self.h['release_root'])/self.h['old_artifact']['package_sha256']/'borg-runtime').encode()
            newpath=str(Path(self.h['release_root'])/self.s['artifact']['package_sha256']/'borg-runtime').encode()
            if old.count(oldpath)!=1 or old.replace(oldpath,newpath)!=new: raise ValueError('unit change exceeds exact executable path')
            raw,m=c.verify_package(self.p,self.h,self.s);release=Path(self.h['release_root'])/self.s['artifact']['package_sha256']
            c.protected(release.parent,directory=True);self.bound_now()
            if release.exists(): raise ValueError('new release path already exists; reconcile instead of overwrite')
            self.clock_now();release.mkdir(mode=0o755);c.sync(release.parent)
            # Verified canonical tar has exactly these two members. No extractall.
            import io
            with tarfile.open(fileobj=io.BytesIO(raw),mode='r:') as tf:
                for name,target,mode in [('bin/borg-runtime','borg-runtime',0o555),('manifest.json','manifest.json',0o444)]:
                    body=tf.extractfile(name).read();self.clock_now();c.write_new(release/target,body,mode)
            c.sync(release);self.bound_now()
            temporary=Path(self.h['unit']['path']).with_name(self.h['service']+'.rollout-'+c.digest(self.r['action_id'].encode()))
            c.write_new(temporary,new,0o600)
            fd=os.open(temporary,os.O_WRONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
            try:
                os.fchown(fd,unit_stat.st_uid,unit_stat.st_gid);os.fchmod(fd,stat.S_IMODE(unit_stat.st_mode));os.fsync(fd)
            finally:os.close(fd)
            c.sync(temporary.parent);self.bound_now()
            if c.read(self.h['unit']['path'],self.l['module_bytes'],True)!=old: raise ValueError('unit changed during install')
            latest=Path(self.h['unit']['path']).lstat()
            if (latest.st_dev,latest.st_ino,latest.st_uid,latest.st_gid,stat.S_IMODE(latest.st_mode))!=unit_identity:raise ValueError('unit metadata changed during install')
            self.clock_now();os.replace(temporary,self.h['unit']['path']);c.sync(temporary.parent)
            self.bound_now();self.command(['daemon-reload'])
            if self.state_files()!=before or self.metadata()!=meta: raise ValueError('install altered retained state/metadata')
            return {'state':before,'preserved_metadata':meta,'unit_sha256':c.digest(new),'artifact':self.s['artifact']}
    def phase_evidence(self):
        values={};pins={}
        for phase in ('stop','install','start'):
            pair={}
            for kind,suffix in (('request','intent'),('result','result')):
                name=phase+'.'+suffix+'.json'
                pair[kind],pins[name]=controller_snapshot_read(self,name)
            request=pair['request']
            if request.get('phase')!=phase or any(request.get(key)!=self.r.get(key) for key in (
                    'revision','action_id','subject','profile_path','profile_sha256','profile_bytes','dispatch_until_ms')):
                raise ValueError('reconciliation_original_phase_identity')
            values[phase]=pair
        return values,pins
    def execute(self,phase):
        self.after=phase=='inspect'
        self.compatibility();_,self._verified_manifest=c.verify_package(self.p,self.h,self.s)
        if phase=='preflight':
            size=None;inventory=None
            def worker_inventory(active=False):
                self.clean()
                sample=ActiveWorkerSample(self) if active else None
                if sample is None:
                    state=self.state_files();cleanup=self.cleanup_snapshot(state);proof={}
                else:
                    state=self._state_files(preflight=sample)
                    # Cleanup semantics consume the same descriptor bytes, never
                    # a later atomic replacement of the sampled record.
                    self._worker_capture=sample
                    try:cleanup=self.cleanup_snapshot(state)
                    finally:del self._worker_capture
                    proof=sample.finish(state)
                self.clean()
                return {'sha256':c.digest(c.canonical(state)),'rows':len(state),'stats':self.inventory_stats,
                        'cleanup_records':len(cleanup),'clean':True,'owned_snapshot':False,**proof}
            if 'stopped_predecessor' in self.h:
                with self.owner():
                    row=self.check_stopped_predecessor()
                    size=self.state_size();inventory=worker_inventory()
                    if self.stopped()!=row:raise ValueError('stopped predecessor service raced')
            else:
                row=self.check_running(False)
                if self.h['role']=='worker':
                    active=active_preflight_writers(self) is not None
                    if not active:size=self.state_size()
                    inventory=worker_inventory(active)
                    if active:size={**self.inventory_stats,'content_hashed':inventory['temporary_targets_observed']==0,'owned_snapshot':False,
                                    'simultaneous_snapshot':False}
                    if self.check_running(False)!=row:raise ValueError('preflight service changed during sample')
            tasks=self.tasks() if self.h['role']=='controller' else None
            return {'service':row,'tasks':tasks,'metadata':self.metadata(),'formats':self.p['formats']['before'],'state_size':size,'worker_inventory':inventory}
        if phase=='stop':
            if 'stopped_predecessor' in self.h:
                with self.owner():
                    row=self.check_stopped_predecessor();self.bound_now()
                    state=self.state_files();snapshot=None
                    if c.digest(c.canonical(state))!=self.h['stopped_predecessor']['state_sha256'] or self.stopped()!=row:raise ValueError('stopped predecessor changed before snapshot')
                    cleanup_records=self.cleanup_snapshot(state)
                    codex_homes=codex_home_snapshot(self,state) if self.codex_home_scope() else None
            else:
                row=self.check_running(False);self.bound_now();self.command(['stop',self.h['service']]);row=self.stopped()
                with self.owner():
                    state=self.state_files();snapshot=self.sqlite_snapshot('stopped-copy') if self.h['role']=='controller' else None
                    cleanup_records=self.cleanup_snapshot(state)
                    codex_homes=codex_home_snapshot(self,state) if self.codex_home_scope() else None
            return {'service':row,'state':state,'metadata':self.metadata(),'snapshot':snapshot,'cleanup_records':cleanup_records,
                    **({'codex_homes':codex_homes} if codex_homes is not None else {})}
        if phase=='install': return self.install()
        if phase=='start':
            self.stopped()
            with self.owner():pass
            if c.digest(c.read(self.h['unit']['path'],self.l['module_bytes'],True))!=self.h['new_unit']['sha256']: raise ValueError('new unit differs')
            self.bound_now();self.command(['start',self.h['service']]);self.after=True;row=self.ready()
            if self.h['role']=='controller':
                self.tasks() # actual expected-wire read after canonical startup
                self.bound_now();self.command(['stop',self.h['service']]);self.stopped()
                with self.owner(): after=self.sqlite_snapshot('restart-copy')
                before=c.strict(c.read(self.work/'stop.result.json',self.l['receipt_bytes'],True))['snapshot']
                c.controller_retained(self.p,before,after)
                self.bound_now();self.command(['start',self.h['service']]);row=self.ready();self.tasks()
            return {'service':row,'registration':self.registration(row) if self.h['role']=='worker' else None}
        if phase=='inspect':
            proofs,phase_pins=self.phase_evidence()
            row=self.check_running(True)
            if row!=proofs['start']['result']['service']:raise ValueError('reconciliation_started_incarnation_changed')
            stop=proofs['stop']['result']
            preserved={};cleanup_observations={};extra={}
            if self.h['role']=='worker':
                capture=None
                if self.cleanup_policy() is not None and stop.get('cleanup_records'):
                    capture=WorkerStateCapture(self,c,stop);self._worker_capture=capture
                authority=self.r.get('cleanup_authority')
                if authority is not None:
                    if capture is None:raise ValueError('cleanup transition capture required')
                    self._cleanup_transitions=CleanupTransitions(self,c,stop,authority)
                current=capture.inventory() if capture else self.state_files()
                preserved,cleanup_observations=self.worker_history(stop,current)
                if getattr(self,'_codex_home_proof',None):extra['codex_home_preservation']=self._codex_home_proof
                if capture:extra['cleanup_capture_proof']=capture.proof()
                if getattr(self,'_cleanup_transitions',None):extra['cleanup_transition_proof']=self._cleanup_transitions.proof()
                if getattr(self,'_stop_markers',None):extra['stop_marker_proof']=self._stop_markers.proof()
                if getattr(self,'_charter_manifests',None):extra['charter_manifest_proof']=self._charter_manifests.proof()
            receipt_ids=[]
            if self.h['role']=='controller':
                receipts=verify_controller_receipts(self)
                receipt_ids=receipts['preserved_controller_receipts'];extra['controller_receipt_proof']=receipts['proof']
            metadata=self.metadata();tasks=self.tasks() if self.h['role']=='controller' else None
            registration=self.registration(row) if self.h['role']=='worker' else None
            after_proofs,after_pins=self.phase_evidence()
            if after_proofs!=proofs or after_pins!=phase_pins or self.service()!=row:
                raise ValueError('reconciliation_original_evidence_changed')
            return {'preserved_controller_receipts':receipt_ids,'preserved_worker_state':preserved,
                    'cleanup_observations':cleanup_observations,'phase_proofs':proofs,'phase_file_proof':phase_pins,
                    'service':row,'metadata':metadata,'tasks':tasks,'registration':registration,
                    'artifact':self.s['artifact'],'reconciliation_scope':{
                        'original_action_id':self.r['action_id'],'host_effect_repeated':False,
                        'phase_result_written':False,'live_controller_database_opened':False,
                        'original_phase_pins':phase_pins},**extra}
        raise ValueError('unsupported fixed phase')


def phase(profile,host,request):
    fields='revision action_id subject profile_path profile_sha256 profile_bytes phase dispatch_until_ms deadline_ms'
    if 'cleanup_authority' in request:
        if host['role']!='worker' or request.get('phase')!='inspect':raise ValueError('cleanup authority phase scope')
        fields+=' cleanup_authority'
    c.fields(request,fields,'host request')
    if request['revision']!=1:raise ValueError('host protocol revision')
    if request['phase'] not in ('preflight','stop','install','start','inspect'):raise ValueError('closed host phase')
    if not isinstance(request['action_id'],str) or not request['action_id']:raise ValueError('host action identity')
    for name in ('deadline_ms','profile_bytes'):c.positive(request[name],name)
    if type(request['dispatch_until_ms']) is not int or request['dispatch_until_ms']<0:raise ValueError('host dispatch deadline')
    h=Host(profile,host,request['subject'],request);bound=profile['limits']['receipt_bytes']
    with c.locked(h.root/'host.lock'):
        if not h.work.exists():h.work.mkdir(mode=0o700);c.sync(h.root)
        intent=h.work/(request['phase']+'.intent.json');result=h.work/(request['phase']+'.result.json')
        if request['phase'] in ('preflight','inspect'):return h.execute(request['phase'])
        if intent.exists():
            if c.strict(c.read(intent,bound,True))!=request:raise ValueError('phase identity conflict')
            if result.exists():return c.strict(c.read(result,bound,True))
            raise ValueError('possible original phase effect; read-only reconciliation required')
        c.save(intent,request,bound)
        try:value=h.execute(request['phase'])
        except Exception as error:
            c.save(h.work/(request['phase']+'.error.json'),c.error_record(error,request['phase']),bound)
            raise
        c.save(result,value,bound);return value



def observation_reader(profile,request,values):
    if all(v is None for v in values):
        selected=profile['modules']['host']
        if (str(Path(__file__).absolute())!=selected['path']
                or c.digest(c.read(__file__,profile['limits']['module_bytes'],True))!=selected['sha256']):
            raise ValueError('unselected host reader requires explicit binding')
        return None
    if any(v is None for v in values) or request.get('phase')!='inspect':
        raise ValueError('observation binding requires inspect-only scope')
    binding=dict(zip(('path','sha256','bytes'),values))
    c.positive(binding['bytes'],'observation binding byte bound')
    if binding['bytes']>profile['limits']['module_bytes']:raise ValueError('observation binding byte bound')
    raw=c.pin({k:binding[k] for k in ('path','sha256')},binding['bytes'])
    if len(raw)!=binding['bytes']:raise ValueError('observation binding length')
    descriptor=c.strict(raw)
    c.fields(descriptor['observation_modules'],'adapter host','observation modules')
    reader=c.load_module('rollout_observation_adapter',descriptor['observation_modules']['adapter'],profile['limits']['module_bytes'])
    return reader.validate_observation_binding(profile,request['profile_path'],request['profile_sha256'],
        request['profile_bytes'],request['subject'],request['action_id'],binding,'host',__file__)


def main():
    p=argparse.ArgumentParser();p.add_argument('--profile',required=True);p.add_argument('--profile-sha256',required=True);p.add_argument('--profile-bytes',required=True,type=int);p.add_argument('--host',required=True)
    p.add_argument('--observation-binding');p.add_argument('--observation-binding-sha256');p.add_argument('--observation-binding-bytes',type=int);a=p.parse_args()
    profile=c.load_profile(a.profile,a.profile_sha256,a.profile_bytes)
    host=next(h for h in profile['hosts'] if h['id']==a.host)
    raw=sys.stdin.buffer.read(profile['limits']['transport_bytes']+1)
    if len(raw)>profile['limits']['transport_bytes']:raise ValueError('host request bound')
    req=c.strict(raw)
    if req['profile_path']!=a.profile or req['profile_sha256']!=a.profile_sha256 or req['profile_bytes']!=a.profile_bytes:raise ValueError('host profile binding')
    observation=observation_reader(profile,req,(a.observation_binding,a.observation_binding_sha256,a.observation_binding_bytes))
    try:
        value=phase(profile,host,req)
        if observation is not None:value={**value,'observation_reader':observation['provenance']}
    except Exception as error:
        failure=c.canonical({'host_error':c.error_record(error,req.get('phase'))})
        if len(failure)>profile['limits']['transport_bytes']:raise ValueError('host error reply bound')
        sys.stdout.buffer.write(failure+b'\n');return 1
    raw=c.canonical(value)
    if len(raw)>profile['limits']['transport_bytes']:raise ValueError('host reply bound')
    sys.stdout.buffer.write(raw+b'\n')
if __name__=='__main__':
    try:sys.exit(main() or 0)
    except Exception as error:
        sys.stdout.buffer.write(c.canonical({'host_error':c.error_record(error,None)})+b'\n');sys.exit(1)
