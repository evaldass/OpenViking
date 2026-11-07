import asyncio
import json

import pytest


@pytest.mark.asyncio
async def test_native_compile_json_readers_do_not_observe_truncated_writes(service):
    store = service.compile.lifecycle.store
    account_id = service.user.account_id
    target = "viking://resources/compile-json-reader-writer-probe"
    async with store.edit(account_id, target, create=True) as record:
        record["probe_generation"] = 0
        record["probe_payload"] = "0" * (512 * 1024)
    done = asyncio.Event()
    errors = []
    reads = 0

    async def writer():
        try:
            for generation in range(1, 101):
                async with store.edit(account_id, target) as record:
                    record["probe_generation"] = generation
                    record["probe_payload"] = str(generation % 10) * (512 * 1024)
        finally:
            done.set()

    async def reader():
        nonlocal reads
        while not done.is_set():
            try:
                record = await store.get(account_id, target)
                assert record is not None
                assert record["probe_payload"] == str(record["probe_generation"] % 10) * (
                    512 * 1024
                )
                reads += 1
            except json.JSONDecodeError as error:
                errors.append(repr(error))
            await asyncio.sleep(0)

    await asyncio.wait_for(asyncio.gather(writer(), reader()), timeout=60)
    assert reads > 0
    assert not errors, {
        "native_concurrent_json_decode_errors": len(errors),
        "sample": errors[:3],
        "successful_reads": reads,
    }


@pytest.mark.asyncio
async def test_malformed_compile_json_remains_fatal_without_rewrite(service):
    store = service.compile.lifecycle.store
    account_id = service.user.account_id
    target = "viking://resources/compile-malformed-json-probe"
    async with store.edit(account_id, target, create=True):
        pass
    path = store._record_path(account_id, target)
    malformed = b'{"incomplete":'
    async with store.locked(path) as lease:
        from openviking.pyagfs.async_client import fs_ctx_from_agfs_path

        fs_ctx = fs_ctx_from_agfs_path(path)
        fs_ctx["lease_ref"] = lease["lease_ref"]
        await store._agfs.write(path, malformed, fs_ctx=fs_ctx)
    with pytest.raises(json.JSONDecodeError):
        await store.get(account_id, target)
    assert await store._agfs.read(path) == malformed
