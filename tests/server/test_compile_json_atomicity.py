import asyncio
import json

import pytest

from openviking.pyagfs.async_client import fs_ctx_from_agfs_path


@pytest.fixture(params=["configuration", "run"])
def compile_json(service, request):
    store = service.compile.lifecycle.store
    owner = (service.user.account_id, service.user.user_id, "atomic-json-probe")
    if request.param == "configuration":
        return store, store._record_path(*owner), lambda: store.get(*owner)
    task_id = "cmp_" + "a" * 32
    return store, store._run_path(*owner, task_id), lambda: store.get_run(*owner, task_id)


@pytest.mark.asyncio
async def test_native_compile_json_readers_do_not_observe_truncated_writes(compile_json):
    store, path, read = compile_json

    async def publish(generation):
        async with store.locked(path) as lease:
            await store.write_json(
                path,
                {
                    "probe_generation": generation,
                    "probe_payload": str(generation % 10) * (512 * 1024),
                },
                lease,
            )

    await publish(0)
    done = asyncio.Event()
    ready = asyncio.Event()
    reads = 0

    async def writer():
        await ready.wait()
        try:
            for generation in range(1, 101):
                await publish(generation)
        finally:
            done.set()

    async def reader():
        nonlocal reads
        ready.set()
        while not done.is_set():
            record = await read()
            assert record is not None
            assert record["probe_payload"] == str(record["probe_generation"] % 10) * (512 * 1024)
            reads += 1
            await asyncio.sleep(0)

    await asyncio.wait_for(asyncio.gather(writer(), reader()), timeout=90)
    assert reads > 0
    assert (await read())["probe_generation"] == 100


@pytest.mark.asyncio
async def test_malformed_compile_json_remains_fatal_without_rewrite(compile_json):
    store, path, read = compile_json
    malformed = b'{"incomplete":'
    async with store.locked(path) as lease:
        await store.write_json(path, {"previous": "valid"}, lease)
        fs_ctx = fs_ctx_from_agfs_path(path)
        fs_ctx["lease_ref"] = lease["lease_ref"]
        await store._agfs.write(path, malformed, fs_ctx=fs_ctx)
    with pytest.raises(json.JSONDecodeError):
        await read()
    assert await store._agfs.read(path) == malformed
