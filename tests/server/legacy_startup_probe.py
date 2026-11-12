import asyncio
import json
import os
import tempfile
from importlib.metadata import version
from pathlib import Path

import httpx

from openviking.models.embedder.base import DenseEmbedderBase, EmbedResult
from openviking.server.app import create_app
from openviking.server.config import ServerConfig
from openviking.server.dependencies import get_service
from openviking.server.identity import RequestContext, Role
from openviking_cli.session.user_id import UserIdentifier
from openviking_cli.utils.config.embedding_config import EmbeddingConfig
from openviking_cli.utils.config.open_viking_config import OpenVikingConfigSingleton


class OfflineEmbedder(DenseEmbedderBase):
    """Only model inference is fake; native storage and reconciliation are real."""

    def __init__(self):
        super().__init__(model_name="offline-startup-test")

    def embed(self, text, is_query=False):
        return EmbedResult(dense_vector=[0.1] * 512)

    def get_dimension(self):
        return 512


async def main():
    with tempfile.TemporaryDirectory(prefix="ov-legacy-startup-") as tmp:
        root = Path(tmp)
        workspace = root / "workspace"
        # Native local backend stores /local beneath workspace/viking.
        registry = workspace / "viking/_system/accounts.json"
        registry.parent.mkdir(parents=True)
        raw = json.dumps(
            {
                "accounts": {
                    "chatwoot:1": {"created_at": "legacy"},
                    "valid-account": {"created_at": "legacy"},
                }
            }
        ).encode()
        registry.write_bytes(raw)
        # Existing accounts now require their user registries: missing files
        # are deliberately fatal rather than a silently writable empty baseline.
        user_registries = []
        for account_id in ("chatwoot:1", "valid-account"):
            users = workspace / "viking" / account_id / "_system/users.json"
            users.parent.mkdir(parents=True)
            users.write_bytes(b'{"users": {}}')
            user_registries.append(users)
        persisted_settings = {}
        for account_id, settings in {
            "valid-account": {
                "namespace": "legacy",
                "resource_acl": {"auto_protect_new_content": True},
            },
            "chatwoot:1": {
                "namespace": "legacy",
                "resource_acl": {"auto_protect_new_content": True},
            },
        }.items():
            path = workspace / "viking" / account_id / "_system/setting.json"
            content = json.dumps(settings, sort_keys=True).encode()
            path.write_bytes(content)
            persisted_settings[path] = content
        conf = root / "ov.conf"
        conf.write_text(
            json.dumps(
                {
                    "storage": {"workspace": str(workspace)},
                    "reconcile_system_assets_on_startup": True,
                }
            )
        )
        os.environ["OPENVIKING_CONFIG_FILE"] = str(conf)
        OpenVikingConfigSingleton.reset_instance()
        OpenVikingConfigSingleton.initialize(config_path=str(conf))
        EmbeddingConfig.get_embedder = lambda self: OfflineEmbedder()
        app = create_app(ServerConfig(auth_mode="api_key", root_api_key="isolated-root-key"))
        async with app.router.lifespan_context(app):
            accounts = await app.state.api_key_manager.list_accounts()
            print("ACCOUNTS", accounts, flush=True)
            assert {x["account_id"] for x in accounts} >= {"chatwoot:1", "valid-account"}, accounts
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.get("/health")
                assert response.status_code == 200, response.text
                print("HEALTH", response.text, flush=True)
            assert registry.read_bytes() == raw
            # Compare every packaged asset byte-for-byte through the real native
            # filesystem; there must be no asset directory for the legacy ID.
            service = get_service()
            ctx = RequestContext(user=UserIdentifier("valid-account", "default"), role=Role.ROOT)
            acl = service._vikingdb_manager.acl_manager
            assert await acl.is_enabled("valid-account") is True
            assert await acl.is_enabled("chatwoot:1") is False
            assert all(path.read_bytes() == content for path, content in persisted_settings.items())
            marker = workspace / "viking/_system/system_assets_manifest.json"
            assert marker.read_bytes() == service._system_asset_synchronizer.manifest
            assert await service._system_asset_synchronizer.sync(ctx) == 0
            for source, uri in service._system_asset_synchronizer._iter_managed_assets():
                assert await service.viking_fs.read(uri, ctx=ctx) == source.read_bytes(), uri
            legacy_root = workspace / "viking/chatwoot:1"
            assert {
                str(path.relative_to(legacy_root))
                for path in legacy_root.rglob("*")
                if path.is_file()
            } == {"_system/users.json", "_system/setting.json"}
            assert all(path.read_bytes() == b'{"users": {}}' for path in user_registries)
            try:
                await app.state.api_key_manager.create_account("new:invalid", "admin")
            except Exception as exc:
                from openviking_cli.exceptions import InvalidArgumentError

                assert isinstance(exc, InvalidArgumentError), repr(exc)
            else:
                raise AssertionError("new invalid account accepted")
            assert registry.read_bytes() == raw
            print(
                "PASS full application lifespan native RAGFS FileStore api_key real reconciliation",
                version("openviking"),
                flush=True,
            )


asyncio.run(main())
