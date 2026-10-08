# common/config.py
"""从 Infisical 拉密钥，反射注入模块全局。

惰性加载：首次访问 config.XXX 时触发加载，之后冻结。
`import common.config` 无副作用——不读 ENV、不拉密钥、不改 globals。

约束：ENV 环境变量必须在"首次访问 config.XXX"之前设好。
    - 生产：ENV=prod python ...
    - E2E：tests/run_e2e_test.py 第一行 os.environ["ENV"]="test"
    - pytest：conftest.py 顶部设一次
"""
import os
import typing
from pathlib import Path
from infisical_sdk import InfisicalSDKClient
from dotenv import load_dotenv

# ⚠️ 必须在任何 os.getenv 之前执行（只读 .env，无副作用）
load_dotenv(Path(__file__).resolve().parents[1] / ".env")


class _Schema:
    PG_HOST: str
    PG_PORT: int
    PG_DB: str
    PG_USER: str
    PG_PASSWORD: str

    MINIO_BUCKET: str
    MINIO_ENDPOINT: str
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str

    KAFKA_BOOTSTRAP: str
    KAFKA_MAX_REQUEST_SIZE: int


_FIELDS: dict[str, type] = typing.get_type_hints(_Schema)
_VALID_ENVS = ("test", "prod")

# ── 状态 ──
_loaded = False


# ── IDE / mypy 补全：运行时 false，静态检查时可见 ──
if typing.TYPE_CHECKING:
    PG_HOST: str
    PG_PORT: int
    PG_DB: str
    PG_USER: str
    PG_PASSWORD: str
    MINIO_BUCKET: str
    MINIO_ENDPOINT: str
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str
    KAFKA_BOOTSTRAP: str
    KAFKA_MAX_REQUEST_SIZE: int


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"缺少环境变量 {name}（检查 .env 或 K8s Secret）")
    return value


def _optional(name: str, default: str) -> str:
    return os.getenv(name, default)


def _load() -> None:
    """从 Infisical 拉密钥并注入 globals()。只执行一次。"""
    global _loaded
    if _loaded:
        return

    env = _require("ENV")
    if env not in _VALID_ENVS:
        raise ValueError(f"ENV 必须是 {_VALID_ENVS}，当前: {env!r}")


    client = InfisicalSDKClient(
        host=_optional("INFISICAL_HOST", "https://app.infisical.com"),
    )
    client.auth.universal_auth.login(
        _require("INFISICAL_CLIENT_ID"),
        _require("INFISICAL_CLIENT_SECRET"),
    )

    resp = client.secrets.list_secrets(
        project_id=_require("INFISICAL_PROJECT_ID"),
        environment_slug=env,
        secret_path=_optional("INFISICAL_SECRET_PATH", "/"),
        expand_secret_references=True,
        view_secret_value=True,
        recursive=False,
        include_imports=True,
        tag_filters=[],
    )
    secrets_map = {s.secretKey: s.secretValue.strip() for s in resp.secrets}

    # 缺字段检查
    missing = [k for k in _FIELDS if k not in secrets_map]
    if missing:
        raise KeyError(f"Infisical 缺少字段: {missing}")

    # 多余字段警告
    extra = set(secrets_map) - set(_FIELDS)
    if extra:
        print(f"[CONFIG] 警告：Infisical 有未声明字段 {sorted(extra)}，已忽略")

    # 命名冲突检查
    g = globals()
    clash = (set(g) - set(_FIELDS)) & set(_FIELDS)
    if clash:
        raise RuntimeError(f"字段名与模块属性冲突: {sorted(clash)}")

    # 强转并注入
    for name, converter in _FIELDS.items():
        g[name] = converter(secrets_map[name])

    _loaded = True
    print(f"[CONFIG] env={env} 加载 {len(_FIELDS)} 个字段: {sorted(_FIELDS)}")


def __getattr__(name: str):
    """PEP 562 模块属性钩子：首次访问配置字段时触发加载。"""
    if name in _FIELDS:
        _load()
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    """让 dir(config) 也能列出字段（可选）。"""
    return sorted(list(globals().keys()) + list(_FIELDS))


__all__ = list(_FIELDS)