"""Riot DPoP 模块的离线自测（不联网、不依赖 Riot）。

重点验证三件「错了就整条链路报废」的事：
  1. **ES256 签名是裸 r||s（ieee-p1363）**，不是 cryptography 默认的 DER —— 写错的话
     Riot 只会回 invalid_dpop_proof，而且从外面看不出哪里错；
  2. **JWK 指纹（cnf.jkt）** 用的是 RFC 7638 的规范化 JSON，和 Riot 算出来的必须一模一样；
  3. 证明 JWT 的头/载荷字段（htm 大写、htu 去 query、jti、ath）。

另外带一个**跨语言互操作测试**：用本机 node 现写一段等价的 JS，双方互相验签。
本机没装 node 时自动跳过（不影响其他用例）。
"""
import json
import os
import re
import shutil
import subprocess
import time
import uuid

import pytest
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric import utils as asym_utils

from build_mcp.services import riot_dpop


# ── 测试内的小工具 ──────────────────────────────────────────────────────────

def _verify_es256(pub: dict, signing_input: bytes, sig_b64: str) -> None:
    """用 jwk 里的公钥验签（r||s → DER）；验不过会抛异常。"""
    x = int.from_bytes(riot_dpop.b64url_decode(pub["x"]), "big")
    y = int.from_bytes(riot_dpop.b64url_decode(pub["y"]), "big")
    key = ec.EllipticCurvePublicNumbers(x, y, ec.SECP256R1()).public_key()
    raw = riot_dpop.b64url_decode(sig_b64)
    assert len(raw) == 64, f"ES256 签名必须是定长 64 字节(r||s 各 32)，实际 {len(raw)}（多半写成 DER 了）"
    der = asym_utils.encode_dss_signature(int.from_bytes(raw[:32], "big"),
                                         int.from_bytes(raw[32:], "big"))
    key.verify(der, signing_input, ec.ECDSA(hashes.SHA256()))


def _split(proof: str):
    h, p, s = proof.split(".")
    return json.loads(riot_dpop.b64url_decode(h)), json.loads(riot_dpop.b64url_decode(p)), h, p, s


# ── 1. 密钥 ────────────────────────────────────────────────────────────────

def test_new_dpop_key_shape_and_derivation():
    jwk = riot_dpop.new_dpop_key()
    assert jwk["kty"] == "EC" and jwk["crv"] == "P-256"
    for k in ("x", "y", "d"):
        assert riot_dpop.b64url_decode(jwk[k]).__len__() == 32, f"{k} 必须是定长 32 字节"

    # 用 d 反推公钥，必须和 x/y 一致（定长编码写错的话这里就会不等）
    derived = riot_dpop._load_private_key(jwk).public_key().public_numbers()
    assert derived.x == int.from_bytes(riot_dpop.b64url_decode(jwk["x"]), "big")
    assert derived.y == int.from_bytes(riot_dpop.b64url_decode(jwk["y"]), "big")

    assert "d" not in riot_dpop.public_jwk(jwk)
    assert set(riot_dpop.public_jwk(jwk)) == {"kty", "crv", "x", "y"}


def test_thumbprint_is_rfc7638_canonical():
    jwk = riot_dpop.new_dpop_key()
    pub = riot_dpop.public_jwk(jwk)
    # 规范形：只含 crv/kty/x/y，字典序，紧凑分隔符
    canonical = '{"crv":"%s","kty":"%s","x":"%s","y":"%s"}' % (pub["crv"], pub["kty"], pub["x"], pub["y"])
    import hashlib
    expect = riot_dpop.b64url_encode(hashlib.sha256(canonical.encode()).digest())
    assert riot_dpop.jwk_thumbprint(pub) == expect
    # 稳定 + 与 d 无关
    assert riot_dpop.jwk_thumbprint(pub) == riot_dpop.jwk_thumbprint(pub)
    assert len(riot_dpop.jwk_thumbprint(pub)) == 43


# ── 2. PKCE ───────────────────────────────────────────────────────────────

def test_pkce_and_authorize_url():
    verifier, challenge = riot_dpop.new_pkce()
    assert 43 <= len(verifier) <= 128
    assert re.fullmatch(r"[A-Za-z0-9_\-]+", verifier)
    import hashlib
    assert challenge == riot_dpop.b64url_encode(hashlib.sha256(verifier.encode()).digest())
    assert len(challenge) == 43

    url = riot_dpop.build_authorize_url(challenge, "n0nce")
    assert url.startswith("https://auth.riotgames.com/authorize?")
    q = dict(p.split("=", 1) for p in url.split("?", 1)[1].split("&"))
    import urllib.parse
    assert urllib.parse.unquote_plus(q["client_id"]) == "ritoplus"
    assert urllib.parse.unquote_plus(q["redirect_uri"]) == "http://localhost/redirect"
    assert q["code_challenge"] == challenge
    assert q["code_challenge_method"] == "S256"
    assert q["response_type"] == "code"
    assert q["nonce"] == "n0nce"
    scope = urllib.parse.unquote_plus(q["scope"])
    assert "offline_access" in scope          # 没有它就没有 refresh_token
    assert "openid" in scope


# ── 3. 证明 JWT ───────────────────────────────────────────────────────────

def test_dpop_proof_signature_and_claims():
    jwk = riot_dpop.new_dpop_key()
    before = int(time.time())
    proof = riot_dpop.create_dpop_proof(jwk, "post", "https://auth.riotgames.com/token")
    header, payload, h, p, s = _split(proof)

    assert header["typ"] == "dpop+jwt" and header["alg"] == "ES256"
    assert set(header["jwk"]) == {"kty", "crv", "x", "y"}, "证明头部只能带公钥分量"
    assert header["jwk"] == riot_dpop.public_jwk(jwk)

    assert payload["htm"] == "POST"                      # 必须大写
    assert payload["htu"] == "https://auth.riotgames.com/token"
    assert before <= payload["iat"] <= before + 5
    uuid.UUID(payload["jti"])                            # jti 必须是 uuid
    assert "ath" not in payload and "nonce" not in payload

    # ★ 核心：签名能用头部里的公钥验过（证明 r||s 编码 + 签名输入都正确）
    _verify_es256(header["jwk"], f"{h}.{p}".encode(), s)
    # 换一个字节就验不过（确认验签逻辑不是恒真）
    with pytest.raises(Exception):
        _verify_es256(header["jwk"], f"{h}.{p}x".encode(), s)


def test_dpop_proof_ath_and_nonce_and_htu():
    jwk = riot_dpop.new_dpop_key()
    proof = riot_dpop.create_dpop_proof(
        jwk, "GET", "https://auth.riotgames.com/userinfo?x=1#frag",
        access_token="tok-abc", nonce="N1")
    _, payload, h, p, s = _split(proof)
    import hashlib
    assert payload["ath"] == riot_dpop.b64url_encode(hashlib.sha256(b"tok-abc").digest())
    assert payload["nonce"] == "N1"
    assert payload["htu"] == "https://auth.riotgames.com/userinfo"   # query/fragment 都要丢掉
    assert payload["htm"] == "GET"
    _verify_es256(riot_dpop.public_jwk(jwk), f"{h}.{p}".encode(), s)


def test_normalize_htu():
    assert riot_dpop.normalize_htu("https://a.b/c?d=1#e") == "https://a.b/c"
    assert riot_dpop.normalize_htu("https://a.b/c") == "https://a.b/c"
    assert riot_dpop.normalize_htu("https://a.b") == "https://a.b"


def test_decode_jwt():
    jwk = riot_dpop.new_dpop_key()
    proof = riot_dpop.create_dpop_proof(jwk, "POST", "https://auth.riotgames.com/token")
    dec = riot_dpop.decode_jwt(proof)
    assert dec["header"]["alg"] == "ES256"
    assert dec["payload"]["htm"] == "POST"
    assert riot_dpop.decode_jwt("not-a-jwt") == {}


# ── 4. 解析用户回贴的内容 ─────────────────────────────────────────────────

def test_parse_code_variants():
    assert riot_dpop.parse_code("http://localhost/redirect?code=ABC123def456") == "ABC123def456"
    assert riot_dpop.parse_code("http://localhost/redirect?code=A%2FB%2BC") == "A/B+C"
    assert riot_dpop.parse_code("  http://localhost/redirect?state=x&code=XYZ&foo=1  ") == "XYZ"
    assert riot_dpop.parse_code('"http://localhost/redirect?code=QQQ"') == "QQQ"
    assert riot_dpop.parse_code("aBcD1234eFgH5678iJkL") == "aBcD1234eFgH5678iJkL"   # 裸 code
    # 拿不到就该给空串，好让上层提示「没识别到授权码」
    assert riot_dpop.parse_code("") == ""
    assert riot_dpop.parse_code("http://localhost/redirect?error=access_denied") == ""
    assert riot_dpop.parse_code("随便一句话") == ""


def test_parse_code_in_authorize_roundtrip():
    """自己拼的 authorize URL 里的 code_challenge，能从回贴地址里把 code 抠出来。"""
    _, challenge = riot_dpop.new_pkce()
    url = riot_dpop.build_authorize_url(challenge)
    assert riot_dpop.parse_code(url) == ""            # authorize 地址里没有 code
    back = "http://localhost/redirect?code=" + uuid.uuid4().hex
    assert riot_dpop.parse_code(back) == back.split("code=", 1)[1]


# ── 5. 跨语言互操作（对照参考实现的算法，本机 node 现写一段等价 JS）────────

_JS = r"""
const crypto = require("node:crypto");
const fs = require("fs");
const input = JSON.parse(fs.readFileSync(0, "utf8"));
const jwk = input.jwk;
const b64u = (b) => Buffer.from(b).toString("base64url");
const pub = ({ kty, crv, x, y }) => ({ kty, crv, x, y });
const thumb = (k) => {
  const p = pub(k);
  return b64u(crypto.createHash("sha256").update(
    JSON.stringify({ crv: p.crv, kty: p.kty, x: p.x, y: p.y })).digest());
};
// 用参考实现的同样做法签一条证明
const header = { typ: "dpop+jwt", alg: "ES256", jwk: pub(jwk) };
const payload = { htm: "POST", htu: "https://auth.riotgames.com/token", iat: 1, jti: "j" };
const si = b64u(JSON.stringify(header)) + "." + b64u(JSON.stringify(payload));
const key = crypto.createPrivateKey({ key: jwk, format: "jwk" });
const sig = crypto.sign("sha256", Buffer.from(si), { key, dsaEncoding: "ieee-p1363" });
const nodeProof = si + "." + b64u(sig);
// 反向：验证 Python 生成的证明
const [h, p, s] = input.pyProof.split(".");
const ok = crypto.verify("sha256", Buffer.from(h + "." + p),
  { key: crypto.createPublicKey({ key: input.pyHeaderJwk, format: "jwk" }),
    dsaEncoding: "ieee-p1363" }, Buffer.from(s, "base64url"));
console.log(JSON.stringify({ thumb: thumb(jwk), nodeProof, pyProofVerified: ok }));
"""


@pytest.mark.skipif(not shutil.which("node"), reason="本机没有 node，跳过跨语言互操作")
def test_interop_with_reference_js_implementation():
    jwk = riot_dpop.new_dpop_key()
    py = riot_dpop.create_dpop_proof(jwk, "POST", "https://auth.riotgames.com/token")
    py_header, py_payload, _, _, _ = _split(py)

    proc = subprocess.run(["node", "-e", _JS], input=json.dumps({
        "jwk": jwk, "pyProof": py,
        "pyHeaderJwk": py_header["jwk"],
    }), capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, proc.stderr
    out = json.loads(proc.stdout)

    # ① 指纹必须一字不差（Riot 就是拿它跟 cnf.jkt 比）
    assert out["thumb"] == riot_dpop.jwk_thumbprint(riot_dpop.public_jwk(jwk)), \
        "JWK 指纹和参考实现算出来的不一致 —— cnf.jkt 会被 Riot 判为不匹配"

    # ② node 能验过 Python 的签名（证明 r||s 编码正确）
    assert out["pyProofVerified"] is True, \
        "参考实现的验签逻辑认不出我们生成的证明（ES256 编码/签名输入有问题）"

    # ③ Python 能验过 node 的签名
    nh, np_, nsig = out["nodeProof"].split(".")
    node_header = json.loads(riot_dpop.b64url_decode(nh))
    _verify_es256(node_header["jwk"], f"{nh}.{np_}".encode(), nsig)

    # ④ 两边算出的 htu/算法一致
    node_payload = json.loads(riot_dpop.b64url_decode(np_))
    assert node_payload["htu"] == py_payload["htu"]
    assert node_payload["htm"] == py_payload["htm"]


# ── 6. 错误翻译 ───────────────────────────────────────────────────────────

def test_friendly_error_mapping():
    msg = riot_dpop._friendly_error(400, '{"error":"invalid_dpop_proof"}', "换取令牌")
    assert "DPoP" in msg and "管理员" in msg
    msg = riot_dpop._friendly_error(400, '{"error":"invalid_grant"}', "换取令牌")
    assert "重新发起" in msg
    msg = riot_dpop._friendly_error(0, "TimeoutError: timed out", "换取令牌")
    assert "连不上 Riot" in msg
    msg = riot_dpop._friendly_error(400, '{"error":"invalid_client"}', "换取令牌")
    assert "客户端" in msg
