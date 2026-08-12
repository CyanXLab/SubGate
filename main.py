#!/usr/bin/env python3
"""
SubGate - 订阅聚合器
====================

批量获取多个订阅源 → 去重 → 开 HTTP 服务器提供整理后的订阅

用法：
    python main.py              # 拉取 + 去重 + 开服务器
    python main.py refresh      # 只拉取 + 去重（不开服务器）
    python main.py sub list     # 查看订阅源
    python main.py sub add <name> <url>  # 添加订阅
"""
from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

# 强制 UTF-8（Windows 兼容）
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

sys.path.insert(0, str(Path(__file__).resolve().parent))

import yaml
import httpx

APP_NAME = "SubGate"
APP_VERSION = "3.0.0"

# ----------------------------- 配置 -----------------------------

DEFAULT_CONFIG = """# SubGate 配置
# 订阅源列表（顺序即优先级）
subscriptions:
  - https://raw.githubusercontent.com/pawdroid/Free-servers/main/sub
  - https://raw.githubusercontent.com/mfuu/v2ray/master/v2ray
  - https://raw.githubusercontent.com/MatinGhanbari/v2ray-configs/main/subscriptions/v2ray/all_sub.txt
  - https://raw.githubusercontent.com/free18/v2ray/refs/heads/main/v.txt
  - https://raw.githubusercontent.com/barry-far/V2ray-Config/main/All_Configs_Sub.txt
  - https://raw.githubusercontent.com/hello-world-1989/cn-news/main/end-gfw-together
  - https://raw.githubusercontent.com/ermaozi/get_subscribe/main/subscribe/v2ray.txt
  - https://raw.githubusercontent.com/freefq/free/master/v2

# 服务器端口
port: 8787

# 拉取配置
fetch:
  timeout: 8
  retries: 1
  max_workers: 16
  use_mirrors: true
  use_doh: true

# 去重模式: simple / standard / deep
dedupe_mode: standard
"""


def load_config(path: str) -> dict:
    if not os.path.isfile(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write(DEFAULT_CONFIG)
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# ----------------------------- 拉取 -----------------------------

GITHUB_MIRRORS = [
    lambda u: u,
    lambda u: u.replace("https://raw.githubusercontent.com", "https://raw.gitmirror.com"),
    lambda u: _jsdelivr(u, "cdn.jsdelivr.net"),
    lambda u: _jsdelivr(u, "fastly.jsdelivr.net"),
    lambda u: _jsdelivr(u, "gcore.jsdelivr.net"),
    lambda u: "https://ghproxy.net/" + u,
    lambda u: "https://gh-proxy.com/" + u,
    lambda u: "https://ghps.cc/" + u,
]

DOH_SERVERS = [
    "https://cloudflare-dns.com/dns-query",
    "https://dns.google/resolve",
    "https://dns.alidns.com/dns-query",
]

_DNS_CACHE: dict = {}


def _jsdelivr(u, cdn):
    if "raw.githubusercontent.com" not in u:
        return u
    parts = urlparse(u).path.strip("/").split("/", 3)
    if len(parts) < 4:
        return u
    return f"https://{cdn}/gh/{parts[0]}/{parts[1]}@{parts[2]}/{parts[3]}"


def _doh_resolve(host):
    if host in _DNS_CACHE:
        return _DNS_CACHE[host]
    for url in DOH_SERVERS:
        try:
            r = httpx.get(url, params={"name": host, "type": "A"},
                         headers={"Accept": "application/dns-json"}, timeout=5)
            ips = [a["data"] for a in r.json().get("Answer", []) if a.get("type") == 1]
            if ips:
                _DNS_CACHE[host] = ips
                return ips
        except Exception:
            continue
    return []


def fetch_url(url, timeout=8, retries=1, use_doh=True, use_mirrors=True):
    """拉取单个 URL，带 DoH + 镜像回退。"""
    # 本地文件
    if not url.startswith("http"):
        if os.path.isfile(url):
            with open(url, "r", encoding="utf-8", errors="replace") as f:
                return f.read()
        raise FileNotFoundError(url)

    import ssl
    headers = {"User-Agent": "clash-verge/v2.0"}

    # 1. 原始 URL
    for _ in range(retries):
        try:
            r = httpx.get(url, timeout=timeout, follow_redirects=True,
                         headers=headers, verify=False)
            r.raise_for_status()
            if r.text.strip():
                return r.text
        except Exception:
            pass

    # 2. DoH 直连（仅 GitHub）
    is_github = "githubusercontent.com" in url or "github.com" in url
    if is_github and use_doh:
        host = urlparse(url).hostname
        ips = _doh_resolve(host)
        for ip in ips:
            try:
                new_url = url.replace(host, ip, 1)
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                r = httpx.get(new_url, timeout=timeout, follow_redirects=True,
                             headers={**headers, "Host": host}, verify=ctx)
                r.raise_for_status()
                if r.text.strip():
                    return r.text
            except Exception:
                continue

    # 3. 镜像
    if is_github and use_mirrors:
        for mirror in GITHUB_MIRRORS[1:]:
            try:
                murl = mirror(url)
                r = httpx.get(murl, timeout=timeout, follow_redirects=True,
                             headers=headers, verify=False)
                r.raise_for_status()
                if r.text.strip():
                    return r.text
            except Exception:
                continue

    raise RuntimeError(f"所有方式均失败: {url}")


def fetch_all(urls, cfg):
    """多线程并发拉取所有订阅。"""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import random

    fc = cfg.get("fetch", {})
    timeout = fc.get("timeout", 8)
    retries = fc.get("retries", 1)
    max_workers = fc.get("max_workers", 16)
    use_doh = fc.get("use_doh", True)
    use_mirrors = fc.get("use_mirrors", True)

    results = []
    failed = []

    with ThreadPoolExecutor(max_workers=min(max_workers, len(urls))) as ex:
        futures = {ex.submit(fetch_url, u, timeout, retries, use_doh, use_mirrors): u for u in urls}
        done = 0
        total = len(futures)
        for fut in as_completed(futures):
            url = futures[fut]
            done += 1
            try:
                text = fut.result()
                results.append((url, text))
                print(f"  [OK]   [{done}/{total}] {url[:70]}  ({len(text)}B)")
            except Exception as e:
                failed.append(url)
                print(f"  [FAIL] [{done}/{total}] {url[:70]}  -> {e}")

    if failed:
        print(f"  {len(failed)} 个源失败")
    return results


# ----------------------------- 解析 -----------------------------

import base64
import json
import re
from urllib.parse import unquote, urlparse as urlsplit, parse_qs


def _b64decode(s):
    s = re.sub(r'[^A-Za-z0-9+/=]', '', s.strip())
    s = s.replace("-", "+").replace("_", "/")
    s += "=" * ((-len(s)) % 4)
    try:
        return base64.b64decode(s).decode("utf-8", errors="replace")
    except Exception:
        return ""


def _to_int(v, default=0):
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


VALID_CIPHERS = {
    "aes-128-gcm", "aes-192-gcm", "aes-256-gcm",
    "aes-128-cfb", "aes-192-cfb", "aes-256-cfb",
    "aes-128-ctr", "aes-192-ctr", "aes-256-ctr",
    "chacha20", "chacha20-ietf", "chacha20-ietf-poly1305",
    "xchacha20-ietf-poly1305", "rc4-md5", "rc4",
    "none", "plain",
}


def parse_vmess(link):
    try:
        info = json.loads(_b64decode(link[8:]))
    except Exception:
        return None
    if not info.get("add") or not info.get("port"):
        return None
    tls = str(info.get("tls", "")).lower() in ("tls", "true", "1", "yes")
    net = info.get("net", "tcp")
    node = {
        "name": info.get("ps") or f"{info['add']}:{info['port']}",
        "type": "vmess", "server": info["add"], "port": _to_int(info["port"]),
        "uuid": info.get("id", ""), "alterId": _to_int(info.get("aid"), 0),
        "cipher": info.get("scy", "auto"), "network": net, "tls": tls,
    }
    if net == "ws":
        node["ws-opts"] = {"path": info.get("path", "/"), "headers": {"Host": info.get("host", "")}}
    if tls:
        sni = info.get("sni") or info.get("host", "")
        if sni:
            node["servername"] = sni
    return node


def parse_vless(link):
    p = urlsplit(link)
    if not p.hostname or not p.port:
        return None
    q = {k: v[0] for k, v in parse_qs(p.query).items()}
    net = q.get("type", "tcp")
    tls = q.get("security", "none") in ("tls", "reality")
    node = {
        "name": unquote(p.fragment) or f"{p.hostname}:{p.port}",
        "type": "vless", "server": p.hostname, "port": int(p.port),
        "uuid": p.username, "network": net, "tls": tls, "udp": True,
    }
    if tls:
        sni = q.get("sni", q.get("peer", ""))
        if sni:
            node["servername"] = sni
    if q.get("security") == "reality":
        pbk = q.get("pbk", "")
        sid = q.get("sid", "")
        # 校验 short-id
        if sid:
            try:
                bytes.fromhex(sid)
            except (ValueError, TypeError):
                return None
        node["reality-opts"] = {"public-key": pbk, "short-id": sid}
    if net == "ws":
        node["ws-opts"] = {"path": unquote(q.get("path", "/"))}
    return node


def parse_trojan(link):
    p = urlsplit(link)
    if not p.hostname or not p.port or not p.username:
        return None
    q = {k: v[0] for k, v in parse_qs(p.query).items()}
    net = q.get("type", "tcp")
    return {
        "name": unquote(p.fragment) or f"{p.hostname}:{p.port}",
        "type": "trojan", "server": p.hostname, "port": int(p.port),
        "password": unquote(p.username),
        "sni": q.get("sni", q.get("peer", p.hostname)),
        "network": net, "tls": True, "udp": True,
    }


def parse_ss(link):
    body = link[5:]
    name = ""
    if "#" in body:
        body, name = body.split("#", 1)
        name = unquote(name)
    if "@" in body:
        userinfo, hostport = body.rsplit("@", 1)
        method_pass = _b64decode(userinfo)
    else:
        decoded = _b64decode(body)
        if "@" not in decoded:
            return None
        method_pass, hostport = decoded.rsplit("@", 1)
    if ":" not in method_pass or ":" not in hostport:
        return None
    method, password = method_pass.split(":", 1)
    server, port_s = hostport.rsplit(":", 1)
    method = method.strip().lower()
    if method not in VALID_CIPHERS:
        return None
    try:
        port = int(port_s)
    except ValueError:
        return None
    return {
        "name": name or f"{server}:{port}", "type": "ss",
        "server": server, "port": port, "cipher": method,
        "password": password, "udp": True,
    }


def parse_text(text):
    """从文本中解析所有节点。"""
    nodes = []
    # Clash YAML
    if "proxies:" in text[:300].lower():
        try:
            data = yaml.safe_load(text)
            for p in data.get("proxies", []) or []:
                if p.get("server") and p.get("port") and p.get("name") and p.get("type"):
                    nodes.append(p)
        except Exception:
            pass
    # 分享链接
    for m in re.finditer(r'(vmess|vless|trojan|ss|ssr)://[^\s"\'<>]+', text, re.IGNORECASE):
        link = m.group(0).strip()
        try:
            t = link.split("://")[0].lower()
            if t == "vmess":
                n = parse_vmess(link)
            elif t == "vless":
                n = parse_vless(link)
            elif t == "trojan":
                n = parse_trojan(link)
            elif t == "ss":
                n = parse_ss(link)
            else:
                n = None
            if n:
                nodes.append(n)
        except Exception:
            pass
    # base64 订阅
    stripped = text.strip()
    if not nodes and len(stripped) > 40:
        decoded = _b64decode(stripped)
        if decoded and ("vmess://" in decoded or "ss://" in decoded):
            return parse_text(decoded)
    return nodes


def parse_all(sources):
    """解析所有拉取到的源。"""
    all_nodes = []
    for url, text in sources:
        before = len(all_nodes)
        nodes = parse_text(text)
        all_nodes.extend(nodes)
        print(f"  解析 {url[:50]}... -> +{len(nodes)} 节点（累计 {len(all_nodes)}）")
    return all_nodes


# ----------------------------- 去重 -----------------------------

import hashlib


def _norm_str(v):
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, sort_keys=True, ensure_ascii=False)
    s = str(v).strip().lower()
    # 去零宽字符
    s = re.sub(r'[\u200b\u200c\u200d\ufeff\u00ad]', '', s)
    return s


def _norm_path(v):
    s = _norm_str(v)
    if not s:
        return "/"
    try:
        s = unquote(s)
    except Exception:
        pass
    if s != "/" and s.endswith("/"):
        s = s.rstrip("/")
    return s


def _node_key(node, mode="standard"):
    """生成节点指纹。"""
    ntype = _norm_str(node.get("type"))
    server = _norm_str(node.get("server"))
    port = str(_to_int(node.get("port")))

    if mode == "simple":
        return hashlib.sha1(f"{ntype}|{server}|{port}".encode()).hexdigest()

    fields = [ntype, server, port]

    if ntype == "ss":
        fields += [_norm_str(node.get("cipher")), _norm_str(node.get("password"))]
    elif ntype == "vmess":
        fields += [
            _norm_str(node.get("uuid")),
            str(_to_int(node.get("alterId"))),
            _norm_str(node.get("cipher")),
            _norm_str(node.get("network")),
            str(node.get("tls", False)),
            _norm_str(node.get("servername")),
            _norm_path(node.get("ws-opts", {}).get("path") if isinstance(node.get("ws-opts"), dict) else ""),
        ]
    elif ntype == "vless":
        fields += [
            _norm_str(node.get("uuid")),
            _norm_str(node.get("network")),
            _norm_str(node.get("flow")),
            str(node.get("tls", False)),
            _norm_str(node.get("servername")),
        ]
        ro = node.get("reality-opts") or {}
        fields += [_norm_str(ro.get("public-key")), _norm_str(ro.get("short-id"))]
    elif ntype == "trojan":
        fields += [
            _norm_str(node.get("password")),
            _norm_str(node.get("network")),
            _norm_str(node.get("sni")),
        ]

    return hashlib.sha1("|".join(fields).encode()).hexdigest()


def deduplicate(nodes, mode="standard"):
    """去重。"""
    seen = set()
    result = []
    for n in nodes:
        key = _node_key(n, mode)
        if key not in seen:
            seen.add(key)
            result.append(n)
    return result


# ----------------------------- 节点校验 -----------------------------

def is_valid_node(node):
    """检查节点是否有效。"""
    if not node.get("server") or not node.get("port") or not node.get("name"):
        return False
    ntype = str(node.get("type", "")).lower()
    if ntype not in ("ss", "vmess", "vless", "trojan"):
        return False
    try:
        port = int(node.get("port", 0))
        if port < 1 or port > 65535:
            return False
    except (ValueError, TypeError):
        return False
    # server 不含乱码
    server = str(node.get("server", ""))
    if any(ord(c) > 127 and c not in ":.[]" for c in server):
        return False
    # SS cipher
    if ntype == "ss":
        if str(node.get("cipher", "")).lower() not in VALID_CIPHERS:
            return False
        if not node.get("password"):
            return False
    # vmess/vless uuid
    if ntype in ("vmess", "vless") and not node.get("uuid"):
        return False
    # trojan password
    if ntype == "trojan" and not node.get("password"):
        return False
    # REALITY short-id
    if ntype == "vless":
        ro = node.get("reality-opts") or {}
        sid = ro.get("short-id", "")
        if sid:
            try:
                bytes.fromhex(sid)
            except (ValueError, TypeError):
                return False
    return True


# ----------------------------- 输出 -----------------------------

def _make_unique_names(nodes):
    """保证节点名唯一。"""
    used = set()
    count = {}
    out = []
    for n in nodes:
        base = n.get("name") or f"{n.get('server')}:{n.get('port')}"
        if base not in used:
            name = base
        else:
            k = count.get(base, 1) + 1
            while f"{base}_{k}" in used:
                k += 1
            name = f"{base}_{k}"
        used.add(name)
        count[base] = count.get(base, 0) + 1
        n2 = dict(n)
        n2["name"] = name
        out.append(n2)
    return out


def to_clash_yaml(nodes):
    """生成 Clash YAML。"""
    nodes = _make_unique_names(nodes)
    proxies = []
    for n in nodes:
        p = dict(n)
        for k in ("latency_ms", "alive", "error"):
            p.pop(k, None)
        proxies.append(p)

    config = {
        "mixed-port": 7890,
        "allow-lan": False,
        "mode": "rule",
        "log-level": "info",
        "proxies": proxies,
    }
    if proxies:
        config["proxy-groups"] = [{
            "name": "Auto",
            "type": "url-test",
            "proxies": [p["name"] for p in proxies],
            "url": "http://www.gstatic.com/generate_204",
            "interval": 300,
        }]
        config["rules"] = ["MATCH,Auto"]
    return yaml.safe_dump(config, allow_unicode=True, sort_keys=False,
                         default_flow_style=False, width=10000)


def to_v2ray_sub(nodes):
    """生成 V2Ray base64 订阅。"""
    lines = []
    for n in nodes:
        uri = node_to_uri(n)
        if uri:
            lines.append(uri)
    raw = "\n".join(lines)
    return base64.b64encode(raw.encode("utf-8")).decode("ascii")


def node_to_uri(n):
    t = str(n.get("type", "")).lower()
    if t == "vmess":
        info = {
            "v": "2", "ps": n.get("name", ""),
            "add": n.get("server", ""), "port": str(n.get("port", "")),
            "id": n.get("uuid", ""), "aid": str(n.get("alterId", 0)),
            "scy": n.get("cipher", "auto"), "net": n.get("network", "tcp"),
            "tls": "tls" if n.get("tls") else "", "sni": n.get("servername", ""),
        }
        ws = n.get("ws-opts") or {}
        if ws:
            info["path"] = ws.get("path", "/")
            info["host"] = (ws.get("headers") or {}).get("Host", "")
        return "vmess://" + base64.b64encode(json.dumps(info, ensure_ascii=False).encode()).decode()
    if t == "vless":
        qs = [f"type={n.get('network','tcp')}"]
        if n.get("tls"):
            qs.append("security=tls")
            if n.get("servername"):
                qs.append(f"sni={n['servername']}")
        return f"vless://{n.get('uuid','')}@{n.get('server','')}:{n.get('port','')}?{'&'.join(qs)}#{n.get('name','')}"
    if t == "trojan":
        return f"trojan://{n.get('password','')}@{n.get('server','')}:{n.get('port','')}?sni={n.get('sni','')}#{n.get('name','')}"
    if t == "ss":
        user = base64.urlsafe_b64encode(f"{n.get('cipher','')}:{n.get('password','')}".encode()).decode().rstrip("=")
        return f"ss://{user}@{n.get('server','')}:{n.get('port','')}#{n.get('name','')}"
    return ""


# ----------------------------- HTTP 服务器 -----------------------------

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        path = urlparse(self.path).path
        if path in ("", "/", "/index", "/index.html"):
            self._serve_home()
        elif path == "/clash":
            self._serve_file("nodes_clash.yaml", "text/yaml; charset=utf-8")
        elif path == "/v2ray":
            self._serve_file("nodes_v2ray.txt", "text/plain; charset=utf-8")
        elif path == "/count":
            nodes = self.server.nodes
            self._send_json(200, {"total": len(nodes)})
        else:
            self._send_text(404, "Not Found", f"404\n\n可用: /clash /v2ray /count")

    def _send_text(self, code, status, text, ct="text/plain; charset=utf-8"):
        try:
            body = text.encode("utf-8")
            self.send_response(code, status)
            self.send_header("Content-Type", ct)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
        except Exception:
            pass

    def _send_json(self, code, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
        except Exception:
            pass

    def _serve_file(self, filename, ct):
        path = os.path.join("output", filename)
        if not os.path.isfile(path):
            self._send_text(404, "Not Found", "文件不存在，请先刷新\n")
            return
        try:
            with open(path, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", ct)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
        except Exception as e:
            self._send_text(500, "Error", str(e))

    def _serve_home(self):
        nodes = self.server.nodes
        html = f"""<!doctype html><html><head><meta charset="utf-8">
<title>SubGate</title><meta name="viewport" content="width=device-width,initial-scale=1">
<style>
body {{ font-family: -apple-system, sans-serif; max-width: 700px; margin: 40px auto; padding: 0 20px; color: #2c3e50; }}
h1 {{ color: #1a1a2e; }}
.url {{ background: #f7f9fb; padding: 12px; border-radius: 8px; margin: 12px 0; font-family: monospace; word-break: break-all; }}
.btn {{ display: inline-block; padding: 8px 16px; background: #4a90e2; color: #fff; text-decoration: none; border-radius: 6px; margin: 4px; }}
.stat {{ display: inline-block; margin: 8px 16px 8px 0; }}
.stat .v {{ font-size: 28px; font-weight: 700; color: #4a90e2; }}
.stat .l {{ color: #7f8c8d; font-size: 12px; }}
</style></head><body>
<h1>SubGate 订阅聚合</h1>
<div>
<div class="stat"><div class="v">{len(nodes)}</div><div class="l">可用节点</div></div>
</div>
<h2>订阅地址</h2>
<p>Clash / mihomo：</p>
<div class="url">http://{self.headers.get('Host','127.0.0.1:'+str(self.server.server_port))}/clash</div>
<p>V2Ray (base64)：</p>
<div class="url">http://{self.headers.get('Host','127.0.0.1:'+str(self.server.server_port))}/v2ray</div>
<p>
<a class="btn" href="/clash">下载 Clash</a>
<a class="btn" href="/v2ray">下载 V2Ray</a>
</p>
</body></html>"""
        body = html.encode("utf-8")
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except Exception:
            pass


def run_server(nodes, port):
    """启动 HTTP 服务器。"""
    httpd = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    httpd.nodes = nodes
    print(f"\n[SERVER] HTTP 服务已启动: http://0.0.0.0:{port}")
    print(f"[SERVER]   首页:   http://127.0.0.1:{port}/")
    print(f"[SERVER]   Clash: http://127.0.0.1:{port}/clash")
    print(f"[SERVER]   V2Ray: http://127.0.0.1:{port}/v2ray")
    print(f"[SERVER] Ctrl+C 退出\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[SERVER] 已停止")
    finally:
        httpd.server_close()


# ----------------------------- 主流程 -----------------------------

def refresh(cfg):
    """拉取 + 解析 + 去重 + 输出。返回节点列表。"""
    urls = cfg.get("subscriptions", [])
    if not urls:
        print("[ERROR] 没有订阅源")
        return []

    print(f"\n[1/3] 拉取 {len(urls)} 个订阅源...")
    sources = fetch_all(urls, cfg)
    print(f"  成功拉取 {len(sources)} 个")

    print(f"\n[2/3] 解析 + 去重...")
    nodes = parse_all(sources)
    print(f"  解析出 {len(nodes)} 个节点")

    # 过滤无效
    before = len(nodes)
    nodes = [n for n in nodes if is_valid_node(n)]
    if len(nodes) < before:
        print(f"  过滤无效节点: {before} -> {len(nodes)}")

    # 去重
    mode = cfg.get("dedupe_mode", "standard")
    before = len(nodes)
    nodes = deduplicate(nodes, mode)
    print(f"  去重 ({mode}): {before} -> {len(nodes)}")

    # 输出
    print(f"\n[3/3] 输出订阅文件...")
    os.makedirs("output", exist_ok=True)
    clash_yaml = to_clash_yaml(nodes)
    with open("output/nodes_clash.yaml", "w", encoding="utf-8") as f:
        f.write(clash_yaml)
    print(f"  [OK] output/nodes_clash.yaml ({len(nodes)} 节点)")

    v2ray_sub = to_v2ray_sub(nodes)
    with open("output/nodes_v2ray.txt", "w", encoding="utf-8") as f:
        f.write(v2ray_sub)
    print(f"  [OK] output/nodes_v2ray.txt ({len(nodes)} 节点)")

    return nodes


def main():
    parser = argparse.ArgumentParser(description=f"{APP_NAME} - 订阅聚合器")
    parser.add_argument("-c", "--config", default="config.yaml")
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("refresh", help="拉取+去重+输出")
    p_sub = sub.add_parser("sub", help="订阅管理")
    sub_sub = p_sub.add_subparsers(dest="sub_cmd")
    sub_sub.add_parser("list")
    p = sub_sub.add_parser("add")
    p.add_argument("name")
    p.add_argument("url")
    p = sub_sub.add_parser("remove")
    p.add_argument("name_or_url")

    args = parser.parse_args()
    cfg = load_config(args.config)

    cmd = args.cmd or "serve"

    if cmd == "refresh":
        refresh(cfg)
        return

    if cmd == "sub":
        subs = cfg.get("subscriptions", [])
        if args.sub_cmd == "list":
            print(f"\n订阅源 ({len(subs)} 个):")
            for i, u in enumerate(subs, 1):
                print(f"  {i}. {u}")
        elif args.sub_cmd == "add":
            if args.url not in subs:
                subs.append(args.url)
                cfg["subscriptions"] = subs
                with open(args.config, "w", encoding="utf-8") as f:
                    yaml.safe_dump(cfg, f, allow_unicode=True, sort_keys=False)
                print(f"[OK] 已添加: {args.name}")
            else:
                print("[INFO] 已存在")
        elif args.sub_cmd == "remove":
            subs = [u for u in subs if u != args.name_or_url and args.name_or_url not in u]
            cfg["subscriptions"] = subs
            with open(args.config, "w", encoding="utf-8") as f:
                yaml.safe_dump(cfg, f, allow_unicode=True, sort_keys=False)
            print(f"[OK] 已删除")
        return

    # 默认: serve
    print(f"{'='*50}")
    print(f"  {APP_NAME} v{APP_VERSION}")
    print(f"{'='*50}")

    # 刷新
    nodes = refresh(cfg)

    # 开服务器
    port = cfg.get("port", 8787)
    run_server(nodes, port)


if __name__ == "__main__":
    main()
