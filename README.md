# SubGate

订阅聚合器：批量获取多个订阅源 → 去重 → 开 HTTP 服务器提供整理后的订阅。

## 用法

```bash
pip install -r requirements.txt
python main.py
```

运行后：
1. 拉取所有订阅源（多线程 + DoH + 镜像回退）
2. 解析节点（vmess/vless/trojan/ss）
3. 去重
4. 输出到 `output/`
5. 启动 HTTP 服务器

## 订阅地址

- Clash: `http://127.0.0.1:8787/clash`
- V2Ray: `http://127.0.0.1:8787/v2ray`

## 命令

```bash
python main.py              # 拉取+去重+开服务器
python main.py refresh      # 只拉取+去重（不开服务器）
python main.py sub list     # 查看订阅源
python main.py sub add <name> <url>   # 添加订阅
python main.py sub remove <name|url>  # 删除订阅
```

## 配置

编辑 `config.yaml`（首次运行自动生成）：
- `subscriptions`: 订阅源列表
- `port`: 服务器端口（默认 8787）
- `dedupe_mode`: 去重模式（simple/standard/deep）
- `fetch.timeout`: 拉取超时
- `fetch.use_doh`: DoH 解析（绕过 DNS 污染）
- `fetch.use_mirrors`: GitHub 镜像回退
