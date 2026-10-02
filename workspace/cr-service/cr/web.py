"""HTTP API 与节点进度页面（仅标准库，ThreadingHTTPServer）。

API 一览（JSON）::

    GET  /api/status                 集群总览（作业/代际/节点/租约/事件）
    GET  /api/jobs/{id}              作业详情（进程、位点、输出统计）
    POST /api/jobs                   创建/重置演示作业
    POST /api/jobs/{id}/run-steps    推进计算 {"steps": n}
    POST /api/jobs/{id}/preempt      抢占（支持故障注入参数）
    POST /api/jobs/{id}/recover      从 SEALED 代际恢复
    POST /api/jobs/{id}/zombie       模拟旧进程迟到写入
    GET  /api/jobs/{id}/generations  代际列表
    GET  /api/generations/{gid}      代际详情（分片/承诺/缺失项）
    GET  /api/jobs/{id}/report       恢复验证报告（JSON）
    GET  /api/jobs/{id}/report.md    恢复验证报告（Markdown）
    GET  /api/events?since=N         增量事件流
    POST /api/demo                   一键跑完整演示故事线
    GET  /                          节点进度页面
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Tuple
from urllib.parse import parse_qs, urlparse

from . import errors
from .cluster import Cluster
from .demo import run_full_demo
from .report import build_report, to_markdown

PAGE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>抢占与检查点恢复 · 节点进度</title>
<style>
:root{--bg:#0f1420;--panel:#171e2e;--line:#27314a;--txt:#dbe4f5;
--muted:#8b98b8;--ok:#38d39f;--warn:#f7b955;--bad:#ff6b6b;--accent:#5b8cff;}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--txt);
 font:13px/1.55 -apple-system,"PingFang SC","Microsoft YaHei",monospace}
header{padding:14px 20px;border-bottom:1px solid var(--line);
 display:flex;align-items:center;gap:16px;flex-wrap:wrap;
 position:sticky;top:0;background:rgba(15,20,32,.92);z-index:5}
h1{font-size:15px;margin:0}
.badge{padding:2px 9px;border-radius:10px;font-size:12px;border:1px solid var(--line)}
.ok{color:var(--ok);border-color:var(--ok)} .bad{color:var(--bad);border-color:var(--bad)}
.warn{color:var(--warn);border-color:var(--warn)}
.layout{display:grid;grid-template-columns:1.15fr .85fr;gap:14px;padding:14px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:10px;
 padding:14px;margin-bottom:14px}
.card h2{font-size:13px;margin:0 0 10px;color:var(--muted);font-weight:600;
 letter-spacing:.5px;text-transform:uppercase}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:8px}
.rank{border:1px solid var(--line);border-radius:8px;padding:8px 10px}
.rank .top{display:flex;justify-content:space-between}
.st{font-size:11px;padding:0 7px;border-radius:8px}
.st.RUNNING{color:var(--ok)} .st.PAUSED{color:var(--warn)}
.st.CHECKPOINTING{color:var(--accent)} .st.LOST{color:var(--bad)}
table{width:100%;border-collapse:collapse;font-size:12px}
td,th{padding:5px 7px;border-bottom:1px solid var(--line);text-align:left;
 vertical-align:top}
th{color:var(--muted);font-weight:500}
.bar{height:7px;background:#10172400;border-radius:4px;overflow:hidden;
 border:1px solid var(--line);margin-top:4px}
.bar > i{display:block;height:100%;background:var(--accent)}
.bar.done > i{background:var(--ok)}
.bar.bad > i{background:var(--bad)}
.ev{padding:4px 8px;border-left:3px solid var(--line);margin-bottom:3px;
 font-size:12px;white-space:pre-wrap;word-break:break-all}
.ev.ok{border-color:var(--ok)} .ev.warn{border-color:var(--warn)}
.ev.reject,.ev.error{border-color:var(--bad)} .ev.info{border-color:var(--accent)}
.ev .t{color:var(--muted);margin-right:7px}
button{background:#212c44;color:var(--txt);border:1px solid var(--line);
 border-radius:7px;padding:6px 11px;cursor:pointer;font-size:12px}
button:hover{border-color:var(--accent)}
button.danger{border-color:#5a2c2c} button.danger:hover{border-color:var(--bad)}
.row{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px}
.muted{color:var(--muted)} .mono{font-family:ui-monospace,Menlo,monospace}
#toast{position:fixed;right:18px;bottom:18px;max-width:420px;z-index:9}
.toast{background:#1d2740;border:1px solid var(--line);border-radius:9px;
 padding:10px 13px;margin-top:8px;font-size:12px;white-space:pre-wrap}
.toast.err{border-color:var(--bad)} .toast.good{border-color:var(--ok)}
pre.md{white-space:pre-wrap;word-break:break-word;max-height:60vh;overflow:auto;
 font-size:12px;background:#101726;border:1px solid var(--line);border-radius:8px;
 padding:12px}
.modal{position:fixed;inset:0;background:rgba(5,8,15,.7);display:none;
 align-items:center;justify-content:center;z-index:10}
.modal.open{display:flex}
.modal .box{background:var(--panel);border:1px solid var(--line);border-radius:10px;
 width:min(860px,92vw);max-height:86vh;overflow:auto;padding:18px}
@media(max-width:1000px){.layout{grid-template-columns:1fr}}
</style></head>
<body>
<header>
  <h1>🛡️ 抢占与检查点恢复服务</h1>
  <span id="overall" class="badge">—</span>
  <span id="epochBadge" class="badge mono">epoch —</span>
  <span id="leaseBadge" class="badge mono">lease —</span>
  <span id="outBadge" class="badge mono">outputs —</span>
  <span style="flex:1"></span>
  <button onclick="runDemo()">▶ 一键完整演示</button>
  <button onclick="resetJob()">↺ 重置作业</button>
</header>
<div class="layout">
<div>
  <div class="card">
    <h2>动作（调度器 / 故障注入）</h2>
    <div class="row">
      <button onclick="act('run-steps',{steps:2})">运行 2 步</button>
      <button onclick="act('preempt',{})">正常抢占+检查点</button>
      <button onclick="act('preempt',{corrupt_upload:['r01-s0'],duplicate:true})">
        抢占（损坏分片+重复检查点）</button>
    </div>
    <div class="row">
      <button class="danger" onclick="act('preempt',{lost_nodes:['sim-001-node-01']})">
        抢占（节点失联→不完整）</button>
      <button class="danger" onclick="act('preempt',{slow_ranks:[3],slow_delay:1.5,deadline:.4})">
        抢占（慢节点超时）</button>
    </div>
    <div class="row">
      <button onclick="recover()">从最近检查点恢复</button>
      <button class="danger" onclick="act('zombie',{rank:0,seq:0})">
        旧进程迟到写（同内容）</button>
      <button class="danger" onclick="act('zombie',{rank:1,seq:1,divergent:true})">
        旧进程迟到写（分歧内容）</button>
      <button class="danger" onclick="act('zombie-newkey',{rank:2})">
        旧进程写新键</button>
    </div>
    <div class="row">
      <button onclick="showReport()">📄 恢复验证报告</button>
      <span class="muted">提示：所有拒绝都可在右侧事件流看到原因（旧纪元/租约吊销/哈希不符/不完整）。</span>
    </div>
  </div>

  <div class="card">
    <h2>进程集合（rank / 节点 / pid / 输入位点）</h2>
    <div id="ranks" class="grid"></div>
  </div>

  <div class="card">
    <h2>检查点代际</h2>
    <table id="gens"><thead><tr>
      <th>代际</th><th>纪元</th><th>状态</th><th>分片校验</th><th>进程记录</th>
      <th>manifest</th><th></th></tr></thead><tbody></tbody></table>
  </div>
</div>

<div>
  <div class="card">
    <h2>节点</h2>
    <table id="nodes"><tbody></tbody></table>
  </div>
  <div class="card">
    <h2>输出安全</h2>
    <div id="outstats"></div>
  </div>
  <div class="card">
    <h2>事件流（实时）</h2>
    <div id="events"></div>
  </div>
</div>
</div>

<div class="modal" id="modal" onclick="if(event.target.id==='modal')closeModal()">
 <div class="box"><h2 id="mtitle">恢复验证报告</h2>
 <div id="mbody"></div>
 <div class="row" style="margin-top:12px">
   <button onclick="closeModal()">关闭</button>
 </div></div></div>
<div id="toast"></div>

<script>
const JOB='sim-001';
let lastSeq=0;

function esc(s){return String(s??'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));}
function toast(msg,isErr){const d=document.createElement('div');
 d.className='toast '+(isErr?'err':'good');d.textContent=msg;
 document.getElementById('toast').appendChild(d);
 setTimeout(()=>d.remove(),7000);}

async function api(path,opts){
 const r=await fetch(path,{headers:{'Content-Type':'application/json'},...opts});
 const j=await r.json().catch(()=>({}));
 if(!r.ok){const e=(j&&j.error)||('HTTP '+r.status);
  throw new Error(e+': '+(j&&j.message||''));}
 return j;}

async function act(name,body){
 try{const j=await api('/api/jobs/'+JOB+'/'+name,
   {method:'POST',body:JSON.stringify(body)});
  toast('✓ '+name+' 完成\\n'+JSON.stringify(j,null,1).slice(0,600));
  await refresh(true);
 }catch(e){toast('✗ '+e.message,true);await refresh(true);}}

async function recover(){
 try{const gens=await api('/api/jobs/'+JOB+'/generations');
  const sealed=[...gens].reverse().find(g=>g.status==='SEALED');
  if(!sealed)throw new Error('没有 SEALED 代际可恢复');
  const j=await api('/api/jobs/'+JOB+'/recover',
    {method:'POST',body:JSON.stringify({gen_id:sealed.gen_id})});
  toast('✓ 从 '+sealed.gen_id+' 恢复，新租约 token='+j.token+
    '\\n重放去重 '+j.replay.dedup+'/'+j.replay.replayed);
  await refresh(true);
 }catch(e){toast('✗ '+e.message,true);await refresh(true);}}

async function resetJob(){
 try{await api('/api/jobs',{method:'POST',
   body:JSON.stringify({job_id:JOB})});
 lastSeq=0;await refresh(true);toast('✓ 已重置');}
 catch(e){toast('✗ '+e.message,true);}}

async function runDemo(){
 try{toast('▶ 完整演示运行中（含慢节点等待，约 2 秒）…');
  const j=await api('/api/demo',{method:'POST',body:'{}'});
  await refresh(true);
  toast('✓ 演示完成，总体结论：'+j.overall+
    '（报告已写入 reports/recovery-report.md）');
 }catch(e){toast('✗ '+e.message,true);await refresh(true);}}

async function showReport(){
 const md=await fetch('/api/jobs/'+JOB+'/report.md').then(r=>r.text());
 document.getElementById('mtitle').textContent='恢复验证报告';
 const pre=document.createElement('pre');pre.className='md';pre.textContent=md;
 const b=document.getElementById('mbody');b.innerHTML='';b.appendChild(pre);
 document.getElementById('modal').classList.add('open');}
function closeModal(){document.getElementById('modal').classList.remove('open');}

function shardBar(g){
 const p=g.progress;const pct=p.shards_total?
   Math.round(100*p.shards_verified/p.shards_total):0;
 const cls=g.status==='SEALED'?'bar done':(p.shards_rejected?'bar bad':'bar');
 return `<div class="${cls}"><i style="width:${pct}%"></i></div>
 <span class="muted">${p.shards_verified}/${p.shards_total}`+
 (p.shards_rejected?` (${p.shards_rejected} 被拒)`:'')+'</span>';}

function recBar(g){const p=g.progress;const pct=p.records_total?
 Math.round(100*p.records_submitted/p.records_total):0;
 return `<div class="bar ${g.status==='SEALED'?'done':''}">
 <i style="width:${pct}%"></i></div>
 <span class="muted">${p.records_submitted}/${p.records_total}</span>`;}

async function refresh(force){
 try{
  const s=await api('/api/status');
  const job=s.jobs.find(j=>j.job_id===JOB);
  const ob=document.getElementById('overall');
  if(job){ob.textContent='作业 RUNNING 视图';ob.className='badge ok';
   document.getElementById('epochBadge').textContent=
    'epoch '+(s.coordinator.epoch[JOB]??0);
   const lb=document.getElementById('leaseBadge');
   if(job.lease){lb.textContent='lease token='+job.lease.token+
    ' gen='+job.lease.gen_id+(job.lease.revoked?' (revoked)':'');
    lb.className='badge mono '+(job.lease.revoked?'bad':'ok');}
   document.getElementById('outBadge').textContent=
    '输出: 唯一键 '+job.outputs.unique_keys+' / 去重 '+job.outputs.dedup+
    ' / 栅栏拦截 '+job.outputs.fenced;
   // ranks
   document.getElementById('ranks').innerHTML=
    Object.values(job.ranks_state).map(r=>`<div class="rank">
     <div class="top"><b>rank ${r.rank}</b><span class="st ${r.status}">${r.status}</span></div>
     <div class="muted">${esc(r.node)}</div>
     <div>pid ${r.pid} · 步 ${r.step} · 位点 ${r.step}*${4096}</div>
     <div class="muted">out_seq=${r.last_output_seq} token=${r.token}
     gen=${esc(r.gen_id)}</div></div>`).join('');
  } else {ob.textContent='无作业';ob.className='badge warn';}
  // generations
  const tb=document.querySelector('#gens tbody');
  tb.innerHTML=s.coordinator.generations.filter(g=>g.job_id===JOB)
   .map(g=>{const sc=g.status==='SEALED'?'ok':g.status==='ABORTED'?'bad':'warn';
    return `<tr><td class="mono">${g.gen_id}</td><td>${g.epoch}</td>
    <td><span class="badge ${sc}">${g.status}</span></td>
    <td style="min-width:120px">${shardBar(g)}</td>
    <td style="min-width:110px">${recBar(g)}</td>
    <td class="mono muted">${(g.manifest_digest||'—').slice(0,10)}</td>
    <td><button onclick="genDetail('${g.gen_id}')">详情</button></td></tr>`;}).join('')
   ||'<tr><td colspan=7 class=muted>尚无检查点代际</td></tr>';
  // nodes
  document.getElementById('nodes').innerHTML=
   s.nodes.nodes.filter(n=>n.node_id.startsWith(JOB)||n.allocated_to===JOB)
   .map(n=>`<tr><td>${esc(n.node_id)}</td>
   <td class="${n.healthy?'':'bad'}">${n.healthy?'在线':'失联'}</td>
   <td>${esc(n.allocated_to||'空闲')}</td>
   <td class="muted">[${(n.ranks||[]).join(',')}]</td></tr>`).join('');
  // output stats
  const st=job?job.outputs:null;
  document.getElementById('outstats').innerHTML=st?
   `唯一输出键 <b>${st.unique_keys}</b>（双重输出 = 0）<br>
   接受 ${st.accepted} · 幂等去重 ${st.dedup} · 栅栏拦截 ${st.fenced}
   · 内容冲突 ${st.content_conflicts}`:'';
  // events
  const since=force?0:lastSeq;
  const ev=await api('/api/events?since='+since);
  if(force)document.getElementById('events').innerHTML='';
  lastSeq=ev.length?ev[ev.length-1].seq:lastSeq;
  const box=document.getElementById('events');
  ev.forEach(e=>{const d=document.createElement('div');
   d.className='ev '+e.kind;
   d.innerHTML='<span class=t>'+new Date(e.ts*1000).toLocaleTimeString()+
    '</span><span class="muted">['+e.subsystem+']</span> '+esc(e.message)+
    (Object.keys(e.fields||{}).length?
     '\\n<span class="muted">'+esc(JSON.stringify(e.fields))+'</span>':'');
   box.appendChild(d);});
  box.scrollTop=box.scrollHeight;
 }catch(e){/* 轮询偶发失败静默 */}
}
async function genDetail(gid){
 const g=await api('/api/generations/'+gid);
 document.getElementById('mtitle').textContent='代际 '+gid+' · '+g.status;
 document.getElementById('mbody').innerHTML=
  '<pre class="md">'+esc(JSON.stringify(g,null,2))+'</pre>';
 document.getElementById('modal').classList.add('open');}

refresh(true);setInterval(()=>refresh(false),1000);
</script></body></html>"""


def _json_response(handler: BaseHTTPRequestHandler, status: int,
                   payload: Any) -> None:
    body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class _Handler(BaseHTTPRequestHandler):
    server_version = "CRService/1.0"

    # 由 server 注入
    cluster: Cluster = None  # type: ignore[assignment]

    def log_message(self, fmt: str, *args: Any) -> None:  # 静默
        return

    # -- 基础工具 --------------------------------------------------------

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if not length:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            raise errors.CRError("请求体不是合法 JSON")
        return data if isinstance(data, dict) else {}

    def _send(self, payload: Any, status: int = 200) -> None:
        _json_response(self, status, payload)

    def _send_text(self, text: str, ctype: str, status: int = 200) -> None:
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # -- 路由 ------------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            q = parse_qs(parsed.query)
            if path == "/":
                return self._send_text(PAGE, "text/html; charset=utf-8")
            if path == "/api/status":
                return self._send(self.cluster.status())
            if path == "/api/events":
                since = int(q.get("since", ["0"])[0])
                return self._send({"events": self.cluster.events.since(since)})
            if path.startswith("/api/jobs/"):
                parts = path.split("/")
                # /api/jobs/{id}/...
                if len(parts) == 4:
                    job_id = parts[3]
                    return self._send(self.cluster.job_view(job_id))
                if len(parts) == 5 and parts[4] == "generations":
                    return self._send(
                        self.cluster.coordinator.list_generations(parts[3])
                    )
                if len(parts) == 5 and parts[4] == "report":
                    return self._send(build_report(self.cluster, parts[3]))
                if len(parts) == 5 and parts[4] == "report.md":
                    report = build_report(self.cluster, parts[3])
                    return self._send_text(
                        to_markdown(report),
                        "text/markdown; charset=utf-8",
                    )
            if path.startswith("/api/generations/"):
                gid = path.rsplit("/", 1)[-1]
                return self._send(self.cluster.coordinator.get_generation(gid))
            self._send({"error": "not_found", "message": path}, 404)
        except errors.CRError as exc:
            self._send(exc.to_dict(), exc.http_status)
        except Exception as exc:  # noqa: BLE001
            self._send({"error": "internal", "message": str(exc)}, 500)

    def do_POST(self) -> None:  # noqa: N802
        try:
            path = urlparse(self.path).path
            body = self._read_json()
            if path == "/api/jobs":
                job_id = body.get("job_id", "sim-001")
                return self._send(self.cluster.create_job(
                    job_id=job_id,
                    ranks=int(body.get("ranks", 4)),
                    shards_per_rank=int(body.get("shards_per_rank", 2)),
                    shard_size=int(body.get("shard_size", 64 * 1024)),
                ))
            if path == "/api/demo":
                reports = f"{self.cluster.data_dir}/reports"
                result = run_full_demo(
                    self.cluster,
                    job_id=body.get("job_id", "sim-001"),
                    reports_dir=reports,
                )
                return self._send({
                    "overall": result["overall"],
                    "timeline": result["timeline"],
                    "report_paths": result["report_paths"],
                })
            if path.startswith("/api/jobs/"):
                parts = path.split("/")
                if len(parts) != 5:
                    return self._send({"error": "not_found"}, 404)
                job_id, action = parts[3], parts[4]
                return self._send(self._action(job_id, action, body))
            self._send({"error": "not_found", "message": path}, 404)
        except errors.CRError as exc:
            self._send(exc.to_dict(), exc.http_status)
        except Exception as exc:  # noqa: BLE001
            self._send({"error": "internal", "message": str(exc)}, 500)

    def _action(self, job_id: str, action: str, body: dict) -> Any:
        c = self.cluster
        if action == "run-steps":
            return c.run_steps(job_id, steps=int(body.get("steps", 1)))
        if action == "preempt":
            return c.do_preempt(
                job_id,
                lost_nodes=body.get("lost_nodes") or None,
                corrupt_upload=body.get("corrupt_upload") or None,
                flip_after=body.get("flip_after") or None,
                duplicate=bool(body.get("duplicate", False)),
                slow_ranks=body.get("slow_ranks"),
                slow_delay=float(body.get("slow_delay", 2.0)),
                deadline=body.get("deadline"),
            )
        if action == "recover":
            return c.recover(
                job_id,
                gen_id=body.get("gen_id"),
                replay=bool(body.get("replay", True)),
            )
        if action == "zombie":
            return c.zombie_write(
                job_id,
                rank=int(body.get("rank", 0)),
                seq=body.get("seq"),
                divergent=bool(body.get("divergent", False)),
            )
        if action == "zombie-newkey":
            return c.zombie_write_new_key(job_id, rank=int(body.get("rank", 0)))
        raise errors.CRError(f"未知动作 {action}")


def serve(data_dir: str, host: str = "127.0.0.1", port: int = 8080,
          preload_job: str | None = "sim-001") -> Tuple[ThreadingHTTPServer, Cluster]:
    cluster = Cluster(data_dir)
    # 仅当作业在（已自举的）内存态完全不存在时才预置；已经恢复到
    # SEALED 代际并持有租约的作业，重启后保留原租约，不重新签发。
    if preload_job and preload_job not in cluster.jobs:
        cluster.create_job(preload_job)

    handler = type("BoundHandler", (_Handler,), {"cluster": cluster})
    httpd = ThreadingHTTPServer((host, port), handler)

    thread = threading.Thread(target=httpd.serve_forever, daemon=True,
                              name="http")
    thread.start()
    cluster.events.emit(
        "web", "info", f"节点进度页面与 API 已启动: http://{host}:{port}/",
    )
    return httpd, cluster
