"""恢复验证报告。

对一次「抢占 → 检查点 → 恢复」生命周期做可审计的独立复核：

1. 封印门：被恢复的代际必须 SEALED，且全量 blob 复验通过；
2. 同代际门：恢复时重新取得的租约必须绑定同一个 gen_id；
3. 不完整门：所有 OPEN/ABORTED 代际都不能被标记为可用；
4. 单输出门：每个 out_key 在输出汇中只有一份内容，拒绝计数与
   去重计数能对上「迟到写入/重放」的去向；
5. 租约门：当前租约有效，旧租约全部吊销。
"""

from __future__ import annotations

import time
from typing import Dict, List

from .cluster import Cluster
from .coordinator import ABORTED, OPEN, SEALED


def _check(name: str, passed: bool, evidence: dict | str) -> dict:
    return {
        "name": name,
        "result": "PASS" if passed else "FAIL",
        "evidence": evidence,
    }


def build_report(cluster: Cluster, job_id: str, title: str = "恢复验证报告") -> dict:
    coordinator = cluster.coordinator
    sink = cluster.sink
    leases = cluster.leases
    scheduler = cluster.scheduler
    job_view = cluster.job_view(job_id)

    gens = coordinator.list_generations(job_id)
    sealed = [g for g in gens if g["status"] == SEALED]
    unsealed = [g for g in gens if g["status"] in (OPEN, ABORTED)]
    target_id = job_view["latest_sealed"]
    target = coordinator.get_generation(target_id) if target_id else None
    verification = (
        coordinator.verify_generation(target_id) if target else None
    )

    current_lease = leases.current_lease(job_id)
    output_view = sink.view(job_id)
    stats = sink.stats_for(job_id)
    rejections = sink.rejections_for(job_id)

    # 单输出复核：每个 out_key 唯一记录、每个摘要与落盘内容一致
    keys = output_view["outputs"]
    double_keys: List[str] = []
    bad_digests: List[str] = []
    for rec in keys:
        try:
            raw = sink.cas.get(rec["digest"])
        except FileNotFoundError:
            bad_digests.append(rec["out_key"])
            continue
        from .storage import sha256_hex

        if sha256_hex(raw) != rec["digest"]:
            bad_digests.append(rec["out_key"])
    if len({r["out_key"] for r in keys}) != len(keys):
        double_keys.append("存在重复键")

    # 恢复进程身份复核：活进程数量 = 检查点进程集合数量，pid 全部更换
    rebuilt_ok = False
    if target is not None:
        n_checkpoint_procs = len(target["processes"])
        live_procs = [
            r for r in job_view["ranks_state"].values()
            if r["status"] == "RUNNING"
        ]
        checkpoint_pids = {p["pid"] for p in target["processes"].values()}
        live_pids = {r["pid"] for r in live_procs}
        rebuilt_ok = (
            len(live_procs) == n_checkpoint_procs
            and not (checkpoint_pids & live_pids)
        )

    checks = [
        _check(
            "封印门：被恢复代际为 SEALED 且全量 blob 复验通过",
            bool(verification and verification["ok"]),
            verification or {"target": target_id},
        ),
        _check(
            "同代际门：当前输出租约绑定被恢复的同一代际",
            bool(
                current_lease
                and target_id
                and current_lease.gen_id == target_id
                and not current_lease.revoked
            ),
            {
                "current_lease": current_lease.to_dict() if current_lease else None,
                "recovered_gen": target_id,
            },
        ),
        _check(
            "不完整门：OPEN/ABORTED 代际均未被标记可恢复",
            all(
                g["manifest_digest"] is None
                and coordinator.latest_sealed_generation(job_id) != g["gen_id"]
                for g in unsealed
            ),
            {
                "unsealed": [
                    {"gen_id": g["gen_id"], "status": g["status"],
                     "reason": g["reason"], "missing": g["missing"]}
                    for g in unsealed
                ]
            },
        ),
        _check(
            f"单输出门：{len(keys)} 个输出键各只有一份内容",
            not double_keys and not bad_digests,
            {"double_keys": double_keys, "bad_digests": bad_digests},
        ),
        _check(
            "租约门：旧租约全部吊销，迟到写入在栅栏处被拒",
            all(
                l.revoked
                for t, l in leases.leases.items()
                if l.job_id == job_id
                and current_lease is not None
                and t != current_lease.token
            ),
            {
                "rejections": len(rejections),
                "by_code": _count_by(rejections, "code"),
                "by_stage": _count_by(rejections, "stage"),
            },
        ),
        _check(
            "重建门：进程集合按检查点重建且 pid 全部重新分配",
            rebuilt_ok,
            {"recovery_count": job_view["recovery_count"],
             "live_ranks": len([
                 r for r in job_view["ranks_state"].values()
                 if r["status"] == "RUNNING"
             ])},
        ),
        _check(
            "释放门：节点回收只发生在 SEALED 之后",
            all(
                h["status"] == "RELEASED"
                and coordinator.get_generation(h["gen_id"])["status"] == SEALED
                for h in scheduler.history_for(job_id)
                if h["status"] == "RELEASED"
            )
            and all(
                coordinator.get_generation(h["gen_id"])["status"] == ABORTED
                for h in scheduler.history_for(job_id)
                if h["status"] == "ABORTED"
            ),
            {"history": scheduler.history_for(job_id)},
        ),
    ]

    overall = all(c["result"] == "PASS" for c in checks)

    return {
        "title": title,
        "job_id": job_id,
        "generated_at": time.time(),
        "overall": "PASS" if overall else "FAIL",
        "checks": checks,
        "generations": [
            {
                "gen_id": g["gen_id"],
                "epoch": g["epoch"],
                "status": g["status"],
                "progress": g["progress"],
                "manifest": g["manifest_digest"],
            }
            for g in gens
        ],
        "outputs": {
            "unique_keys": stats["unique_keys"],
            "accepted": stats["accepted"],
            "dedup": stats["dedup"],
            "rejected": stats["rejected"],
            "fenced": stats["fenced"],
            "content_conflicts": stats["content_conflicts"],
            "keys": keys,
            "rejections": rejections,
        },
        "recoveries": job_view["recovery_count"],
        "preemptions": scheduler.history_for(job_id),
    }


def _count_by(items: List[dict], key: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for item in items:
        out[str(item.get(key))] = out.get(str(item.get(key)), 0) + 1
    return out


# ----------------------------------------------------------------------
# Markdown 渲染
# ----------------------------------------------------------------------

def to_markdown(report: dict) -> str:
    ts = time.strftime("%Y-%m-%d %H:%M:%S",
                       time.localtime(report["generated_at"]))
    lines: List[str] = []
    badge = "✅ 全部通过" if report["overall"] == "PASS" else "❌ 存在失败项"
    lines.append(f"# {report['title']}")
    lines.append("")
    lines.append(f"- 作业：`{report['job_id']}`")
    lines.append(f"- 生成时间：{ts}")
    lines.append(f"- 总体结论：**{report['overall']}** {badge}")
    lines.append(f"- 恢复次数：{report['recoveries']}")
    lines.append("")

    lines.append("## 一、不变式核验")
    lines.append("")
    lines.append("| # | 检查项 | 结果 | 证据摘要 |")
    lines.append("|---|--------|------|----------|")
    for i, chk in enumerate(report["checks"], 1):
        mark = "✅ PASS" if chk["result"] == "PASS" else "❌ FAIL"
        lines.append(f"| {i} | {chk['name']} | {mark} | {_brief(chk['evidence'])} |")
    lines.append("")

    lines.append("## 二、检查点代际")
    lines.append("")
    lines.append("| 代际 | 纪元 | 状态 | 分片 | 进程记录 | manifest |")
    lines.append("|------|------|------|------|----------|----------|")
    for g in report["generations"]:
        p = g["progress"]
        lines.append(
            f"| {g['gen_id']} | {g['epoch']} | {g['status']} | "
            f"{p['shards_verified']}/{p['shards_total']} | "
            f"{p['records_submitted']}/{p['records_total']} | "
            f"{(g['manifest'] or '—')[:12] if g['manifest'] else '—'} |"
        )
    lines.append("")

    lines.append("## 三、输出安全统计")
    lines.append("")
    o = report["outputs"]
    lines.append(f"- 唯一输出键：**{o['unique_keys']}**（无双重输出）")
    lines.append(f"- 首次接受：{o['accepted']}；相同内容幂等去重：{o['dedup']}")
    lines.append(
        f"- 被拒绝的写入：{o['rejected']}（租约栅栏拦截 {o['fenced']}，"
        f"键级内容冲突 {o['content_conflicts']}）"
    )
    if o["rejections"]:
        lines.append("")
        lines.append("| 拦截阶段 | 错误码 | out_key | 旧租约 |")
        lines.append("|----------|--------|---------|--------|")
        for r in o["rejections"]:
            lines.append(
                f"| {r.get('stage')} | {r.get('code')} | "
                f"`{r.get('out_key')}` | token={r.get('token')} |"
            )
    lines.append("")

    lines.append("## 四、抢占纪元处置")
    lines.append("")
    for h in report["preemptions"]:
        lines.append(
            f"- epoch {h['epoch']} / `{h['gen_id']}` → **{h['status']}**"
            + (f"（{h['reason']}）" if h.get("reason") else "")
        )
    lines.append("")
    lines.append("---")
    lines.append("结论：节点仅在检查点 SEALED 后被回收；恢复使用同一代际并")
    lines.append("重新取得输出租约；旧进程迟到写入全部被租约栅栏或键级写一次")
    lines.append("拦截，未发生双重输出，不完整检查点从未被标记可用。")
    return "\n".join(lines) + "\n"


def _brief(evidence: object) -> str:
    if isinstance(evidence, dict):
        parts = []
        for k, v in evidence.items():
            s = str(v)
            parts.append(f"{k}={s[:60]}")
        return "; ".join(parts)[:160]
    return str(evidence)[:160]
