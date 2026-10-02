"""端到端演示场景：在真实的协调器/调度器/租约/输出汇上驱动整个故事线。

阶段 0  启动作业，运行计算，产生初始输出；
阶段 1  正常抢占：含「上传损坏分片被拒后修复」+「重复检查点幂等」，
        全部校验完成后封印、确认释放节点；
阶段 2  从同一代际恢复，新节点/新 pid/新租约，重放输出全部幂等去重；
        旧进程并发迟到写入（同内容/分歧内容/新键）全部被拦截；
阶段 3  第二次抢占：部分节点失联 => 检查点不完整 => 封印被拒、纪元中止，
        节点不予回收；随后从上一个 SEALED 代际恢复并继续计算；
阶段 4  慢节点抢占超时：纪元中止，旧进程迟到上传被旧纪元栅栏拒绝；
阶段 5  对已封印检查点做静默损坏：恢复验证发现 FAIL，修复后 PASS；
阶段 6  生成恢复验证报告（结构化 + Markdown）。
"""

from __future__ import annotations

import os
import time
from typing import List

from . import errors
from .cluster import Cluster
from .report import build_report, to_markdown
from .storage import atomic_write


def run_full_demo(cluster: Cluster, job_id: str = "sim-001",
                  reports_dir: str | None = None) -> dict:
    timeline: List[dict] = []

    def note(stage: str, title: str, result: dict) -> None:
        timeline.append({"stage": stage, "title": title, "result": result})
        cluster.events.emit("demo", "ok", f"[{stage}] {title}")

    # ---- 阶段 0：启动 ---------------------------------------------------
    cluster.create_job(job_id, ranks=4, shards_per_rank=2,
                       shard_size=64 * 1024, nodes_per=2)
    note("0", "作业启动，4 个进程分布在 2 个节点上", cluster.job_view(job_id))
    run0 = cluster.run_steps(job_id, steps=3)
    note("0.1", "运行 3 个计算步，每个 rank 产出 3 个输出", run0)

    # ---- 阶段 1：正常抢占 + 损坏修复 + 重复检查点 ------------------------
    preempt1 = cluster.do_preempt(
        job_id,
        corrupt_upload=["r01-s0"],   # rank1 的第 0 个分片上传损坏字节
        duplicate=True,              # 每个分片/记录重复提交一遍
    )
    note("1", "抢占纪元 1：损坏分片被校验拒绝后修复，重复检查点幂等去重，"
              "全量校验通过后封印并释放节点", preempt1)
    gen1 = preempt1["gen_id"]

    # ---- 阶段 2：恢复 + 旧进程迟到写入并发 ------------------------------
    # 在恢复的同一时刻，让被取代的旧进程也尝试写入（并发窗口）
    late: List[dict] = []

    def zombie_same() -> None:
        # 稍等恢复开始，旧进程拿着旧 token 抢写
        time.sleep(0.05)
        late.append({"case": "旧进程同内容迟到写入",
                     **cluster.zombie_write(job_id, rank=0, seq=0)})

    def zombie_divergent() -> None:
        time.sleep(0.05)
        late.append({"case": "旧进程分歧内容迟到写入（双重输出企图）",
                     **cluster.zombie_write(job_id, rank=1, seq=1,
                                            divergent=True)})

    def zombie_newkey() -> None:
        time.sleep(0.05)
        late.append({"case": "旧进程写恢复后的新键",
                     **cluster.zombie_write_new_key(job_id, rank=2)})

    threads = [
        __import__("threading").Thread(target=f)
        for f in (zombie_same, zombie_divergent, zombie_newkey)
    ]
    for t in threads:
        t.start()
    recover1 = cluster.recover(job_id, gen1)
    for t in threads:
        t.join()
    note("2", "从同一 SEALED 代际恢复：新节点/新 pid/新租约，"
              "重放输出全部幂等去重；并发的旧进程迟到写入全部被拦截",
         {"recover": recover1, "late_writes": late})

    # 恢复后继续计算
    run1 = cluster.run_steps(job_id, steps=2)
    note("2.1", "恢复后的进程继续运行 2 个计算步", run1)

    # ---- 阶段 3：部分节点失联，抢占必须失败 -----------------------------
    view = cluster.job_view(job_id)
    node_of_rank3 = view["ranks_state"]["3"]["node"]
    preempt2 = cluster.do_preempt(job_id, lost_nodes=[node_of_rank3])
    note("3", "新一轮抢占：节点失联导致分片缺失，封印被拒，"
              "不完整检查点中止且节点未回收", preempt2)

    # 从上一个已封印代际恢复（注意：绝不能用刚中止的 ep3）
    recover2 = cluster.recover(job_id, gen1)
    note("3.1", "从上一个完整代际重新恢复（跳过 ABORTED 代际）", recover2)

    # ---- 阶段 4：慢节点超时 + 迟到上传 ----------------------------------
    preempt3 = cluster.do_preempt(
        job_id, slow_ranks=[3], slow_delay=1.5, deadline=0.3,
    )
    note("4", "又一轮抢占：慢 rank 导致超时，纪元中止；其迟到分片被拒绝",
         preempt3)

    # ---- 阶段 5：已封印检查点静默损坏的恢复验证 -------------------------
    damaged_shard = cluster.corrupt_sealed_blob(gen1)
    bad_verification = cluster.coordinator.verify_generation(gen1)
    try:
        cluster.recover(job_id, gen1, replay=False)
        corrupt_recovery = {"blocked": False}
    except errors.ShardHashMismatch as exc:
        corrupt_recovery = {"blocked": True, "code": exc.code,
                            "problems": exc.details.get("problems")}
    note("5", "对已封印检查点注入静默损坏：恢复复验发现问题并拒绝恢复",
         {"damaged_shard": damaged_shard,
          "verification": bad_verification,
          "recovery_attempt": corrupt_recovery})

    cluster.repair_sealed_blob(job_id, gen1, damaged_shard)
    fixed_verification = cluster.coordinator.verify_generation(gen1)
    note("5.1", "损坏 blob 从权威内容重建，复验恢复通过", fixed_verification)

    # ---- 阶段 6：报告 ---------------------------------------------------
    report = build_report(cluster, job_id,
                          title="抢占与检查点恢复 — 恢复验证报告")
    md = to_markdown(report)
    report_paths = {}
    if reports_dir:
        os.makedirs(reports_dir, exist_ok=True)
        md_path = os.path.join(reports_dir, "recovery-report.md")
        atomic_write(md_path, md)
        report_paths["markdown"] = md_path
    return {
        "timeline": timeline,
        "report": report,
        "markdown": md,
        "report_paths": report_paths,
        "overall": report["overall"],
    }
