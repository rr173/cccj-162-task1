"""端到端集群编排测试 + 恢复验证报告断言。"""

from __future__ import annotations

import tempfile
import threading
import unittest

from cr import errors
from cr.cluster import Cluster
from cr.cluster import output_payload, out_key
from cr.demo import run_full_demo
from cr.report import build_report
from cr.scheduler import ABORTED, RELEASED


class ClusterEndToEndTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.cluster = Cluster(self._tmp.name)
        self.cluster.create_job("e2e", ranks=4, shards_per_rank=2,
                                shard_size=16 * 1024)
        self.cluster.run_steps("e2e", steps=2)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_preempt_seal_release_recover_no_double_output(self) -> None:
        c = self.cluster
        pre = c.do_preempt("e2e")
        self.assertEqual(pre["outcome"], "released")
        self.assertTrue(pre["sealed"])

        # 释放闸门：只有 SEALED 才 RELEASED
        active = c.scheduler.history_for("e2e")[-1]
        self.assertEqual(active["status"], RELEASED)

        rec = c.recover("e2e", pre["gen_id"])
        self.assertEqual(rec["outcome"], "recovered")
        self.assertTrue(rec["verification"]["ok"])
        # 重放全部命中去重（恢复前已有 8 个输出）
        self.assertEqual(rec["replay"]["replayed"], 8)
        self.assertEqual(rec["replay"]["dedup"], 8)
        self.assertEqual(rec["replay"]["accepted"], 0)

        # 旧进程迟到写入：三种情形全部被拦截
        z1 = c.zombie_write("e2e", rank=0, seq=0)
        z2 = c.zombie_write("e2e", rank=1, seq=1, divergent=True)
        z3 = c.zombie_write_new_key("e2e", rank=2)
        for z in (z1, z2, z3):
            self.assertTrue(z["blocked"], z)

        stats = c.sink.stats_for("e2e")
        self.assertEqual(stats["unique_keys"], 8)
        self.assertEqual(stats["rejected"], 3)
        self.assertEqual(stats["fenced"], 3)
        self.assertEqual(stats["content_conflicts"], 0)  # 栅栏先挡住，未到内容冲突层

        report = build_report(c, "e2e")
        self.assertEqual(report["overall"], "PASS")
        self.assertTrue(
            all(chk["result"] == "PASS" for chk in report["checks"]),
            report["checks"],
        )

    def test_concurrent_recovery_and_zombie_writes_no_double_output(self) -> None:
        """恢复在途时，多个旧进程在同一窗口并发迟到写入：无一产生第二份输出。"""

        c = self.cluster
        pre = c.do_preempt("e2e")
        # 拿到将被吊销的旧租约/身份
        old = []
        for rank in range(4):
            r = c.jobs["e2e"].rank_state[rank]
            old.append({"rank": rank, "token": r.token, "epoch": r.epoch,
                        "gen_id": r.gen_id})

        results: list[dict] = []
        barrier = threading.Barrier(5)  # 4 个僵尸 + 主线程恢复

        def zombie(idx: int) -> None:
            v = old[idx]
            barrier.wait()  # 与恢复尽量挤在同一时刻
            for seq in (0, 1):
                payload = (
                    b"RACING-DIVERGENT\n"
                    if (idx + seq) % 2 else
                    output_payload("e2e", v["rank"], seq)
                )
                try:
                    w = c.sink.write(
                        "e2e", v["token"], v["epoch"], v["gen_id"],
                        out_key("e2e", v["rank"], seq), payload,
                        rank=v["rank"], seq=seq,
                    )
                    # 窗口内成功只可能是相同内容的幂等去重
                    results.append({"blocked": False, "rank": v["rank"],
                                    "result": w["result"]})
                except errors.CRError:
                    results.append({"blocked": True, "rank": v["rank"]})

        threads = [threading.Thread(target=zombie, args=(i,))
                   for i in range(4)]
        for t in threads:
            t.start()
        barrier.wait()
        rec = c.recover("e2e", pre["gen_id"])
        for t in threads:
            t.join()

        self.assertEqual(rec["outcome"], "recovered")
        self.assertEqual(len(results), 8)
        unblocked = [r for r in results if not r["blocked"]]
        # 窗口内成功的全部是相同内容幂等去重（分歧内容必然被写一次拒绝）
        self.assertTrue(
            all(r["result"] == "dedup" for r in unblocked), unblocked
        )
        # 不变式是「不产生双重输出」，而不是「窗口内必须全部在栅栏失败」：
        # 在租约吊销前的极窄窗口用旧（仍有效）租约到达的同内容写入，
        # 由第二道防线（键级写一次）幂等去重；分歧内容则在写一次处拒绝。
        stats = c.sink.stats_for("e2e")
        self.assertEqual(stats["unique_keys"], 8)          # 没有任何新键
        # 4 个分歧写入：要么在栅栏被拦（fenced），要么在键级写一次被拒
        # （content_conflicts），两道防线合计必须覆盖全部 4 个；
        # 4 个同内容写入则要么被栅栏拦，要么 dedup —— 都不产生第二份输出。
        self.assertGreaterEqual(stats["fenced"] + stats["content_conflicts"], 4)
        self.assertGreater(stats["fenced"] + stats["dedup"], 0)

        # 恢复（租约吊销）完成之后，旧进程的任何迟到写入必须全部被拦。
        after = [c.zombie_write("e2e", rank=r, seq=0) for r in range(4)]
        after.append(c.zombie_write("e2e", rank=2, seq=1, divergent=True))
        self.assertTrue(all(z["blocked"] for z in after), after)
        self.assertEqual(c.sink.stats_for("e2e")["unique_keys"], 8)

    def test_corrupt_upload_repaired_and_duplicate_idempotent(self) -> None:
        c = self.cluster
        pre = c.do_preempt(
            "e2e", corrupt_upload=["r02-s1"], duplicate=True,
        )
        self.assertEqual(pre["outcome"], "released")
        self.assertEqual(pre["counters"]["hash_rejected"], 1)
        self.assertEqual(pre["counters"]["repaired"], 1)
        # 4 个 rank 各 2 分片 + 4 条记录，都被重复提交一次
        self.assertEqual(pre["counters"]["duplicate_shards"], 8)
        self.assertEqual(pre["counters"]["duplicate_records"], 4)

    def test_flip_after_verify_caught_at_seal_then_repaired(self) -> None:
        c = self.cluster
        pre = c.do_preempt("e2e", flip_after=["r00-s0"])
        self.assertEqual(pre["outcome"], "released")
        self.assertEqual(pre["counters"]["seal_rejected"], 1)
        self.assertEqual(pre["counters"]["repaired"], 1)
        gen = c.coordinator.get_generation(pre["gen_id"])
        self.assertTrue(c.coordinator.verify_generation(gen["gen_id"])["ok"])

    def test_lost_node_keeps_incomplete_checkpoint_unsealed(self) -> None:
        c = self.cluster
        # rank 0/1 在 node-0，rank 2/3 在 node-1（ranks % 2）
        pre = c.do_preempt("e2e", lost_nodes=["e2e-node-01"])
        self.assertEqual(pre["outcome"], "aborted")
        gen = c.coordinator.get_generation(pre["gen_id"])
        self.assertEqual(gen["status"], ABORTED)
        self.assertIsNone(gen["manifest_digest"])
        # 缺失的正是失联 rank 的分片（rank % 2 == 1 的 rank 在 node-01 上）
        missing_sids = set(gen["missing"]["shards"])
        missing_ranks = set(gen["missing"]["ranks"])
        self.assertTrue(any(sid.startswith("r01") for sid in missing_sids))
        self.assertTrue(any(sid.startswith("r03") for sid in missing_sids))
        self.assertEqual(missing_ranks, {1, 3})
        # ABORTED 代际不可恢复
        with self.assertRaises(errors.GenerationNotSealed):
            c.recover("e2e", gen["gen_id"])
        # 最近可用代际仍为 None（尚未有任何 SEALED）
        self.assertIsNone(c.coordinator.latest_sealed_generation("e2e"))

    def test_slow_rank_timeout_aborts_and_late_upload_rejected(self) -> None:
        c = self.cluster
        pre = c.do_preempt(
            "e2e", slow_ranks=[3], slow_delay=1.2, deadline=0.2,
        )
        self.assertEqual(pre["outcome"], "aborted")
        self.assertGreaterEqual(pre["counters"]["late_rejected"], 1)

    def test_recover_only_from_sealed_and_corrupt_sealed_detected(self) -> None:
        c = self.cluster
        pre = c.do_preempt("e2e")
        c.recover("e2e", pre["gen_id"])
        sid = c.corrupt_sealed_blob(pre["gen_id"])
        verify = c.coordinator.verify_generation(pre["gen_id"])
        self.assertFalse(verify["ok"])
        with self.assertRaises(errors.ShardHashMismatch):
            c.recover("e2e", pre["gen_id"], replay=False)
        c.repair_sealed_blob("e2e", pre["gen_id"], sid)
        self.assertTrue(
            c.coordinator.verify_generation(pre["gen_id"])["ok"]
        )
        rec = c.recover("e2e", pre["gen_id"])
        self.assertEqual(rec["outcome"], "recovered")


class FullDemoTests(unittest.TestCase):
    def test_full_demo_report_passes(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        result = run_full_demo(
            Cluster(tmp.name), job_id="sim-001",
            reports_dir=f"{tmp.name}/reports",
        )
        self.assertEqual(result["overall"], "PASS", result["markdown"])
        # 时间线覆盖全部承诺的场景
        titles = " ".join(t["title"] for t in result["timeline"])
        for word in ("损坏", "重复检查点", "失联", "迟到", "复验", "代际"):
            self.assertIn(word, titles)
        stats = result["report"]["outputs"]
        self.assertGreater(stats["rejected"], 0)
        # 单输出不变式：报告中的键集合无重复，数量与 unique_keys 一致
        key_list = [r["out_key"] for r in stats["keys"]]
        self.assertEqual(len(key_list), stats["unique_keys"])
        self.assertEqual(len(set(key_list)), len(key_list))


if __name__ == "__main__":
    unittest.main()
