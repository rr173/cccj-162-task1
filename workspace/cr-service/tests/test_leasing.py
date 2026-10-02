"""输出租约栅栏与键级写一次的测试。"""

from __future__ import annotations

import tempfile
import unittest

from cr import errors
from cr.events import EventLog
from cr.leasing import LeaseManager, OutputSink


class LeaseSinkTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        root = self._tmp.name
        self.events = EventLog()
        self.leases = LeaseManager(root, self.events)
        self.sink = OutputSink(f"{root}/cas", root, self.leases, self.events)
        self.l1 = self.leases.acquire("j1", 0, "origin", "start")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_same_payload_dedup_within_valid_lease(self) -> None:
        r1 = self.sink.write("j1", self.l1.token, 0, "origin",
                             "k1", b"hello", 0, 0)
        r2 = self.sink.write("j1", self.l1.token, 0, "origin",
                             "k1", b"hello", 0, 0)
        self.assertEqual(r1["result"], OutputSink.ACCEPTED)
        self.assertEqual(r2["result"], OutputSink.DEDUP)
        self.assertEqual(self.sink.stats_for("j1")["unique_keys"], 1)

    def test_divergent_payload_within_valid_lease_conflicts(self) -> None:
        self.sink.write("j1", self.l1.token, 0, "origin",
                        "k1", b"hello", 0, 0)
        with self.assertRaises(errors.DuplicateOutput):
            self.sink.write("j1", self.l1.token, 0, "origin",
                            "k1", b"WORLD", 0, 0)
        self.assertEqual(self.sink.stats_for("j1")["content_conflicts"], 1)
        self.assertEqual(self.sink.stats_for("j1")["unique_keys"], 1)

    def test_old_lease_rejected_after_recovery(self) -> None:
        self.sink.write("j1", self.l1.token, 0, "origin",
                        "k1", b"v1", 0, 0)
        l2 = self.leases.acquire("j1", 1, "j1-ep1", "recover")
        # 旧进程迟到写：旧 token 已吊销
        with self.assertRaises(errors.LeaseInvalid):
            self.sink.write("j1", self.l1.token, 0, "origin",
                            "k2", b"late", 0, 1)
        # 即使内容与已有输出相同，旧租约也在栅栏处被拒（不是 dedup）
        with self.assertRaises(errors.LeaseInvalid):
            self.sink.write("j1", self.l1.token, 0, "origin",
                            "k1", b"v1", 0, 0)
        # 新租约写同键同内容 -> 恢复重放，幂等去重，无双输出
        r = self.sink.write("j1", l2.token, 1, "j1-ep1",
                            "k1", b"v1", 0, 0)
        self.assertEqual(r["result"], OutputSink.DEDUP)
        stats = self.sink.stats_for("j1")
        self.assertEqual(stats["unique_keys"], 1)
        self.assertEqual(stats["rejected"], 2)

    def test_wrong_epoch_or_gen_token_rejected(self) -> None:
        l2 = self.leases.acquire("j1", 1, "j1-ep1", "recover")
        with self.assertRaises(errors.LeaseEpochStale):
            # 伪造 epoch 与租约不符
            self.sink.write("j1", l2.token, 2, "j1-ep1",
                            "k9", b"x", 0, 9)
        with self.assertRaises(errors.LeaseGenerationStale):
            self.sink.write("j1", l2.token, 1, "j1-ep9",
                            "k9", b"x", 0, 9)


if __name__ == "__main__":
    unittest.main()
