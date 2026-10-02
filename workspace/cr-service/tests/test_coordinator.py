"""协调器状态机的单元不变式测试。"""

from __future__ import annotations

import tempfile
import threading
import unittest

from cr import errors
from cr.cluster import shard_payload
from cr.coordinator import OPEN, SEALED
from cr.events import EventLog
from cr.storage import CASStore, sha256_hex
from cr.coordinator import CheckpointCoordinator


def make_coordinator(root: str):
    cas = CASStore(f"{root}/cas")
    events = EventLog()
    coord = CheckpointCoordinator(root, cas, events)
    coord.create_job("j1", ranks=2)
    return coord, cas


def specs(epoch: int, size: int = 1024):
    procs = []
    shards = []
    for rank in range(2):
        ids = [f"r{rank:02d}-s{j}" for j in range(2)]
        procs.append({
            "rank": rank, "node": f"n{rank}", "pid": 1000 + rank,
            "shards": ids,
            "input_positions": {"input-0": 1234 * rank},
            "last_output_seq": 2,
        })
        for j, sid in enumerate(ids):
            data = shard_payload("j1", rank, epoch, j, size)
            shards.append({"shard_id": sid, "size": size,
                           "expected_digest": sha256_hex(data)})
    return procs, shards


def fill(coord, gen_id: str, epoch: int, size: int = 1024) -> None:
    for rank in range(2):
        for j in range(2):
            sid = f"r{rank:02d}-s{j}"
            coord.upload_shard(
                gen_id, epoch, sid,
                shard_payload("j1", rank, epoch, j, size),
            )
        coord.submit_rank_record(gen_id, epoch, rank, {
            "rank": rank, "node": f"n{rank}", "pid": 1000 + rank,
            "shards": [f"r{rank:02d}-s{j}" for j in range(2)],
            "input_positions": {"input-0": 1234 * rank},
            "last_output_seq": 2,
        })


class CoordinatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = self._tmp.name
        self.coord, self.cas = make_coordinator(self.root)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_happy_path_seal_and_manifest(self) -> None:
        procs, sh = specs(epoch=1)
        view = self.coord.begin_preemption("j1", procs, sh)
        self.assertEqual(view["status"], OPEN)
        fill(self.coord, view["gen_id"], epoch=1)
        sealed = self.coord.seal(view["gen_id"], epoch=1)
        self.assertEqual(sealed["status"], SEALED)
        self.assertIsNotNone(sealed["manifest_digest"])
        verify = self.coord.verify_generation(sealed["gen_id"])
        self.assertTrue(verify["ok"], verify)
        self.assertEqual(self.coord.latest_sealed_generation("j1"),
                         sealed["gen_id"])

    def test_incomplete_cannot_seal(self) -> None:
        procs, sh = specs(epoch=1)
        view = self.coord.begin_preemption("j1", procs, sh)
        # 只上传一半分片、不交任何进程记录
        self.coord.upload_shard(
            view["gen_id"], 1, "r00-s0",
            shard_payload("j1", 0, 1, 0, 1024),
        )
        with self.assertRaises(errors.CheckpointIncomplete):
            self.coord.seal(view["gen_id"], 1)
        self.assertEqual(self.coord.get_generation(view["gen_id"])["status"],
                         OPEN)

    def test_bad_shard_rejected_and_remains_unavailable(self) -> None:
        procs, sh = specs(epoch=1)
        view = self.coord.begin_preemption("j1", procs, sh)
        with self.assertRaises(errors.ShardHashMismatch):
            self.coord.upload_shard(
                view["gen_id"], 1, "r00-s0", b"definitely wrong bytes\n",
            )
        entry = view = self.coord.get_generation(view["gen_id"])
        self.assertEqual(entry["shards"]["r00-s0"]["status"], "REJECTED")
        # 其余全交齐也不能封印（损坏的那个仍然没校验）
        for rank in range(2):
            for j in range(2):
                sid = f"r{rank:02d}-s{j}"
                if sid == "r00-s0":
                    continue
                self.coord.upload_shard(
                    entry["gen_id"], 1, sid,
                    shard_payload("j1", rank, 1, j, 1024),
                )
            self.coord.submit_rank_record(entry["gen_id"], 1, rank, {
                "rank": rank, "node": f"n{rank}", "pid": 1000 + rank,
                "shards": [f"r{rank:02d}-s{j}" for j in range(2)],
                "input_positions": {"input-0": 1234 * rank},
                "last_output_seq": 2,
            })
        with self.assertRaises(errors.CheckpointIncomplete):
            self.coord.seal(entry["gen_id"], 1)

    def test_duplicate_checkpoint_idempotent(self) -> None:
        procs, sh = specs(epoch=1)
        v1 = self.coord.begin_preemption("j1", procs, sh)
        v2 = self.coord.begin_preemption("j1", procs, sh)  # 重复签发
        self.assertEqual(v1["gen_id"], v2["gen_id"])
        fill(self.coord, v1["gen_id"], 1)
        data = shard_payload("j1", 0, 1, 0, 1024)
        # 同摘要重复上传必须幂等成功，不得产生冲突/双份
        self.coord.upload_shard(v1["gen_id"], 1, "r00-s0", data)
        self.coord.seal(v1["gen_id"], 1)

    def test_duplicate_with_different_content_conflicts(self) -> None:
        procs, sh = specs(epoch=1)
        v = self.coord.begin_preemption("j1", procs, sh)
        data = shard_payload("j1", 0, 1, 0, 1024)
        self.coord.upload_shard(v["gen_id"], 1, "r00-s0", data)
        with self.assertRaises(errors.CheckpointConflict):
            self.coord.upload_shard(v["gen_id"], 1, "r00-s0",
                                    b"different content for same shard\n")

    def test_stale_epoch_upload_rejected(self) -> None:
        procs, sh = specs(epoch=1)
        v = self.coord.begin_preemption("j1", procs, sh)
        self.assertEqual(v["epoch"], 1)
        with self.assertRaises(errors.EpochStale):
            self.coord.upload_shard(v["gen_id"], 0, "r00-s0",
                                    shard_payload("j1", 0, 1, 0, 1024))

    def test_abort_then_late_upload_rejected(self) -> None:
        procs, sh = specs(epoch=1)
        v = self.coord.begin_preemption("j1", procs, sh)
        self.coord.abort(v["gen_id"], "节点失联")
        with self.assertRaises(errors.GenerationAborted):
            self.coord.upload_shard(
                v["gen_id"], 1, "r00-s0",
                shard_payload("j1", 0, 1, 0, 1024),
            )
        with self.assertRaises(errors.GenerationNotSealed):
            self.coord.verify_generation(v["gen_id"])

    def test_bit_rot_after_seal_caught_by_reverify(self) -> None:
        procs, sh = specs(epoch=1)
        v = self.coord.begin_preemption("j1", procs, sh)
        fill(self.coord, v["gen_id"], 1)
        sealed = self.coord.seal(v["gen_id"], 1)
        self.cas.corrupt_for_test(sealed["shards"]["r01-s1"]["digest"])
        verify = self.coord.verify_generation(sealed["gen_id"])
        self.assertFalse(verify["ok"])
        self.assertTrue(any("r01-s1" in p for p in verify["problems"]))

    def test_concurrent_uploads_seal_once(self) -> None:
        procs, sh = specs(epoch=1)
        v = self.coord.begin_preemption("j1", procs, sh)
        errors_seen: list[Exception] = []

        def worker(rank: int) -> None:
            try:
                for j in range(2):
                    self.coord.upload_shard(
                        v["gen_id"], 1, f"r{rank:02d}-s{j}",
                        shard_payload("j1", rank, 1, j, 1024),
                    )
                self.coord.submit_rank_record(v["gen_id"], 1, rank, {
                    "rank": rank, "node": f"n{rank}", "pid": 1000 + rank,
                    "shards": [f"r{rank:02d}-s{j}" for j in range(2)],
                    "input_positions": {"input-0": 1234 * rank},
                    "last_output_seq": 2,
                })
            except Exception as exc:  # noqa: BLE001
                errors_seen.append(exc)

        threads = [threading.Thread(target=worker, args=(r,)) for r in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors_seen, [])
        sealed = self.coord.seal(v["gen_id"], 1)
        self.assertEqual(sealed["status"], SEALED)
        # 重复封印幂等
        again = self.coord.seal(v["gen_id"], 1)
        self.assertEqual(again["status"], SEALED)


if __name__ == "__main__":
    unittest.main()
