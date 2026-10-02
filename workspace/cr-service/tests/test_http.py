"""HTTP API 冒烟测试（真实端口 + urllib）。"""

from __future__ import annotations

import json
import tempfile
import urllib.error
import urllib.request
import unittest

from cr.web import serve


def _request(method: str, url: str, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


class HttpSmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._tmp = tempfile.TemporaryDirectory()
        cls.httpd, cls.cluster = serve(
            cls._tmp.name, host="127.0.0.1", port=0,
        )
        host, port = cls.httpd.server_address
        cls.base = f"http://{host}:{port}"

    @classmethod
    def tearDownClass(cls) -> None:
        cls.httpd.shutdown()
        cls._tmp.cleanup()

    def test_page_and_status(self) -> None:
        with urllib.request.urlopen(f"{self.base}/", timeout=5) as resp:
            html = resp.read().decode()
        self.assertIn("抢占与检查点恢复", html)
        status, payload = _request("GET", f"{self.base}/api/status")
        self.assertEqual(status, 200)
        self.assertTrue(payload["jobs"])

    def test_preempt_recover_flow_via_api(self) -> None:
        base = self.base
        status, pre = _request("POST", f"{base}/api/jobs/sim-001/preempt", {})
        self.assertEqual(status, 200, pre)
        self.assertEqual(pre["outcome"], "released")

        status, gens = _request(
            "GET", f"{base}/api/jobs/sim-001/generations"
        )
        self.assertEqual(status, 200)
        self.assertTrue(any(g["status"] == "SEALED" for g in gens))

        status, rec = _request(
            "POST", f"{base}/api/jobs/sim-001/recover",
            {"gen_id": pre["gen_id"]},
        )
        self.assertEqual(status, 200, rec)
        self.assertEqual(rec["token"] is not None, True)

        status, z = _request(
            "POST", f"{base}/api/jobs/sim-001/zombie",
            {"rank": 0, "seq": 0},
        )
        self.assertEqual(status, 200)
        self.assertTrue(z["blocked"])
        self.assertIn(z["code"], ("lease_invalid", "lease_epoch_stale",
                                   "lease_generation_stale"))

        status, report = _request(
            "GET", f"{base}/api/jobs/sim-001/report"
        )
        self.assertEqual(status, 200)
        self.assertEqual(report["overall"], "PASS")

    def test_lost_node_api_returns_aborted_and_no_seal(self) -> None:
        # 重置数据用新作业，避免与上面的状态纠缠
        _request("POST", f"{self.base}/api/jobs",
                 {"job_id": "smoke2", "ranks": 4})
        status, res = _request(
            "POST", f"{self.base}/api/jobs/smoke2/preempt",
            {"lost_nodes": ["smoke2-node-01"]},
        )
        self.assertEqual(status, 200)
        self.assertEqual(res["outcome"], "aborted")
        status, gens = _request("GET", f"{self.base}/api/jobs/smoke2/generations")
        self.assertTrue(all(g["status"] == "ABORTED" for g in gens))


if __name__ == "__main__":
    unittest.main()
