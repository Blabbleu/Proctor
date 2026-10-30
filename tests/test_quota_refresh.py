"""Offline quota refresh regression tests."""
import atexit
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

_sandbox = tempfile.TemporaryDirectory()
atexit.register(_sandbox.cleanup)
os.environ["PROCTOR_ETC"] = str(Path(_sandbox.name) / "etc")
os.environ["PROCTOR_STATE"] = str(Path(_sandbox.name) / "state")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from proctor import broker, db  # noqa: E402

SETTINGS = {"quota": {"enabled": True, "max_5h": 0.90, "max_7d": 0.90},
            "review": {"for_openai-codex": ["claude-sonnet-5-5", "gpt-5.6-terra"]}}
PROJECT = {"id": "tp"}
JOB = {"harness": "codex", "model": "gpt-6-luna"}
T0 = 1_800_000_000.0


class Env:
    """Fresh DB, fake clock, fake `hermes usage` that records calls and returns scripted readings."""

    def __init__(self, live=(0.04, 0.04), fail=False):
        self.dir = tempfile.TemporaryDirectory()
        self.conn = sqlite3.connect(":memory:", isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        db.init(self.conn)
        self.t, self.calls, self.live, self.fail = T0, [], live, fail
        # fresh ChatGPT reading, so only the Claude row is under test (the next reviewer is GPT)
        self.conn.execute("INSERT INTO quota(id,ts,util_5h,util_7d,status,source) VALUES (?,?,0.1,0.1,NULL,'test')",
                          (broker.QUOTA_ROW["openai-codex"], T0))
        self.saved = (broker.now, broker.handle_quota, broker.model_allowed)
        broker.now = lambda: self.t
        broker.handle_quota = self.fake_usage
        broker.model_allowed = lambda project, settings, m: True

    def fake_usage(self, conn, req, payload):
        self.calls.append(payload["provider"])
        if self.fail:
            raise RuntimeError("hermes usage unavailable")
        u5, u7 = self.live
        broker.record_quota(conn, [("anthropic-ratelimit-unified-5h-utilization", str(u5)),
                                   ("anthropic-ratelimit-unified-7d-utilization", str(u7))],
                            source="hermes usage", provider=payload["provider"])

    def set_row(self, u5, u7, age):
        self.conn.execute("INSERT INTO quota(id,ts,util_5h,util_7d,status,source) VALUES (?,?,?,?,NULL,'test') "
                          "ON CONFLICT(id) DO UPDATE SET ts=excluded.ts, util_5h=excluded.util_5h, "
                          "util_7d=excluded.util_7d, source=excluded.source",
                          (broker.QUOTA_ROW["anthropic"], self.t - age, u5, u7))

    def close(self):
        broker.now, broker.handle_quota, broker.model_allowed = self.saved
        self.conn.close()
        self.dir.cleanup()


def t_stale_high_reading_is_refreshed_and_sonnet_reviews():
    # The incident: 90% weekly reading two days old, actual usage 4% -> Sonnet must be picked.
    e = Env(live=(0.07, 0.04))
    try:
        e.set_row(0.08, 0.90, age=2 * 86400)
        assert broker.quota_block_reason(e.conn, SETTINGS, "anthropic"), "precondition: stale row blocks"
        model, skipped = broker.pick_reviewer(e.conn, SETTINGS, PROJECT, JOB)
        assert model == "claude-sonnet-5-5", (model, skipped)
        assert e.calls == ["anthropic"], e.calls
    finally:
        e.close()


def t_fresh_reading_is_trusted_without_a_usage_call():
    e = Env()
    try:
        e.set_row(0.10, 0.20, age=60)
        model, _ = broker.pick_reviewer(e.conn, SETTINGS, PROJECT, JOB)
        assert model == "claude-sonnet-5-5" and e.calls == [], (model, e.calls)
    finally:
        e.close()


def t_fresh_high_reading_still_blocks():
    # A recent over-limit reading is real: no refresh, Sonnet skipped, next reviewer used.
    e = Env()
    try:
        e.set_row(0.95, 0.50, age=60)
        model, skipped = broker.pick_reviewer(e.conn, SETTINGS, PROJECT, JOB)
        assert model == "gpt-5.6-terra" and e.calls == [], (model, e.calls)
        assert any("5h subscription window" in s for s in skipped), skipped
    finally:
        e.close()


def t_stale_low_reading_that_is_now_high_blocks():
    # Refresh works in both directions: guard must hold if usage climbed since the last reading.
    e = Env(live=(0.95, 0.40))
    try:
        e.set_row(0.05, 0.05, age=3600)
        model, skipped = broker.pick_reviewer(e.conn, SETTINGS, PROJECT, JOB)
        assert model == "gpt-5.6-terra", (model, skipped)
        assert e.calls == ["anthropic"], e.calls
    finally:
        e.close()


def t_missing_row_is_read():
    e = Env()
    try:
        assert broker.refresh_quota_if_stale(e.conn, "anthropic") is True
        q = e.conn.execute("SELECT util_7d FROM quota WHERE id=?", (broker.QUOTA_ROW["anthropic"],)).fetchone()
        assert q is not None and abs(q["util_7d"] - 0.04) < 1e-9, dict(q) if q else None
    finally:
        e.close()


def t_refresh_failure_keeps_old_row_and_old_verdict():
    e = Env(fail=True)
    try:
        e.set_row(0.08, 0.90, age=2 * 86400)
        assert broker.refresh_quota_if_stale(e.conn, "anthropic") is False
        model, skipped = broker.pick_reviewer(e.conn, SETTINGS, PROJECT, JOB)
        assert model == "gpt-5.6-terra", (model, skipped)
        assert any("7d subscription window" in s for s in skipped), skipped
    finally:
        e.close()


TESTS = [t_stale_high_reading_is_refreshed_and_sonnet_reviews, t_fresh_reading_is_trusted_without_a_usage_call,
         t_fresh_high_reading_still_blocks, t_stale_low_reading_that_is_now_high_blocks, t_missing_row_is_read,
         t_refresh_failure_keeps_old_row_and_old_verdict]


class QuotaRefreshTests(unittest.TestCase):
    pass


for _test in TESTS:
    setattr(QuotaRefreshTests, "test_" + _test.__name__[2:], lambda self, fn=_test: fn())


if __name__ == "__main__":
    unittest.main()
