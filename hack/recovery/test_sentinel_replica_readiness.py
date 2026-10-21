import copy
import unittest

from sentinel_replica_readiness import verify


class ReplicaReadinessTest(unittest.TestCase):
    def setUp(self):
        self.primary = ["10.0.0.1", "upgrade-0.lab"]
        self.expected = [
            {"name": "upgrade-1", "addresses": ["10.0.0.2", "upgrade-1.lab"], "runid": "a" * 40},
            {"name": "upgrade-2", "addresses": ["10.0.0.3", "upgrade-2.lab"], "runid": "b" * 40},
        ]
        self.records = [self.record(item) for item in self.expected]

    def record(self, item):
        return {"ip": item["addresses"][0], "port": "6379", "runid": item["runid"],
                "flags": "slave", "role-reported": "slave", "master-host": self.primary[0],
                "master-port": "6379", "master-link-status": "ok", "slave-priority": "100",
                "replica-announced": "1", "info-refresh": "1000",
                "last-ok-ping-reply": "100", "down-after-milliseconds": "5000"}

    def test_current_replicas_pass_with_ip_or_exact_hostname(self):
        self.records[1]["ip"] = "upgrade-2.lab"
        verify(self.records, self.expected, self.primary)

    def test_stale_count_only_false_positive_is_rejected(self):
        for record in self.records:
            record.update(ip="10.9.9.9", flags="s_down,slave", runid="", **{"info-refresh": "0"})
        with self.assertRaises(ValueError):
            verify(self.records, self.expected, self.primary)

    def test_retained_down_records_do_not_count_as_current_replicas(self):
        stale = dict(self.records[0], ip="10.9.9.9", flags="s_down,slave", runid="")
        verify(self.records + [stale], self.expected, self.primary)

    def test_each_eligibility_and_identity_field_is_required(self):
        for key, value in {
            "ip": "upgrade-1.other-namespace", "port": "6380", "runid": "c" * 40,
            "flags": "slave,disconnected", "role-reported": "master", "master-host": "10.9.9.9",
            "master-port": "6380", "master-link-status": "err", "slave-priority": "0",
            "replica-announced": "0", "info-refresh": "30000", "last-ok-ping-reply": "5000",
            "down-after-milliseconds": "bogus",
        }.items():
            with self.subTest(field=key):
                records = copy.deepcopy(self.records)
                records[0][key] = value
                with self.assertRaises(ValueError):
                    verify(records, self.expected, self.primary)

    def test_duplicate_observations_cannot_replace_a_missing_replica(self):
        with self.assertRaises(ValueError):
            verify([self.records[0], self.records[0]], self.expected, self.primary)

    def test_live_unknown_replica_is_rejected_even_when_expected_peers_exist(self):
        unknown = dict(self.records[0], ip="10.9.9.9", runid="c" * 40)
        with self.assertRaises(ValueError):
            verify(self.records + [unknown], self.expected, self.primary)

    def test_missing_fields_are_not_healthy_defaults(self):
        for key in self.records[0]:
            with self.subTest(field=key):
                records = copy.deepcopy(self.records)
                del records[0][key]
                with self.assertRaises(ValueError):
                    verify(records, self.expected, self.primary)


if __name__ == "__main__":
    unittest.main()
