from response_test import ResponseTest
from test_package import packages_to_check
from saved_scenario import SavedScenario

scenarios_to_check = [
    s for s in [
        SavedScenario.query.filter(SavedScenario.package_id == p.package_id).first() for p in packages_to_check if p
    ] if s
]


class TestScenario(ResponseTest):
    def test_scenario(self):
        pass

    def test_scenario_journals(self):
        pass

    def test_scenario_raw(self):
        pass

    def test_scenario_slider(self):
        pass

    def test_scenario_subscriptions(self):
        pass

    def test_scenario_table(self):
        pass

    def test_scenario_export(self):
        pass