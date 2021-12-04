import pytest
from saved_scenario import SavedScenario,save_raw_scenario_to_db,get_latest_scenario_raw,get_latest_scenario

scenario_id2 = '8kPSbFCN' # scott+anothertest@ourresearch.org, "Sage-HopeCollege"/"potatoes"

def test_save_raw_scenario_to_db():
    scenario = SavedScenario.query.get(scenario_id2)
    my_dict = scenario.to_dict_saved_from_db()
    if my_dict['configs']['weight_citation'] == 5:
         new_weight = 10
    else:
        new_weight = 5
    my_dict['configs']['weight_citation'] = new_weight
    save_raw_scenario_to_db(scenario_id2, my_dict, None)
    after_update = scenario.to_dict_saved_from_db()

    assert after_update['configs']['weight_citation'] == new_weight

def test_get_latest_scenario_raw():
    scenario_raw_before_update = get_latest_scenario_raw(scenario_id2)

    scenario = SavedScenario.query.get(scenario_id2)
    my_dict = scenario.to_dict_saved_from_db()
    if my_dict['configs']['notes'] == "":
         new_note = "changing notes from test suite"
    else:
        pytest.skip('skipping test')
    my_dict['configs']['notes'] = new_note
    save_raw_scenario_to_db(scenario_id2, my_dict, None)

    scenario_raw_after_update = get_latest_scenario_raw(scenario_id2)

    assert scenario_raw_before_update[1]['configs']['notes'] == ""
    assert scenario_raw_after_update[1]['configs']['notes'] == new_note

    # cleanup
    my_dict['configs']['notes'] = ""
    save_raw_scenario_to_db(scenario_id2, my_dict, None)

def test_get_latest_scenario():
    scenario_before_update = get_latest_scenario(scenario_id2)

    scenario = SavedScenario.query.get(scenario_id2)
    my_dict = scenario.to_dict_saved_from_db()
    if my_dict['configs']['description'] == "":
         new_description = "this is a description!"
    else:
        pytest.skip('skipping test')
    my_dict['configs']['description'] = new_description
    save_raw_scenario_to_db(scenario_id2, my_dict, None)

    scenario_after_update = get_latest_scenario(scenario_id2)

    assert scenario_before_update.to_dict_summary()['_settings']['description'] == ""
    assert scenario_after_update.to_dict_summary()['_settings']['description'] == new_description

    # cleanup
    my_dict['configs']['description'] = ""
    save_raw_scenario_to_db(scenario_id2, my_dict, None)

def test_saved_scenario():
    scenario_by_query = SavedScenario.query.get(scenario_id2)
    scenario_by_id = SavedScenario(False, scenario_id2, None)
    
    assert id(scenario_by_query) != id(scenario_by_id)
    # in scenario by query package is populated, but not in scenario by id
    assert isinstance(scenario_by_query.to_dict_meta(), dict)
    with pytest.raises(AttributeError):
        scenario_by_id.to_dict_meta()
    
