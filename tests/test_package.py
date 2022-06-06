import datetime
import decimal
import pytest

from package import Package,check_if_to_delete,get_custom_prices
from util import write_to_tempfile

package_id = 'package-kQbzEH9yiZt7' # scott+anothertest@ourresearch.org, "EmptyPackage"
package = Package.query.filter(Package.package_id == package_id).scalar()

package_id2 = 'package-NZe2awMnex6K' # scott+anothertest@ourresearch.org, "Sage-HopeCollege"
package_small = Package.query.filter(Package.package_id == package_id2).scalar()

package_id3 = 'package-testingYJVJmYWBuLSY' # team+consortiumtest@ourresearch.org, "Test Big University"
package_feedback = Package.query.filter(Package.package_id == package_id3).scalar()

def test_jisc_price_warnings():
    # missing_prices warnings shouldn't exist for JISC packages 
    # since we use default prices when there's no public or custom price
    package_id_cra_sage = 'package-jiscsagecra' # team+jisc@ourresearch.org
    package_jisc_cra_sage = Package.query.filter(Package.package_id == package_id_cra_sage).scalar()
    package_id_shu_tf = 'package-jisctfshu'
    package_jisc_shu_tf = Package.query.filter(Package.package_id == package_id_shu_tf).scalar()

    # warnings is empty b/c we use a default price
    assert len(package_jisc_cra_sage.warnings) == 0
    assert len(package_jisc_shu_tf.warnings) == 0

    # but there are actually missing prices
    assert len(package_jisc_cra_sage.journals_missing_prices) > 0
    assert len(package_jisc_shu_tf.journals_missing_prices) > 0

def test_to_package_dict():
    package_dict = package.to_package_dict()
    
    assert isinstance(package_dict, dict)
    assert package_dict['publisher'] == 'Elsevier'
    assert package_dict['is_consortial_proposal_set'] == False

def test_to_package_dict_feedback():
    feedback_package_id = 'feedback-jiscelsnr1' # british antarctic survey, Feedback on Elsevier scenarios
    feedback_package_id = feedback_package_id.replace("feedback-", "package-")
    feedback_package = Package.query.filter(Package.package_id == feedback_package_id).scalar()
    feedback_package_dict = feedback_package.to_package_dict()
    feedback_package_dict_min = feedback_package.to_dict_minimal()
    
    assert isinstance(feedback_package_dict, dict)
    assert feedback_package_dict['publisher'] == 'Elsevier'
    assert feedback_package_dict['currency'] == "GBP"
    assert feedback_package_dict['is_consortial_proposal_set'] == True

def test_update_apc_authorships():
    rows = package_small.update_apc_authorships()
    assert rows is None

def test_methods_that_use_get_base():
    '''
    Test that properties work with bind variables
    '''
    pub2019 = package_small.get_published_in_2019
    pubtoll2019 = package_small.get_published_toll_access_in_2019
    counteruniqrows = package_small.get_counter_unique_rows

    assert isinstance(pub2019, list)
    assert isinstance(pubtoll2019, list)
    assert isinstance(counteruniqrows, list)
    
def test_feedback_rows():
    # not a consortium feedback package
    rows = package_small.feedback_rows
    assert rows == []

    # a consortium feedback package
    rows_feedback = package_feedback.feedback_rows
    assert len(rows_feedback) > 0
    assert isinstance(rows_feedback[0], list)

def test_consortia_scenario_ids_who_own_this_package():
    # not a consortium feedback package
    consrtium_ids = package_small.consortia_scenario_ids_who_own_this_package
    assert consrtium_ids == []

    # a consortium feedback package
    consrtium_ids_feedback = package_feedback.consortia_scenario_ids_who_own_this_package
    assert len(consrtium_ids_feedback) > 0
    assert isinstance(consrtium_ids_feedback[0], str)

def test_data_files_dict():
    out = package_small.data_files_dict
    assert isinstance(out, dict)
    assert list(out.keys()) == ['counter', 'counter-trj2', 'counter-trj3', 'counter-trj4', 'price-public', 'price', 'perpetual-access']
    assert isinstance(out['perpetual-access'], dict)
    assert isinstance(out['perpetual-access']['rows_count'], decimal.Decimal)

def test_check_if_to_delete():
    # check_if_to_delete() only ever used with file='price'
    res = check_if_to_delete(package_small.package_id, "price")
    assert isinstance(res, bool)

def test_get_custom_prices():
    # package w/o custom prices
    out = get_custom_prices(package.package_id)
    assert len(out) == 0

    # package w/ custom prices
    out = get_custom_prices(package_small.package_id)
    assert len(out) > 0
