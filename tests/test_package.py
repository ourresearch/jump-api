import datetime
import pytest

from package import Package
from util import write_to_tempfile

package_id = 'package-kQbzEH9yiZt7' # scott+anothertest@ourresearch.org, "EmptyPackage"
package = Package.query.filter(Package.package_id == package_id).scalar()

package_id2 = 'package-NZe2awMnex6K' # scott+anothertest@ourresearch.org, "Sage-HopeCollege"
package_small = Package.query.filter(Package.package_id == package_id2).scalar()

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

def test_xxx():
    pass

