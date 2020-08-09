# coding: utf-8

from time import time
from simplejson import dumps
import gzip
import StringIO
import datetime


from scenario import Scenario
from consortium import Consortium
from consortium import consortium_get_computed_data
from saved_scenario import SavedScenario

from app import get_db_cursor
from util import elapsed
from util import myconverter
from util import chunks


# select distinct package.package_id from jump_user_institution_permission perm
# join jump_user on perm.user_id=jump_user.id
# join jump_institution inst on perm.institution_id=inst.id
# join jump_account_package package on perm.institution_id=package.institution_id
# where email ilike '%crkn.ca' and publisher='Elsevier'

package_id_lists = {}

package_id_lists["viva"] =  """
    package-52QwUfPkzzeC
    package-YXDjDAdukQqQ
    package-ibrVYqT5oZDP
    package-AZHUvPnphoSX
    package-kL8pHoP2yEa5
    package-jkh3oxPUnZ6E
    package-THLdmhsVG62G
    package-Rrnx9pg7sYBM
    package-Qoh2zwnkD5ew
    package-GzuR3bjQM9ry
    package-p5Pf6ZmzgjWj
    package-AhtpHzphSFz8
    package-ybbvkiYTFkS4
    package-whC8X3MPgmS4
    package-DWxNZ44W6omv
    package-P3mtpwVB6bVc
    package-DzogjcNWE2oP
    package-SCZ8VL5myxNU
    package-dHphAZKWuJQK
    package-ouCkFbGFRSM4
    package-RMbzHJhZPACD
    package-TykKdDtwjEFY
    package-YREpAM2DdTHE
    package-idcs9weHxRWq
    """.split()

package_id_lists["colorado_alliance"] =  """
    publisher-aPeUQvAtnqQt
    publisher-vMe9TF6ve5SJ
    publisher-U9or8bGcEYfH
    publisher-g8XPNueMcRaU
    publisher-K4HHcDcdJEki
    publisher-DAaN7j7W3iWt
    publisher-nPeiwnS83SJu
    publisher-Nk3SY2VnSJsv
    publisher-MvaTciq2NMio
    publisher-6eUSTRkApZmX
    publisher-79tpE9xgrCEw
    publisher-kSmz5DSwMHus
    publisher-cAHG8Mezg3n9
    publisher-9DZjv4MZ67wh
    """.split()

package_id_lists["purdue_consortium"] =  """
    publisher-goSpcRtZ4qNU
    publisher-ioVaFCDAmCrv
    package-rsGRcwftNoMB
    """.split()

package_id_lists["crkn"] =  """
    publisher-o9PauxjfPMz4
    publisher-WRx7ZY5SSsxP
    publisher-o3SdtekPUoRo
    publisher-oS7QKvj9QbiC
    publisher-ZwuGRmw6dP42
    publisher-DiekqPukSkkr
    publisher-TiJsgVE8KEiM
    publisher-5Ec5DKoN2wrF
    publisher-4Vyqo7WkA25H
    publisher-oeCuf2UmHTzq
    publisher-dJHjctFwrEeM
    publisher-VjtUxtLriQQg
    publisher-DQE8frfDNhpt
    publisher-zH7ewjWLU26w
    publisher-nGmHf2o6NB4s
    publisher-rqssfTVhN7Kt
    publisher-oaStZTPTfGMf
    publisher-hknpJJmzsHtx
    publisher-bYigYe5cvgvX
    publisher-SBR7RWAAaFFS
    publisher-EubnAxSqaExJ
    publisher-V8RDuTi2QEwS
    publisher-fQgpHCuU6C8T
    publisher-xft6YjgdGGAy
    publisher-7eeWRrGxbCXL
    publisher-CWMyNKT5Uq4F
    publisher-arbPZEa7Sww3
    publisher-px7NzuZcxhVg
    publisher-icut2SSCcypj
    publisher-q2B3B8cqAFwh
    publisher-tjAmZHmHHSJB
    publisher-PQBbbCuGWhWA
    publisher-z8TYMujibfU8
    publisher-7r8tkM2RNfMP
    publisher-3h5qoJEjg6fR
    publisher-TStLDaferQpP
    publisher-ELVvdVLK3tkN
    publisher-bZU9akRAzqAJ
    publisher-jgQRLKAmvyKZ
    publisher-c7hgxFveTh8H
    publisher-Ni2KU2MD9uCG
    publisher-MknqbgqeXxrE
    publisher-DsgxwSaR5SWa
    publisher-6x26gNeWTgrr
    publisher-VSQ86qXZMRQy
    publisher-L6UCfoZQui9T
    publisher-qJnnTridV9UP
    publisher-NVcnECSCRJrR
    publisher-e6JfAsgotj3w
    publisher-TPdNL9K79Ner
    publisher-7h8P6cdVFoWg
    publisher-nSRwNDrfG868
    publisher-c5mzFGv5tnyf
    publisher-rSwBDZvHZWjt
    publisher-YxgnVr3oMJB9
    publisher-PyJbogWnsbQg
    publisher-2gWZFbB7zyGZ
    publisher-uGfgZQabJ4an
    publisher-V7VcL6WmMpKE
    publisher-5qxFAuXG55vX
    publisher-Y2AhQme5cS7p
    publisher-4mhaHXfpyA62
    publisher-nUXmwUQWBjAd
    publisher-MGhNsnfR4d7V
    publisher-JJYhMFqGtoUg
    publisher-28jomUd6ysVq
    publisher-o9r7QXbQ9XQv
    publisher-sQa5tz7NKPWo
    publisher-8SFDZYWGUsmL
    publisher-42eqUFbzk267
""".split()
package_id_lists["crkn_test"] = package_id_lists["crkn"]

package_id_lists["suny"] =  """
4b13bf38
c154f720
ac94231a
0611f07c
6d97f6e7
d00f5eef
b514d37c
340c2753
68f1af1d
9255d9e3
54507d10
7ac8a211
db00c4e7
03120c5a
72dacc89
e0910428
fb622890
06f6e7eb
1a368990
2cdb3729
d7f44ebb
1ed1e914
e7db8d07
5a41846a
bf9bdf63
3c4ad599
ac002ce0
1663beb4
6926d1c5
ce8168da
35834b2a
431af4c7
69a9ac99
05030fb8
fde097cf
93df2d1d
630b0313
3d96a310
1be0d2ef
2b7daf77
e807b3ab
aaf55e33
30bc16d3
564a809c
ce1ed9c8
6574539e
74c7c680
1dcbb105
ea0dee2b
47c91aca
b0d9faaa
04d176fe
fd8ce776
03ae4f6a
54bee1de
bcf44441
8918aa3c
c1da8de9
""".split()


def jsonify_fast_no_sort_simple(*args, **kwargs):
    if args and kwargs:
        raise TypeError('jsonify() behavior undefined when passed both args and kwargs')
    elif len(args) == 1:  # single args are passed directly to dumps()
        data = args[0]
    else:
        data = args or kwargs

    # turn this to False to be even faster, but warning then responses may not cache
    sort_keys = False

    return dumps(data,
              skipkeys=True,
              ensure_ascii=True,
              check_circular=False,
              allow_nan=True,
              cls=None,
              default=myconverter,
              indent=None,
              # separators=None,
              sort_keys=sort_keys)


def refresh_data_for_consortium(consortium_name):
    print "consortium_name", consortium_name
    scenario_id = consortium_name

    # q = u"delete from jump_scenario_computed where scenario_id='{}'".format(scenario_id)
    # with get_db_cursor() as cursor:
    #     print q
    #     cursor.execute(q)

    settings = {"id": scenario_id,
                "configs": {"cost_bigdeal_increase": 5.0,
                            "include_submitted_version": True,
                            "include_social_networks": True,
                            "package": "658349d9",
                            "include_backfile": True,
                            "backfile_contribution": 100.0,
                            "ill_request_percent_of_delayed": 5.0,
                            "weight_authorship": 100.0,
                            "cost_content_fee_percent": 5.7,
                            "cost_ill": 17.0,
                            "cost_bigdeal": 10000000.0,
                            "include_bronze": True,
                            "weight_citation": 10.0,
                            "cost_alacart_increase": 8.0},
                "name": "{} Elsevier Consortium Scenario".format(scenario_id),
                "subrs": [],
                "customSubrs": []}
    my_dict = {}
    my_dict["meta"] =  {
        "scenario_id": scenario_id,
        "scenario_name": "{} Elsevier Consortium Scenario".format(scenario_id),
        "publisher_id": "consortium-{}".format(scenario_id),
        "publisher_name": "Elsevier",
        "institution_id": scenario_id,
        "institution_name": scenario_id,
        "scenario_created": datetime.datetime.utcnow().isoformat(),
        "is_base_scenario": True
        }
    my_dict["saved"] = settings
    my_dict["journals"] = []

    all_journal_dicts = []

    print "building from db"
    start_time = time()

    command_list = []
    for package_id in package_id_lists[scenario_id]:
        my_live_scenario = Scenario(package_id, settings, my_jwt=None)
        for my_journal in my_live_scenario.journals:
            all_journal_dicts.append(my_journal.to_dict_journals())
            usage = my_journal.use_total
            cpu = my_journal.ncppu
            if not cpu:
                cpu = "null"

            journals_dict_json = jsonify_fast_no_sort_simple(my_journal.to_dict_journals()).replace(u"'", u"''")
            command_list.append(u"('{}', '{}', sysdate, '{}', {}, {}, '{}')".format(
                package_id, scenario_id, my_journal.issn_l, usage, cpu, journals_dict_json))

    print(elapsed(start_time))


    print "now writing to db"
    start_time = time()
    command_start = u"INSERT INTO jump_scenario_computed (member_package_id, consortium_name, updated, issn_l, usage, cpu, journals_dict) values "
    with get_db_cursor() as cursor:
        for short_command_list in chunks(command_list, 1000):
            q = u"{} {};".format(command_start, u",".join(short_command_list))
            # print q, q
            cursor.execute(q)
            print ".",
    print(elapsed(start_time))

    print "done"



def refresh_data_for_consortium_scenario(scenario_id):
    print "scenario_id", scenario_id

    my_consortium = Consortium(scenario_id)
    my_consortium.recompute_journal_dicts()
    # consortium_get_computed_data(scenario_id)



# for consortium_name in ["colorado", "suny"]:  #crkn, purdue

if __name__ == "__main__":
    # for consortium_name in ["purdue", "colorado", "suny"]:  #crkn, purdue
    # for consortium_name in ["crkn"]:  #crkn, purdue
    #     refresh_data_for_consortium(consortium_name)

    import consortium

    consortium_ids = consortium.get_consortium_ids()
    # print consortium_ids

    for scenario_id in [d["scenario_id"] for d in consortium_ids if d["consortium_short_name"]=="crkn_test"]:

        consortium.consortium_get_computed_data(scenario_id)
        consortium.consortium_get_issns(scenario_id)

        # warm cache before we start the threads
        consortium.get_consortium_ids()
        consortium.big_deal_costs_for_members()
        # consortium.consortium_get_computed_data(scenario_id)
        consortium.consortium_get_issns(scenario_id)
        consortium.get_latest_member_institutions_raw(scenario_id)

        import scenario
        scenario.get_common_package_data_for_all()

        start_time = time()
        refresh_data_for_consortium_scenario(scenario_id)
        print u"done refresh_data_for_consortium_scenario for {} in {}s".format(scenario_id, elapsed(start_time))

# for consortium_short_name, member_package_ids in package_id_lists.iteritems():
#     for member_package_id in member_package_ids:
#         print "('{}', '{}'),".format(consortium_short_name, member_package_id.replace("-", "-cmp"))