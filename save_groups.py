# coding: utf-8

from time import time
from simplejson import dumps
import gzip
import io
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




def refresh_data_for_consortium_scenario(scenario_id):
    print("scenario_id", scenario_id)

    my_consortium = Consortium(scenario_id)
    my_consortium.recompute_journal_dicts()
    # consortium_get_computed_data(scenario_id)



# for consortium_name in ["colorado", "suny"]:  #crkn, purdue

if __name__ == "__main__":

    if True:
        import consortium

        consortium_ids = consortium.get_consortium_ids()
        # print consortium_ids

        for scenario_id in [d["scenario_id"] for d in consortium_ids if d["consortium_short_name"]=="julac"]:
            # consortium.consortium_get_computed_data(scenario_id)
            # consortium.consortium_get_issns(scenario_id)

            # consortium.get_consortium_ids()
            # consortium.big_deal_costs_for_members()
            # # consortium.consortium_get_computed_data(scenario_id)
            # consortium.consortium_get_issns(scenario_id)
            # consortium.get_latest_member_institutions_raw(scenario_id)
            #
            # import scenario
            # scenario.get_common_package_data_for_all()

            start_time = time()
            refresh_data_for_consortium_scenario(scenario_id)
            print("done refresh_data_for_consortium_scenario for {} in {}s".format(scenario_id, elapsed(start_time)))

