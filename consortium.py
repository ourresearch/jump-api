# coding: utf-8

from cached_property import cached_property
from collections import defaultdict
from collections import OrderedDict
import datetime
from time import time
import simplejson as json

from app import get_db_cursor
# from app import disk_cache
from consortium_journal import ConsortiumJournal
from util import elapsed

# team+dev@ourresearch.org

from kids.cache import cache


# @disk_cache.memoize()
@cache
def get_consortium_ids():
    q = """select institution_id, old_username as consortium_short_name, p.package_id, s.scenario_id
                from jump_package_scenario s
                join jump_account_package p on p.package_id = s.package_id
                join jump_institution i on i.id = p.institution_id
                where is_consortium = true"""
    with get_db_cursor() as cursor:
        cursor.execute(q)
        rows = cursor.fetchall()
    return rows


# @disk_cache.memoize()
@cache
def consortium_get_computed_data(consortium_name):
    start_time = time()

    command = """select package_id, scenario_id, issn_l, journals_dict from jump_scenario_computed where scenario_id='{}'""".format(consortium_name)
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()

    print "after db get consortium_get_computed_data", elapsed(start_time)

    start_time = time()
    for row in rows:
        row["journals_dict"] = json.loads(row["journals_dict"])
    print "after json loads in consortium_get_computed_data", elapsed(start_time)
    return rows


# @disk_cache.memoize()
@cache
def consortium_get_issns(consortium_name):
    start_time = time()

    command = """select distinct issn_l from jump_scenario_computed where scenario_id='{}'""".format(consortium_name)
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()

    print "after db get consortium_get_issns", elapsed(start_time)
    return [row["issn_l"] for row in rows]


class Consortium(object):
    def __init__(self, scenario_id, package_id=None):
        self.scenario_id = None
        consortium_ids = get_consortium_ids()
        if scenario_id:
            my_row = [d for d in consortium_ids if d["scenario_id"]==scenario_id][0]
            self.scenario_id = scenario_id
        elif package_id:
            my_row = [d for d in consortium_ids if d["package_id"]==package_id][0]

        self.consortium_name = my_row["consortium_short_name"]
        self.package_id = my_row["package_id"]
        self.institution_id = my_row["institution_id"]

    @cached_property
    def journal_member_data(self):
        response = consortium_get_computed_data(self.consortium_name)
        return response

    @cached_property
    def member_institution_included_list(self):
        from saved_scenario import get_latest_member_institutions_raw
        start_time = time()
        member_institutions_status = get_latest_member_institutions_raw(self.scenario_id)
        print "\n\n\n query get member_institutions_status", elapsed(start_time)
        return member_institutions_status

    def to_dict_journals(self):
        from saved_scenario import get_latest_scenario_raw
        scenario_saved_dict = get_latest_scenario_raw(self.scenario_id)

        my_saved_scenario_dict = OrderedDict()
        my_saved_scenario_dict["meta"] = {'publisher_name': u'Elsevier',
                                          'institution_name': u'CRKN',
                                          'scenario_id': self.scenario_id,
                                          'institution_id': self.institution_id,
                                          'scenario_created': datetime.datetime(2020, 7, 18, 17, 12, 40, 335615),
                                          'is_base_scenario': True,
                                          'scenario_name': scenario_saved_dict["scenario_name"],
                                          'publisher_id': self.package_id}

        my_saved_scenario_dict["saved"] = scenario_saved_dict

        start_time = time()
        response_list = [j.to_dict_journals() for j in self.journals]
        print "after to_dict_journals on each journal", elapsed(start_time)

        response_list = sorted(response_list, key=lambda x: x.get("ncppu", None), reverse=False)
        for rank, my_journal_dict in enumerate(response_list):
            my_journal_dict["ncppu_rank"] = rank + 1
        my_saved_scenario_dict["journals"] = response_list

        return my_saved_scenario_dict

    @cached_property
    def package_ids(self):
        return [d["package_id"] for d in self.journal_member_data]


    @cached_property
    def journals(self):
        response = None
        rows = self.journal_member_data
        start_time = time()
        print "creating consortium journals"

        issn_ls = consortium_get_issns(self.consortium_name)
        print "before journals", elapsed(start_time)
        start_time = time()
        journals_dicts_by_issn_l = defaultdict(list)
        for d in rows:
            if d["package_id"] in self.member_institution_included_list:
                journals_dicts_by_issn_l[d["issn_l"]].append(d["journals_dict"])

        print "after calculating", elapsed(start_time)
        start_time = time()
        response = []
        for issn_l in issn_ls:
            if len(journals_dicts_by_issn_l[issn_l]) > 0:
                response.append(ConsortiumJournal(issn_l, self.member_institution_included_list, journals_dicts_by_issn_l[issn_l]))

        print "after journals", elapsed(start_time)
        return response


    def to_dict_institutions(self):
        start_time = time()

        command = """select max(i.id) as institution_id, 
            max(i.old_username) as institution_short_name, 
            max(i.display_name) as institution_name, 
            s.package_id, 
            sum(s.usage) as usage,
            count(s.package_id) as num_journals
            from jump_scenario_computed s
            join jump_account_package p on s.package_id = p.package_id
            join jump_institution i on i.id = p.institution_id
            where s.scenario_id='{consortium_name}' 
            group by s.package_id
            order by usage desc
             """.format(consortium_name=self.consortium_name)
        with get_db_cursor() as cursor:
            cursor.execute(command)
            rows = cursor.fetchall()

        if self.scenario_id is not None:
            for row in rows:
                row["included"] = False
                if row["package_id"] in self.member_institution_included_list:
                    row["included"] = True

        print "after db get to_dict_institutions", elapsed(start_time)
        return rows


    def __repr__(self):
        return u"<{} ({})>".format(self.__class__.__name__, self.package_id)



# @cached_property
# def member_institutions(self):
#     command = """select i.id as institution_id,
#         i.old_username as institution_short_name,
#         i.display_name as institution_name,
#         s.package_id
#         from jump_scenario_computed s
#         join jump_account_package p on s.package_id = p.package_id
#         join jump_institution i on i.id = p.institution_id
#         where package_id in
#         (select package_id from jump_scenario_computed where
#          scenario_id='{}')""".format(self.consortium_name)
#     with get_db_cursor() as cursor:
#         cursor.execute(command)
#         rows = cursor.fetchall()
#     response = {row["package_id"]: row for row in rows}
#     return response


# to thread
# if not hasattr(threading.current_thread(), "_children"):
#     threading.current_thread()._children = weakref.WeakKeyDictionary()
#
# my_thread_pool = ThreadPool(25)
#
# def dosomething(member_id_dict):
#     return 42
#
# self.consortium_member_responses = my_thread_pool.imap_unordered(call_cached_version, self.member_ids)
# my_thread_pool.close()
# my_thread_pool.join()
# my_thread_pool.terminate()