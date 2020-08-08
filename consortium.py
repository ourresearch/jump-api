# coding: utf-8

from cached_property import cached_property
from collections import defaultdict
from collections import OrderedDict
import datetime
from multiprocessing.pool import ThreadPool
import threading
import weakref
from time import time
import simplejson as json
from kids.cache import cache
from simplejson import dumps

from app import app
from app import get_db_cursor
from app import reset_cache
from consortium_journal import ConsortiumJournal
from util import elapsed
from util import chunks
from util import uniquify_list
from util import myconverter

# team+dev@ourresearch.org

from app import memorycache

# NO CACHE FOR NOW @memorycache
def get_latest_member_institutions_raw(scenario_id):
    scenario_members = []
    with get_db_cursor() as cursor:
        command = u"""select scenario_members from jump_consortium_member_institutions where scenario_id='{}' order by updated desc limit 1;""".format(
            scenario_id
        )
        # print command
        cursor.execute(command)
        rows = cursor.fetchall()

    if rows:
        try:
            scenario_members = json.loads(rows[0]["scenario_members"])
        except TypeError:
            scenario_members = []

    return scenario_members

# NO CACHE FOR NOW @memorycache
# too slow to get refreshed across dynos
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


@memorycache
def consortium_get_computed_data(scenario_id):
    start_time = time()

    command = """select member_package_id, scenario_id, issn_l, journals_dict from jump_scenario_computed where scenario_id='{}'""".format(scenario_id)
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()

    print "after db get consortium_get_computed_data", elapsed(start_time)

    start_time = time()
    for row in rows:
        row["journals_dict"] = json.loads(row["journals_dict"])
    print "after json loads in consortium_get_computed_data", elapsed(start_time)
    return rows


@memorycache
def consortium_get_issns(scenario_id):
    start_time = time()

    command = """select distinct issn_l from jump_scenario_computed where scenario_id='{}'""".format(scenario_id)
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()

    print "after db get consortium_get_issns", elapsed(start_time)
    return [row["issn_l"] for row in rows]


@memorycache
def big_deal_costs_for_members():
    start_time = time()

    command = """select package_id, big_deal_cost from jump_account_package 
        join jump_institution on jump_account_package.institution_id = jump_institution.id
        where big_deal_cost is not null
        """
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()

    return rows


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
        response = consortium_get_computed_data(self.scenario_id)
        return response

    @cached_property
    def member_institution_included_list(self):
        start_time = time()
        member_institutions_status = get_latest_member_institutions_raw(self.scenario_id)
        if member_institutions_status:
            return member_institutions_status
        return self.all_member_package_ids

    @cached_property
    def big_deal_cost_for_included_members(self):
        rows = big_deal_costs_for_members()
        big_deal_cost_for_included_members = [row["big_deal_cost"] for row in rows if row["package_id"] in self.member_institution_included_list]
        my_sum = sum(big_deal_cost_for_included_members)
        return my_sum

    @cached_property
    def scenario_saved_dict(self):
        from saved_scenario import get_latest_scenario_raw
        response = get_latest_scenario_raw(self.scenario_id)
        response["configs"]["cost_bigdeal"] = self.big_deal_cost_for_included_members
        return response

    def to_dict_journals(self):
        my_response = OrderedDict()
        my_response["meta"] = {'publisher_name': u'Elsevier',
                                          'institution_name': u'CRKN',
                                          'scenario_id': self.scenario_id,
                                          'institution_id': self.institution_id,
                                          'scenario_created': datetime.datetime(2020, 7, 18, 17, 12, 40, 335615),
                                          'is_base_scenario': True,
                                          'scenario_name': self.scenario_saved_dict["name"],
                                          'publisher_id': self.package_id}
        my_response["saved"] = self.scenario_saved_dict

        start_time = time()
        response_list = [j.to_dict_journals() for j in self.journals]
        print "after to_dict_journals on each journal", elapsed(start_time)

        response_list = sorted(response_list, key=lambda x: x.get("ncppu", None), reverse=False)
        for rank, my_journal_dict in enumerate(response_list):
            my_journal_dict["ncppu_rank"] = rank + 1
        my_response["journals"] = response_list
        my_response["member_institutions"] = self.member_institution_included_list
        my_response["is_locked_pending_update"] = True
        my_response["update_percent_complete"] = 100

        return my_response

    def copy_computed_journal_dicts(self, new_scenario_id):
        q = """
                insert into jump_scenario_computed (scenario_id, member_package_id, updated, issn_l, journals_dict, usage, cpu, consortium_name) 
                (
                    select '{}', member_package_id, updated, issn_l, journals_dict, usage, cpu, consortium_name
                    from jump_scenario_computed
                    where scenario_id = '{}'
                )
            """.format(new_scenario_id, self.scenario_id)
        with get_db_cursor() as cursor:
            print q
            cursor.execute(q)


    @cached_property
    def all_member_package_ids(self):
        from save_groups import package_id_lists

        return package_id_lists[self.consortium_name]


    @cached_property
    def member_package_ids(self):
        return uniquify_list([d["member_package_id"] for d in self.journal_member_data])


    def recompute_journal_dicts(self):

        # delete everything with this scenario_id first
        q = u"delete from jump_scenario_computed where scenario_id='{}'".format(self.scenario_id)
        with get_db_cursor() as cursor:
            print q
            cursor.execute(q)

        from scenario import Scenario

        if not hasattr(threading.current_thread(), "_children"):
            threading.current_thread()._children = weakref.WeakKeyDictionary()
        my_thread_pool = ThreadPool(1)

        print "starting threads"

        def get_insert_rows_for_member(member_package_id):
            command_list = []
            print "in get_insert_rows_for_member with", member_package_id
            try:
                with app.app_context():
                    print "len(app.my_memorycache_dict)", len(app.my_memorycache_dict)

                    my_live_scenario = Scenario(member_package_id, self.scenario_saved_dict, my_jwt=None)
                    print u"after my_live_scenario with {}".format(member_package_id)
                    for my_journal in my_live_scenario.journals:
                        usage = my_journal.use_total
                        cpu = my_journal.ncppu or "null"
                        journals_dict_json = jsonify_fast_no_sort_simple(my_journal.to_dict_journals()).replace(u"'", u"''")
                        command_list.append(u"('{}', '{}', '{}', sysdate, '{}', {}, {}, '{}')".format(
                            member_package_id, self.scenario_id, self.consortium_name, my_journal.issn_l, usage, cpu, journals_dict_json))

                    # save all of these in the db
                    print "now writing to db", member_package_id
                    start_time = time()
                    command_start = u"INSERT INTO jump_scenario_computed (member_package_id, scenario_id, consortium_name, updated, issn_l, usage, cpu, journals_dict) values "
                    with get_db_cursor() as cursor:
                        for short_command_list in chunks(command_list, 1000):
                            q = u"{} {};".format(command_start, u",".join(short_command_list))
                            cursor.execute(q)
                            print ".",
                    print(elapsed(start_time))
                    print "done writing to db", member_package_id

            except Exception as e:
                print "whoops exception", e
                raise
            return command_list

        results = my_thread_pool.imap_unordered(get_insert_rows_for_member, self.all_member_package_ids)
        my_thread_pool.close()
        my_thread_pool.join()
        my_thread_pool.terminate()
        print "done with threads"

        # clear cache
        print "clearing cache"
        reset_cache("consortium", "consortium_get_computed_data", self.scenario_id)
        print "cache clear set"


    @cached_property
    def journals(self):
        response = None
        rows = self.journal_member_data
        start_time = time()
        print "creating consortium journals"

        issn_ls = consortium_get_issns(self.scenario_id)
        print "before journals", elapsed(start_time)
        start_time = time()
        journals_dicts_by_issn_l = defaultdict(list)
        for d in rows:
            if d["member_package_id"] in self.member_institution_included_list:
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
            s.member_package_id as package_id, 
            sum(s.usage) as usage,
            count(s.member_package_id) as num_journals
            from jump_scenario_computed s
            join jump_account_package p on s.member_package_id = p.package_id
            join jump_institution i on i.id = p.institution_id
            where s.scenario_id='{scenario_id}' 
            group by s.member_package_id
            order by usage desc
             """.format(scenario_id=self.scenario_id)
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

