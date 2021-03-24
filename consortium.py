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
from simplejson import dumps

from app import app
from app import get_db_cursor
from app import reset_cache
from consortium_journal import ConsortiumJournal
from util import elapsed
from util import chunks
from util import uniquify_list
from util import myconverter
from util import for_sorting


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
    q = """select institution_id, i.display_name as consortium_name, i.old_username as consortium_short_name, 
            p.package_id, p.publisher, s.scenario_id
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

    from app import cached_consortium_scenario_ids
    # if scenario_id in cached_consortium_scenario_ids:
    if False:
        import boto3
        s3_client = boto3.client("s3")
        print u"made s3_client in consortium_get_computed_data"

        filename = u"consortium_get_computed_data_{}.json".format(scenario_id)
        s3_clientobj = s3_client.get_object(Bucket="unsub-cache", Key=filename)
        print u"made s3_clientobj"
        contents_string = s3_clientobj["Body"].read().decode("utf-8")
        rows = json.loads(contents_string)
        print "after json loads in consortium_get_computed_data using s3 cache", elapsed(start_time)

    else:
        # command = """select member_package_id, scenario_id, issn_l, usage, cpu, subscription_cost, ill_cost, use_social_networks, use_oa, use_backfile, use_subscription, use_other_delayed, use_ill, perpetual_access_years, use_social_networks_percent, use_green_percent, use_hybrid_percent, use_bronze_percent, use_peer_reviewed_percent, bronze_oa_embargo_months, is_hybrid_2019, downloads, citations, authorships
        #                 from jump_scenario_computed where scenario_id='{}'""".format(scenario_id)
        # with get_db_cursor() as cursor:
        #     cursor.execute(command)
        #     rows = cursor.fetchall()
        # print "TEST ALL COLUMNS after db get consortium_get_computed_data not using s3 cache", elapsed(start_time)

        start_time = time()
        command = """select member_package_id
                        from jump_scenario_computed where scenario_id='{}'""".format(scenario_id)
        with get_db_cursor() as cursor:
            cursor.execute(command)
            rows = cursor.fetchall()
        print "TEST just one COLUMNS after db get consortium_get_computed_data not using s3 cache", elapsed(start_time)

        start_time = time()
        command = """select member_package_id, scenario_id, issn_l, journals_dict from jump_scenario_computed where scenario_id='{}'""".format(scenario_id)
        with get_db_cursor() as cursor:
            cursor.execute(command)
            rows = cursor.fetchall()
        print "REAL after db get consortium_get_computed_data not using s3 cache", elapsed(start_time)

    start_time = time()
    for row in rows:
        row["journals_dict"] = row["journals_dict"].split(",")
        # row["journals_dict"] = json.loads(row["journals_dict"])
    print "after json loads in consortium_get_computed_data ", elapsed(start_time)
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
        print "BUILDING SCENARIO OBJECT", scenario_id, package_id
        self.scenario_id = None
        consortium_ids = get_consortium_ids()
        if scenario_id:
            my_row = [d for d in consortium_ids if d["scenario_id"]==scenario_id][0]
            self.scenario_id = scenario_id
        elif package_id:
            my_row = [d for d in consortium_ids if d["package_id"]==package_id][0]

        self.consortium_name = my_row["consortium_name"]
        self.consortium_short_name = my_row["consortium_short_name"]
        self.package_id = my_row["package_id"]
        self.publisher = my_row["publisher"]
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

    @cached_property
    def is_locked_pending_update(self):
        if self.update_notification_email is not None:
            return True
        return False

    @cached_property
    def update_notification_email(self):
        command = "select email from jump_scenario_computed_update_queue where completed is null and scenario_id='{}'".format(self.scenario_id)
        # print command
        with get_db_cursor() as cursor:
            cursor.execute(command)
            rows = cursor.fetchall()
        if rows:
            return rows[0]["email"]
        return None

    @cached_property
    def update_percent_complete(self):
        if self.is_locked_pending_update:
            command = "select count(distinct member_package_id) as num_members_done from jump_scenario_computed where scenario_id='{}'".format(self.scenario_id)
            # print command
            with get_db_cursor() as cursor:
                cursor.execute(command)
                rows = cursor.fetchall()
            if rows:
                num_members_done = rows[0]["num_members_done"]
                return 100 * float(num_members_done)/len(self.all_member_package_ids)
        return None

    @cached_property
    def journals_sorted_cpu(self):
        my_journals = []
        try:
            my_journals = self.journals
            my_journals.sort(key=lambda k: for_sorting(k.cpu), reverse=False)
        except KeyError as e:
            print u"error", e
        return my_journals

    @cached_property
    def journals_sorted_use_total(self):
        self.journals.sort(key=lambda k: for_sorting(k.use_total), reverse=True)
        return self.journals

    # @cached_property
    # def apc_journals_sorted_spend(self):
    #     self.apc_journals.sort(key=lambda k: for_sorting(k.cost_apc_historical), reverse=True)
    #     return self.apc_journals

    def to_dict_journals(self):
        my_response = OrderedDict()
        my_response["meta"] = {'publisher_name': self.publisher,
                                          'institution_name': self.consortium_name,
                                          'scenario_id': self.scenario_id,
                                          'institution_id': self.institution_id,
                                          'scenario_created': datetime.datetime(2020, 7, 18, 17, 12, 40, 335615),
                                          'is_base_scenario': True,
                                          'scenario_name': self.scenario_saved_dict.get("name", "My Scenario"),
                                          'publisher_id': self.package_id}
        my_response["saved"] = self.scenario_saved_dict

        start_time = time()
        response_list = [j.to_dict_journals() for j in self.journals_sorted_cpu]
        print "after to_dict_journals on each journal", elapsed(start_time)

        my_response["journals"] = response_list
        my_response["member_institutions"] = self.member_institution_included_list
        my_response["is_locked_pending_update"] = self.is_locked_pending_update
        my_response["update_notification_email"] = self.update_notification_email
        my_response["update_percent_complete"] = self.update_percent_complete

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
        q = u"select member_package_id from jump_consortium_members where consortium_package_id='{}'".format(self.package_id)
        with get_db_cursor() as cursor:
            cursor.execute(q)
            rows = cursor.fetchall()
        if rows:
            return [row["member_package_id"] for row in rows]
        return []


    @cached_property
    def included_member_package_ids(self):
        return uniquify_list([d["member_package_id"] for d in self.journal_member_data])

    def queue_for_recompute(self, email):
        num_member_institutions = len(self.all_member_package_ids)
        command = u"""insert into jump_scenario_computed_update_queue (
            consortium_name, consortium_short_name, package_name, institution_id, package_id, scenario_id, email, num_member_institutions, created, completed) 
            values ('{}', '{}', '{}', '{}', '{}', '{}', '{}', {}, sysdate, null)""".format(
            self.consortium_name, self.consortium_short_name, self.publisher, self.institution_id, self.package_id, self.scenario_id, email, num_member_institutions)
        print "command queue_for_recompute\n", command
        with get_db_cursor() as cursor:
            cursor.execute(command)


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
            print "in get_insert_rows_for_member with", member_package_id, self.scenario_id
            try:
                with app.app_context():
                    print "len(app.my_memorycache_dict)", len(app.my_memorycache_dict)

                    my_live_scenario = Scenario(member_package_id, self.scenario_saved_dict, my_jwt=None)
                    print u"after my_live_scenario with {} {}".format(member_package_id, self.scenario_id)
                    for my_journal in my_live_scenario.journals:
                        usage = my_journal.use_total
                        cpu = my_journal.cpu or "null"
                        journals_dict_json = jsonify_fast_no_sort_simple(my_journal.to_dict_journals_for_consortium()).replace(u"'", u"''")
                        command_list.append(u"('{}', '{}', '{}', sysdate, '{}', {}, {}, '{}')".format(
                            member_package_id, self.scenario_id, self.consortium_short_name, my_journal.issn_l, usage, cpu, journals_dict_json))

                    # save all of these in the db
                    print "now writing to db", member_package_id, self.scenario_id
                    start_time = time()
                    command_start = u"INSERT INTO jump_scenario_computed (member_package_id, scenario_id, consortium_name, updated, issn_l, usage, cpu, journals_dict) values "
                    with get_db_cursor() as cursor:
                        for short_command_list in chunks(command_list, 1000):
                            q = u"{} {};".format(command_start, u",".join(short_command_list))
                            cursor.execute(q)
                            print ".",
                    print(elapsed(start_time))
                    print "done writing to db", member_package_id, self.scenario_id

            except Exception as e:
                print u"In get_insert_rows_for_member with Error: ", e
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

    def to_dict_journal_zoom(self, issn_l):
        start_time = time()

        command = """select i.id as institution_id, 
            i.display_name as institution_name, 
            s.member_package_id as package_id, 
            s.usage as usage,
            s.cpu as cpu
            from jump_scenario_computed s
            join jump_account_package p on s.member_package_id = p.package_id
            join jump_institution i on i.id = p.institution_id
            where s.scenario_id='{scenario_id}' 
            and s.issn_l = '{issn_l}'
            order by usage desc
             """.format(scenario_id=self.scenario_id, issn_l=issn_l)
        with get_db_cursor() as cursor:
            cursor.execute(command)
            rows = cursor.fetchall()

        response = []
        if self.scenario_id is not None:
            for row in rows:
                if row["package_id"] in self.member_institution_included_list:
                    response.append(row)

        print "after db get to_dict_journal_zoom", elapsed(start_time)
        return response

    def to_dict_export(self):
        response = {}
        response["journals"] = [j.to_dict_export() for j in self.journals_sorted_cpu]
        return response

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
                # journals_dicts_by_issn_l[d["issn_l"]].append(d)

        print "after calculating", elapsed(start_time)
        start_time = time()
        journal_list = []
        for issn_l in issn_ls:
            if len(journals_dicts_by_issn_l[issn_l]) > 0:
                journal_list.append(ConsortiumJournal(issn_l, self.member_institution_included_list, journals_dicts_by_issn_l[issn_l]))

        for my_journal in journal_list:
            if my_journal.issn_l in self.scenario_saved_dict.get("subrs", []):
                my_journal.set_subscribe_bulk()
            if my_journal.issn_l in self.scenario_saved_dict.get("customSubrs", []):
                my_journal.set_subscribe_custom()

        try:
            journal_list = sorted(journal_list, key=lambda x: float('inf') if x.cpu==None else x.cpu, reverse=False)
        except KeyError as e:
            # happens when I change keys, before reset in consortium
            print u"KeyError in journal_list", e
            pass

        for rank, my_journal in enumerate(journal_list):
            my_journal.cpu_rank = rank + 1

        print "after journals", elapsed(start_time)

        return journal_list


    def to_dict_institutions(self):
        start_time = time()

        command = """with tags as (select institution_id, listagg(tag_string, ', ') as tag_listagg from jump_tag_institution group by institution_id)
            select max(i.id) as institution_id, 
            max(i.old_username) as institution_short_name, 
            max(i.display_name) as institution_name, 
            s.member_package_id as package_id, 
            sum(s.usage) as usage,
            count(s.member_package_id) as num_journals,
            max(t.tag_listagg) as tags,
            false as included
            from jump_scenario_computed s
            join jump_account_package p on s.member_package_id = p.package_id
            join jump_institution i on i.id = p.institution_id
            left join tags t on t.institution_id=p.institution_id
            where s.scenario_id='{scenario_id}' 
            group by s.member_package_id
            order by usage desc
             """.format(scenario_id=self.scenario_id)
        with get_db_cursor(use_realdictcursor=True) as cursor:
            cursor.execute(command)
            rows = cursor.fetchall()

        if self.scenario_id is not None:
            for row in rows:
                if row["package_id"] in self.member_institution_included_list:
                    row["included"] = True

        print "after db get to_dict_institutions", elapsed(start_time)
        return rows


    def __repr__(self):
        return u"<{} ({})>".format(self.__class__.__name__, self.package_id)

