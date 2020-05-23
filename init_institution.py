import argparse
import logging
from datetime import datetime

import shortuuid
import glob
from sqlalchemy.orm.exc import MultipleResultsFound
from werkzeug.security import generate_password_hash

from app import db
from app import logger
from grid_id import GridId
from institution import Institution
from package import Package
from permission import Permission, UserInstitutionPermission
from perpetual_access import PerpetualAccessInput
from ror_id import RorId, RorGridCrosswalk
from saved_scenario import SavedScenario
from user import User

from util import read_csv_file

# heroku local:run python init_institution.py

# configuration here

users = [
    {
        'email': u'jane@example.edu',  # required
        'password': u'',  # required
        'name': u'Jane',  # default is None
        'permissions': [u'view', u'modify', u'admin', ]  # default is view, modify, admin
    },
    {
        'email': u'mike2@example.edu',
        'password': u'',
        'name': u'Mike',
    }
]

institution_name = u'West Example State'

ror_ids = [u'049pfb863', ]

# files can be xls, xlsx, or csv
files = {
    'prices': None,             # u'/path/to/journal-prices.xlsx',
    'perpetual_access': None,   # u'/path/to/perpetual-access.xls',
    'counter': None,            # u'/path/to/counter.csv',
}

# configuration above


def add_institution(institution_name, old_username, ror_id):
    logger.info(u'initializing institution {}'.format(institution_name))

    my_institution = db.session.query(Institution).filter(Institution.display_name == institution_name).scalar()

    if my_institution:
        logger.info(u'  *** using existing institution {} ***'.format(my_institution))
    else:
        my_institution = Institution()
        my_institution.display_name = institution_name
        my_institution.old_username = old_username
        my_institution.is_demo_institution = False
        my_institution.is_consortium = False
        db.session.add(my_institution)
        logger.info(u'  adding {}'.format(my_institution))

    logger.info(u'adding ROR IDs')

    for er in db.session.query(RorId).filter(RorId.institution_id == my_institution.id).all():
        logger.info(u'  *** removing existing ROR ID {} for {} ***'.format(er.ror_id, my_institution.display_name))
        db.session.delete(er)

    # could later expand function ot take multiple ror_ids at once

    db.session.add(RorId(institution_id=my_institution.id, ror_id=ror_id))
    logger.info(u'  adding ROR ID {} for {}'.format(ror_id, my_institution.display_name))

    # add grid ids
    logger.info(u'adding GRID IDs')
    logger.info(u'  looking up GRID IDs')
    grid_ids = [x.grid_id for x in RorGridCrosswalk.query.filter(RorGridCrosswalk.ror_id == ror_id).all()]

    if not grid_ids:
        raise ValueError(u'at least one ror id corresponding to a grid id is required)')

    for eg in db.session.query(GridId).filter(GridId.institution_id == my_institution.id).all():
        logger.info(u'  *** removing existing GRID ID {} for {} ***'.format(eg.grid_id, my_institution.display_name))
        db.session.delete(eg)

    for g_id in grid_ids:
        db.session.add(GridId(institution_id=my_institution.id, grid_id=g_id))
        logger.info(u'  adding GRID ID {} for {}'.format(g_id, my_institution.display_name))

        # jump_citing
        logger.info(u'  populating jump_citing for GRID ID {}'.format(g_id))

        num_citing_rows = db.session.execute(
            "select count(*) from jump_citing where grid_id = '{}'".format(g_id)
        ).scalar()

        num_citing_rows_view = db.session.execute(
            "select count(*) from jump_citing_view where grid_id = '{}'".format(g_id)
        ).scalar()

        logger.info("num_citing_rows: {}, num_citing_rows_view {}".format(num_citing_rows, num_citing_rows_view))

        if num_citing_rows:
            logger.info(u'    {} jump_citing rows already exist for grid id {}'.format(num_citing_rows, g_id))
        else:
            num_citing_rows = db.session.execute(
                "insert into jump_citing (select * from jump_citing_view where grid_id = '{}')".format(g_id)
            ).rowcount
            logger.info(u'    created {} jump_citing rows for grid id {}'.format(num_citing_rows, g_id))

        # jump_authorship

        logger.info(u'  populating jump_authorship for GRID ID  {}'.format(g_id))

        num_authorship_rows = db.session.execute(
            "select count(*) from jump_authorship where grid_id = '{}'".format(g_id)
        ).scalar()

        num_authorship_rows_view = db.session.execute(
            "select count(*) from jump_authorship_view where grid_id = '{}'".format(g_id)
        ).scalar()

        logger.info("num_authorship_rows: {}, num_authorship_rows_view {}".format(num_authorship_rows, num_authorship_rows_view))

        if num_authorship_rows:
            logger.info(u'    {} jump_authorship rows already exist for grid id {}'.format(num_authorship_rows, g_id))
        else:
            num_authorship_rows = db.session.execute(
                "insert into jump_authorship (select * from jump_authorship_view where grid_id = '{}')".format(g_id)
            ).rowcount
            logger.info(u'    created {} jump_authorship rows for grid id {}'.format(num_authorship_rows, g_id))


def add_user(user_info):
    # create users and permissions

    logger.info(u'\ninitializing user {}'.format(user_info['email']))

    my_institution = db.session.query(Institution).filter(Institution.display_name == user_info['institution_name']).scalar()

    if my_institution:
        logger.info(u'  *** using existing institution {} ***'.format(my_institution))
    else:
        logger.info(u"  *** FAILED: institution {} doesn't exist, exiting ***".format(user_info['institution_name']))
        return

    my_user = db.session.query(User).filter(User.email == user_info['email']).scalar()

    if my_user:
        logger.info(u'  *** user {} already exists. updating display name but not modifying password. ***'.format(my_user))

    else:
        my_user = User()
        my_user.email = user_info['email']
        my_user.password_hash = generate_password_hash(user_info['password'])

    my_user.display_name = user_info.get('name', None)
    db.session.merge(my_user)
    logger.info(u'  saving {}'.format(my_user))

    logger.info(u"  updating permissions and linking to institution")

    permission_names = user_info.get('permissions', None)
    if not permission_names:
        permission_names = [u'view', u'modify', u'admin']

    existing_permissions = db.session.query(UserInstitutionPermission).filter(
        UserInstitutionPermission.user_id == my_user.id,
        UserInstitutionPermission.institution_id == my_institution.id
    ).all()

    for ep in existing_permissions:
        logger.info(u'  *** removing existing user permission {} ***'.format(ep))
        db.session.delete(ep)

    for permission_name in permission_names:
        perm = Permission.get(permission_name)
        if not perm:
            raise ValueError(u'unknown permission {}'.format(permission_name))
        user_perm = UserInstitutionPermission(
            user_id=my_user.id,
            institution_id=my_institution.id,
            permission_id=perm.id
        )
        db.session.add(user_perm)
        db.session.flush()
        logger.info(u'  adding {}'.format(user_perm))



def add_publisher(institution_username, counter_filename):

    # add a Publisher
    logger.info(u'adding a Publisher')

    now = datetime.utcnow().isoformat()

    try:
        my_institution = db.session.query(Institution).filter(Institution.old_username == institution_username).first()
        print u"my_institution: {}".format(my_institution)

        my_publisher = db.session.query(Package).filter(Package.institution_id == my_institution.id).scalar()
        if my_publisher:
            logger.info(u'  found an existing Publisher {}'.format(my_publisher))
        else:
            my_publisher = Package(
                package_id=u'publisher-{}'.format(shortuuid.uuid()[0:12]),
                publisher=u'Elsevier',
                package_name=u'Elsevier',
                created=now,
                institution_id=my_institution.id,
                is_demo=False
            )
            db.session.add(my_publisher)
            db.session.flush()
            logger.info(u'  adding {}'.format(my_publisher))
    except MultipleResultsFound as e:
        raise MultipleResultsFound(u'more than one publisher already exists for {}')

    # add Scenario

    logger.info(u'adding a Scenario for Publisher {}'.format(my_publisher))
    my_scenarios = db.session.query(SavedScenario).filter(SavedScenario.package_id == my_publisher.package_id).all()

    if not my_scenarios:
        my_scenario = SavedScenario(False, u'scenario-{}'.format(shortuuid.uuid()[0:12]), None)
        my_scenario.package_id = my_publisher.package_id
        my_scenario.scenario_name = u'First Scenario'
        my_scenario.created = now
        my_scenario.is_base_scenario = True

        db.session.add(my_scenario)
        logger.info(u'  adding {}'.format(my_scenario))
    else:
        for scenario in my_scenarios:
            logger.info(u'  found an existing Scenario {}'.format(scenario))

    # jump_apc_authorships
    logger.info(u'populating jump_apc_authorships for Publisher {}'.format(my_publisher))

    num_apc_authorship_rows = db.session.execute(
        "select count(*) from jump_apc_authorships where package_id = '{}'".format(my_publisher.package_id)
    ).scalar()

    if num_apc_authorship_rows:
        logger.info(u'  {} jump_apc_authorships rows already exist for Publisher {}'.format(
            num_apc_authorship_rows, my_publisher
        ))
    else:
        num_apc_authorship_rows = db.session.execute(
            '''
                insert into jump_apc_authorships (
                    select * from jump_apc_authorships_view
                    where package_id = '{}'
                )
            '''.format(my_publisher.package_id)
        ).rowcount

        logger.info(u'  created {} jump_apc_authorships rows for Publisher {}'.format(
            num_apc_authorship_rows, my_publisher
        ))

    log_level = logging.getLogger('').level

    logger.info(u'loading counter {} for publisher {}'.format(
        counter_filename, my_publisher.package_id)
    )
    logging.getLogger('').setLevel(logging.WARNING)
    from counter import CounterInput
    load_result = CounterInput.load(my_publisher.package_id, counter_filename, commit=False)
    logging.getLogger('').setLevel(log_level)
    if load_result['success']:
        logger.info(load_result['message'])
    else:
        raise RuntimeError(load_result['message'])


# def add_prices():
#     logger.info(u'loading journal price list {} for publisher {}'.format(files['prices'], my_publisher.package_id))
#     logging.getLogger('').setLevel(logging.WARNING)
#     from journal_price import JournalPriceInput
#     success, message = JournalPriceInput.load(my_publisher.package_id, files['prices'], commit=False)
#     logging.getLogger('').setLevel(log_level)
#     if success:
#         logger.info(message)
#     else:
#         raise RuntimeError(message)
#
# def add_backfile():
#     logger.info(u'loading perpetual access list {} for publisher {}'.format(
#         files['perpetual_access'], my_publisher.package_id)
#     )
#     logging.getLogger('').setLevel(logging.WARNING)
#     success, message = PerpetualAccessInput.load(my_publisher.package_id, files['perpetual_access'], commit=False)
#     logging.getLogger('').setLevel(log_level)
#     if success:
#         logger.info(message)
#     else:
#         raise RuntimeError(message)

# python init_institution.py --institutions --file ~/Downloads/temp.csv
# python init_institution.py --username emory --counter --file ~/Dropbox/companywide/unpaywall_journals_counter/counter/emory-elsevier-2018.xlsx


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--commit', help='Commit changes.', action='store_true', default=False)
    parser.add_argument('--institutions', help='true if want to add institutions', action="store_true", default=False)
    parser.add_argument('--counter', help='true if want to add counter', action="store_true", default=False)
    parser.add_argument('--users', help='true if want to add users', action="store_true", default=False)
    parser.add_argument('--file', help='Filename of CSV', type=str, default=None)
    parser.add_argument('--username', help='username of institution', type=str, default=None)
    commit = parser.parse_args().commit

    if parser.parse_args().institutions:
        if parser.parse_args().file:
            institution_rows = read_csv_file(parser.parse_args().file)

        for row in institution_rows:
            my_institution = add_institution(row["institution_name"], row["username"], row["ror_id"])

            if commit:
                logger.info('commit')
                db.session.commit()
            else:
                logger.info('rollback, run with --commit to commit')
                db.session.rollback()

    elif parser.parse_args().users:
        if parser.parse_args().file:
            user_rows = read_csv_file(parser.parse_args().file)

        for row in user_rows:
            my_institution = add_user(row)

            if commit:
                logger.info('commit')
                db.session.commit()
            else:
                logger.info('rollback, run with --commit to commit')
                db.session.rollback()

    elif parser.parse_args().counter:
        add_publisher(parser.parse_args().username, parser.parse_args().file)

    #
    # usernames = """
    # orgusername
    # """.split()
    #
    #
    # for username in usernames:
    #     print u"username: {}".format(username)
    #     filenames = glob.glob("/Users/hpiwowar/Dropbox/companywide/unpaywall_journals_counter/counter/{}-elsevier-2018.txt_utf8".format(username))
    #     print filenames[0]
    #     add_publisher(username, filenames[0])
    #
    # if commit:
    #     logger.info('commit')
    #     db.session.commit()
    # else:
    #     logger.info('rollback, run with --commit to commit')
    #     db.session.rollback()

    # elif parser.parse_args().prices:
    #     add_prices(parser.parse_args().file)
    #
    # elif parser.parse_args().backfile:
    #     add_backfile(parser.parse_args().file)

    if commit:
        logger.info('commit')
        db.session.commit()
    else:
        logger.info('rollback, run with --commit to commit')
        db.session.rollback()
