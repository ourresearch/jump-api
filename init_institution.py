import argparse
import logging
from datetime import datetime

import shortuuid
from sqlalchemy.orm.exc import MultipleResultsFound
from werkzeug.security import generate_password_hash

from app import db
from app import logger
from counter import CounterInput
from grid_id import GridId
from institution import Institution
from journal_price import JournalPriceInput
from package import Package
from permission import Permission, UserInstitutionPermission
from perpetual_access import PerpetualAccessInput
from ror_id import RorId, RorGridCrosswalk
from saved_scenario import SavedScenario
from user import User

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--commit', help='Commit changes.', action='store_true', default=False)
    commit = parser.parse_args().commit

    # create institution

    logger.info(u'initializing institution {}'.format(institution_name))

    my_institution = db.session.query(Institution).filter(Institution.display_name == institution_name).scalar()

    if my_institution:
        logger.info(u'  *** modifying existing institution {} ***'.format(my_institution))
    else:
        my_institution = Institution()
        my_institution.display_name = institution_name
        my_institution.is_demo_institution = False
        db.session.add(my_institution)
        logger.info(u'  adding {}'.format(my_institution))

    # create users and permissions

    for user_info in users:
        logger.info(u'initializing user {}'.format(user_info['email']))

        my_user = db.session.query(User).filter(User.email == user_info['email']).scalar()

        if my_user:
            logger.info(u'  *** modifying existing user {} ***'.format(my_user))
        else:
            my_user = User()
            my_user.email = user_info['email']

        my_user.password_hash = generate_password_hash(user_info['password'])
        my_user.display_name = user_info.get('name', None)
        db.session.merge(my_user)
        logger.info(u'  saving {}'.format(my_user))

        permission_names = user_info.get('permissions', [u'view', u'modify', u'admin', ])

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

    # add ror ids
    logger.info(u'adding ROR IDs')

    for er in db.session.query(RorId).filter(RorId.institution_id == my_institution.id).all():
        logger.info(u'  *** removing existing ROR ID {} for {} ***'.format(er.ror_id, my_institution.display_name))
        db.session.delete(er)

    for r_id in ror_ids:
        db.session.add(RorId(institution_id=my_institution.id, ror_id=r_id))
        logger.info(u'  adding ROR ID {} for {}'.format(r_id, my_institution.display_name))

    # add grid ids
    logger.info(u'adding GRID IDs')
    logger.info(u'  looking up GRID IDs')
    grid_ids = [x.grid_id for x in RorGridCrosswalk.query.filter(RorGridCrosswalk.ror_id.in_(ror_ids)).all()]

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

        if num_authorship_rows:
            logger.info(u'    {} jump_authorship rows already exist for grid id {}'.format(num_authorship_rows, g_id))
        else:
            num_authorship_rows = db.session.execute(
                "insert into jump_authorship (select * from jump_authorship_view where grid_id = '{}')".format(g_id)
            ).rowcount
            logger.info(u'    created {} jump_authorship rows for grid id {}'.format(num_authorship_rows, g_id))

    # add a Publisher
    logger.info(u'adding a Publisher')

    now = datetime.utcnow().isoformat()

    try:
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

    if files.get('prices', None):
        logger.info(u'loading journal price list {} for publisher {}'.format(files['prices'], my_publisher.package_id))
        logging.getLogger('').setLevel(logging.WARNING)
        success, message = JournalPriceInput.load(my_publisher.package_id, files['prices'], commit=False)
        logging.getLogger('').setLevel(log_level)
        if success:
            logger.info(message)
        else:
            raise RuntimeError(message)

    if files.get('perpetual_access', None):
        logger.info(u'loading perpetual access list {} for publisher {}'.format(
            files['perpetual_access'], my_publisher.package_id)
        )
        logging.getLogger('').setLevel(logging.WARNING)
        success, message = PerpetualAccessInput.load(my_publisher.package_id, files['perpetual_access'], commit=False)
        logging.getLogger('').setLevel(log_level)
        if success:
            logger.info(message)
        else:
            raise RuntimeError(message)

    if files.get('counter', None):
        logger.info(u'loading counter {} for publisher {}'.format(
            files['counter'], my_publisher.package_id)
        )
        logging.getLogger('').setLevel(logging.WARNING)
        success, message = CounterInput.load(my_publisher.package_id, files['counter'], commit=False)
        logging.getLogger('').setLevel(log_level)
        if success:
            logger.info(message)
        else:
            raise RuntimeError(message)

    if commit:
        logger.info('commit')
        db.session.commit()
    else:
        logger.info('rollback, run with --commit to commit')
        db.session.rollback()
