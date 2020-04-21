import argparse
from werkzeug.security import generate_password_hash

from app import db
from app import logger
from grid_id import GridId
from institution import Institution
from package import Package
from permission import Permission, UserInstitutionPermission
from ror_id import RorId
from user import User

# configuration here

users = [
    {
        'email': u'jane@example.edu',  # required
        'password': u'',  # required
        'name': u'Jane',  # default is None
        'permissions': [u'view', u'modify', u'admin', ]  # default is view, modify, admin
    },
    {
        'email': u'mike@example.edu',
        'password': u'',
        'name': u'Mike',
    }
]

institution_name = u'Eastern Example State'

# set at least one grid id
grid_ids = [u'grid.433631.0', ]
ror_ids = [u'00xbe3815', ]

# configuration above

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--commit', help='Commit changes.', action='store_true', default=False)
    commit = parser.parse_args().commit

    new_institution = Institution()
    new_institution.display_name = institution_name
    new_institution.is_demo_institution = False
    db.session.add(new_institution)
    logger.info(u'adding {}'.format(new_institution))

    for user_info in users:
        if User.query.filter(User.email == user_info['email']).scalar():
            raise ValueError(u'user with email {} already exists'.format(user_info['email']))

        new_user = User()
        new_user.email = user_info['email']
        new_user.password_hash = generate_password_hash(user_info['password'])
        new_user.display_name = user_info.get('name', None)
        db.session.add(new_user)
        logger.info(u'adding {}'.format(new_user))

        permission_names = user_info.get('permissions', [u'view', u'modify', u'admin', ])

        for permission_name in permission_names:
            perm = Permission.get(permission_name)
            if not perm:
                raise ValueError(u'unknown permission {}'.format(permission_name))
            user_perm = UserInstitutionPermission(
                user_id=new_user.id,
                institution_id=new_institution.id,
                permission_id=perm.id
            )
            db.session.add(user_perm)
            db.session.flush()
            logger.info(u'adding {}'.format(user_perm))

        for r_id in ror_ids:
            db.session.add(RorId(institution_id=new_institution.id, ror_id=r_id))
            logger.info(u'adding ROR ID {} for {}'.format(r_id, new_institution.display_name))

        for g_id in grid_ids:
            db.session.add(GridId(institution_id=new_institution.id, grid_id=g_id))
            logger.info(u'adding GRID ID {} for {}'.format(g_id, new_institution.display_name))

        # drop table jump_apc_authorships_new;
        # create table jump_apc_authorships_new distkey (package_id) interleaved sortkey (package_id, doi, issn_l) as (select * from jump_apc_authorships_view);
        # select * from jump_apc_authorships_new order by random() limit 1000;
        # alter table jump_apc_authorships rename to jump_apc_authorships_old;
        # alter table jump_apc_authorships_new rename to jump_apc_authorships;
        # drop table jump_apc_authorships_old;

    if commit:
        logger.info('commit')
        db.session.commit()
    else:
        logger.info('rollback')
        db.session.rollback()
