from app import db
from app import logger
from institution import Institution
from package import Package, clone_demo_package


def get_or_create_placeholder_institution():
    display_name = 'placeholder, see prepared_demo_publisher.py'
    institution = Institution.query.filter(Institution.display_name == display_name).scalar()

    if not institution:
        institution = Institution(
            display_name=display_name,
            is_consortium=False,
            is_demo_institution=True
        )
        db.session.add(institution)

    return institution


def prepare_publishers():
    institution = get_or_create_placeholder_institution()

    prepared_publishers = Package.query.filter(Package.institution_id == institution.id).all()
    num_prepared = len(prepared_publishers)
    to_prepare = 10 - num_prepared

    logger.info(u'Found {} prepared demo publishers. Creating {}.'.format(num_prepared, to_prepare))
    if to_prepare > 0:
        for i in range(0, to_prepare):
            new_publisher = clone_demo_package(institution)
            logger.info(u'Created demo publisher {} ({}).'.format(new_publisher.package_id, institution.id))


def get_demo_publisher(institution, use_prepared=True):
    if use_prepared:
        placeholder_institution = get_or_create_placeholder_institution()
        prepared_publisher = Package.query.filter(Package.institution_id == placeholder_institution.id).first()

        if prepared_publisher:
            prepared_publisher.institution_id = institution.id
            logger.info(u'Got prepared demo publisher {} ({}).'.format(prepared_publisher.package_id, institution.id))
            return prepared_publisher

    new_publisher = clone_demo_package(institution)
    logger.info(u'Created demo publisher {} ({}).'.format(new_publisher.package_id, institution.id))
    return new_publisher


if __name__ == "__main__":
    prepare_publishers()
    db.session.commit()