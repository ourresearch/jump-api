import ssl
from celery import Celery

from app import app

def make_celery(app):
    celery = Celery(
        app.import_name,
        backend=app.config['CELERY_RESULT_BACKEND'],
        broker=app.config['CELERY_BROKER_URL'],
        broker_use_ssl={"ssl_cert_reqs": ssl.CERT_NONE},
        redis_backend_use_ssl={"ssl_cert_reqs": ssl.CERT_NONE}
    )
    celery.conf.update(app.config)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask
    return celery

def update_apc_inst(institution_id):
    from app import get_db_cursor
    from psycopg2.extensions import AsIs

    find_q = 'select id from jump_institution where id = %s'
    delete_q = 'delete from jump_apc_institutional_authorships where institution_id = %s'
    make_temp_table = """
        select * into temp table %s from jump_apc_institutional_authorships_view
            where institution_id = %s and issn_l in
            (select issn_l from openalex_computed)
    """
    insert_from_temp_table = """
        insert into jump_apc_institutional_authorships (select * from %s)
    """
    temp_table_name = 'apc_inst_temp_' + institution_id.replace('-', '_').lower()

    # with get_db_cursor() as cursor:
    #     cursor.execute(find_q, (institution_id,))
    #     row = cursor.fetchone()

    # if row:
    with get_db_cursor() as cursor:
        cursor.execute(delete_q, (institution_id,))

    # with get_db_cursor() as cursor:
    #     print(cursor.mogrify(make_temp_table, (AsIs(temp_table_name), institution_id,)))
    #     # cursor.execute(make_temp_table, (AsIs(temp_table_name), institution_id,))

    # with get_db_cursor() as cursor:
    #     print(cursor.mogrify(insert_from_temp_table, (AsIs(temp_table_name),)))
        # cursor.execute(insert_from_temp_table, (AsIs(temp_table_name),))

    with get_db_cursor() as cursor:
        cursor.execute(make_temp_table, (AsIs(temp_table_name), institution_id,))
        cursor.execute(insert_from_temp_table, (AsIs(temp_table_name),))

    # cleanup temporary table
    # with get_db_cursor() as cursor:
    #     cursor.execute('drop table %s', (AsIs(temp_table_name),))

celery = make_celery(app)

@celery.task(autoretry_for=(Exception,), retry_kwargs={'max_retries': 3, 'countdown': 3})
def update_apc_inst_authships(institution_id):
    update_apc_inst(institution_id)
    return f"apc institutional authorships updated for {institution_id}"
