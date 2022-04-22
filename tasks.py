from celery import Celery
from app import app
from app import get_db_cursor

def make_celery(app):
    celery = Celery(
        app.import_name,
        backend=app.config['CELERY_RESULT_BACKEND'],
        broker=app.config['CELERY_BROKER_URL']
    )
    celery.conf.update(app.config)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask
    return celery

def update_apc(package_id):
    find_q = 'select publisher from jump_account_package where package_id = %s'
    delete_q = 'delete from jump_apc_authorships where package_id = %s'
    insert_q = """
        insert into jump_apc_authorships (
            select * from jump_apc_authorships_view
            where package_id = %s and issn_l in 
            (select issn_l from journalsdb_computed rj where rj.publisher = %s))
    """
    
    with get_db_cursor() as cursor:
        cursor.execute(find_q, (package_id,))
        row = cursor.fetchone()

    if row:
        with get_db_cursor() as cursor:
            cursor.execute(delete_q, (package_id,))
            cursor.execute(insert_q, (package_id, row['publisher'],))

celery = make_celery(app)

@celery.task()
def update_apc_authships(package_id):
    update_apc(package_id)
    return f"apc authorships updated for {package_id}"
