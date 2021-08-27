# from https://flask.palletsprojects.com/en/2.0.x/testing/#testing

# import os
# import tempfile

# import pytest

# from app import app
# # from flaskr.db import init_db

# @pytest.fixture
# def client():
#     db_fd, db_path = tempfile.mkstemp()
#     # app = create_app({'TESTING': True, 'DATABASE': db_path})

#     with app.test_client() as client:
#         with app.app_context():
#             init_db() # from 
#         yield client

#     os.close(db_fd)
#     os.unlink(db_path)
