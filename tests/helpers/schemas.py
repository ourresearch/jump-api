# FIXME: double check this, I'm not actually sure these fields are required
user_schema = {
    "type": "object",
    "required": [
        "id",
        "name",
        "email",
        "username",
        "is_demo",
        "is_password_set",
        "user_permissions",
        "institutions",
        "consortia",
    ],
    "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"},
        "email": {"type": "string"},
        "username": {"type": ["string", "null"]},
        "is_demo": {"type": ["boolean", "null"]},
        "is_password_set": {"type": "boolean"},
        "user_permissions": {"type": "array"},
        "institutions": {"type": "array"},
        "consortia": {"type": "array"},
    },
}

# FIXME: double check this, I'm not actually sure these fields are required
user_permissions_schema = {
    "type": "object",
    "required": [
        "institution_id",
        "user_id",
        "user_email",
        "username",
        "permissions",
        "institution_name",
        "is_consortium",
        "user_name",
        "is_authenticated_user",
        "is_demo_institution",
    ],
    "properties": {
        "institution_id": {"type": "string"},
        "user_id": {"type": "string"},
        "user_email": {"type": "string"},
        "username": {"type": ["string", "null"]},
        "permissions": {"type": "array"},
        "institution_name": {"type": "string"},
        "is_consortium": {"type": "boolean"},
        "user_name": {"type": "string"},
        "is_authenticated_user": {"type": "boolean"},
        "is_demo_institution": {"type": "boolean"},
    },
}
