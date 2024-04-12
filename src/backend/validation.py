from marshmallow import Schema, fields
from marshmallow.validate import Length, Range


class UserSchema(Schema):
    username = fields.String(required=True, validate=Length(max=255))
    firstname = fields.String(required=True, validate=Length(max=255))
    lastname = fields.String(required=True, validate=Length(max=255))
    email = fields.String(required=True, validate=Length(max=255))
    password = fields.String(required=True, validate=Length(max=255))
    phone = fields.String(required=True)


class UpdateUserSchema(Schema):
    username = fields.String(required=False, validate=Length(max=255))
    firstname = fields.String(required=False, validate=Length(max=255))
    lastname = fields.String(required=False, validate=Length(max=255))
    email = fields.Email(required=False, validate=Length(max=255))
    password = fields.String(required=False, validate=Length(max=255))
    phone = fields.String(required=False)