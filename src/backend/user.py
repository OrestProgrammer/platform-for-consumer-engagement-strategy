from flask import Blueprint, Response, request, jsonify
from marshmallow import ValidationError
from flask_bcrypt import Bcrypt
from database.db_model import User, Session, Token
from validation import UserSchema, UpdateUserSchema
from sqlalchemy.exc import IntegrityError
import datetime
import uuid
from flask_cors import CORS
from flask_httpauth import HTTPBasicAuth


user = Blueprint('user', __name__)
CORS(user)
bcrypt = Bcrypt()
session = Session()
auth = HTTPBasicAuth()


@auth.verify_password
def verify_password(username, password):
    try:
        user = session.query(User).filter_by(username=username).first()
        if user and bcrypt.check_password_hash(user.password, password):
            return username
    except:
        return None


@user.route('/api/v1/user', methods=['POST'])
def create_user():
    data = request.get_json(force=True)

    db_user = session.query(User).filter_by(username=data['username']).first()
    if db_user:
        return Response(status=404, response='User with such username already exist.')

    try:
        UserSchema().load(data)
    except ValidationError as err:
        return jsonify(err.messages), 401

    new_user = User()
    new_user.username = data['username']
    new_user.firstname = data['firstname']
    new_user.lastname = data['lastname']
    new_user.email = data['email']
    hashed_password = bcrypt.generate_password_hash(data['password'])
    new_user.password = hashed_password
    new_user.phone = data['phone']

    try:
        session.add(new_user)
        session.commit()
    except IntegrityError:
        session.rollback()
        return Response(status=400, response="Error")

    new_token = Token()
    new_token.user_id = new_user.id
    new_token.personal_token = str(uuid.uuid4())
    new_token.amount_of_processing_records_left = 10000
    new_token.creating_timestamp = datetime.datetime.now()

    try:
        session.add(new_token)
        session.commit()
        return Response(status=200, response="New user and token were added to the database")
    except IntegrityError:
        session.rollback()
        return Response(status=400, response="Failed to create a new token for the user.")
    finally:
        session.close()


@user.route('/api/v1/user/login', methods=['POST'])
def login_user():
    data = request.get_json()

    # Check if user exists
    db_user = session.query(User).filter_by(username=data["username"]).first()
    if not db_user:
        return Response(status=404, response='User with such username does not exist.')

    username = verify_password(data["username"], data["password"])

    if username:
        return Response(status=200, response='Successful login!')
    else:
        return Response(status=403, response='Password is incorrect!')


@user.route('/api/v1/user/<username>', methods=['GET'])
@auth.login_required
def get_user(username):
    if auth.username() != username:
        return Response(status=406, response='Access denied')
    db_user = session.query(User).filter_by(username=username).first()
    db_token = session.query(Token).filter_by(user_id=db_user.id).first()
    if not db_user:
        return Response(status=404, response='User not found')

    user_data = {'id': db_user.id,
                 'username': db_user.username,
                 'firstname': db_user.firstname,
                 'lastname': db_user.lastname,
                 'email': db_user.email,
                 'phone': db_user.phone,
                 'personal_token': db_token.personal_token,
                 'processing_rows_left': db_token.amount_of_processing_records_left
                 }

    session.close()
    return jsonify({"user": user_data})


@user.route('/api/v1/user/info/<username>', methods=['PUT'])
@auth.login_required
def update_user(username):
    if auth.username() != username:
        return Response(status=406, response='Access denied')
    data = request.get_json(force=True)

    db_user = session.query(User).filter_by(username=username).first()
    if not db_user:
        return Response(status=404, response='A user with provided name was not found.')

    try:
        UpdateUserSchema().load(data)
    except ValidationError as err:
        return jsonify(err.messages), 400

    if 'username' in data.keys():
        db_user.username = data['username']
    if 'firstname' in data.keys():
        db_user.firstname = data['firstname']
    if 'lastname' in data.keys():
        db_user.lastname = data['lastname']
    if 'email' in data.keys():
        db_user.email = data['email']
    if 'password' in data.keys():
        hashed_password = bcrypt.generate_password_hash(data['password'])
        db_user.password = hashed_password
    if 'phone' in data.keys():
        db_user.phone = data['phone']

    session.commit()

    user_data = {'id': db_user.id,
                 'username': db_user.username,
                 'firstname': db_user.firstname,
                 'lastname': db_user.lastname,
                 'email': db_user.email,
                 'phone': db_user.phone,
                 }
    return jsonify({"user": user_data})


@user.route('/api/v1/user/<username>', methods=['DELETE'])
@auth.login_required
def delete_user(username):
    if auth.username() != username:
        return Response(status=406, response='Access denied')
    db_user = session.query(User).filter_by(username=username).first()
    if not db_user:
        return Response(status=404, response='User not found')

    session.delete(db_user)
    session.commit()

    return Response(status=200, response='User was deleted.')