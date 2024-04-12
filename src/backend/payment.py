from flask import Blueprint, request, jsonify
from database.db_model import User, Session, Token, Voucher, TransactionHistory
from flask_cors import CORS
from flask_httpauth import HTTPBasicAuth
import datetime
from flask_bcrypt import Bcrypt

payment = Blueprint('payment', __name__)
CORS(payment)
session = Session()
auth = HTTPBasicAuth()
bcrypt = Bcrypt()


@auth.verify_password
def verify_password(username, password):
    try:
        user = session.query(User).filter_by(username=username).first()
        if user and bcrypt.check_password_hash(user.password, password):
            return username
    except:
        return None


@payment.route('/api/v1/payment', methods=['POST'])
@auth.login_required
def make_payment():
    data = request.get_json(force=True)

    try:
        amount = int(data['amount'])
    except ValueError:
        return jsonify({'status': 'error', 'message': 'Quantity should be an integer'}), 400

    if int(data['totalPrice']) < 0 or int(data['amount']) < 0:
        return jsonify({'status': 'error', 'message': 'Quantity should be non-negative'}), 400

    if int(data['amount']) == 0:
        return jsonify({'status': 'error', 'message': 'Quantity should be grater than 0.'}), 400

    if not isinstance(int(data['amount']), int):
        return jsonify({'status': 'error', 'message': 'Quantity should be an integer'}), 400

    db_user = session.query(User).filter_by(username=data['username']).first()
    db_token = session.query(Token).filter_by(user_id=db_user.id).first()

    db_token.amount_of_processing_records_left += int(data['amount'])

    record = TransactionHistory()
    record.user_id = db_user.id
    record.token_id = db_token.id
    record.amount_of_processing_records_to_add = data['amount']
    record.transaction_amount = data['totalPrice']
    record.transaction_timestamp = datetime.datetime.now()

    session.add(record)
    session.commit()
    session.close()

    return jsonify({'status': 'success'}), 200


@payment.route('/api/v1/voucher', methods=['POST'])
@auth.login_required
def voucher_usage():
    data = request.get_json(force=True)

    db_user = session.query(User).filter_by(username=data['username']).first()
    db_token = session.query(Token).filter_by(user_id=db_user.id).first()
    db_voucher = session.query(Voucher).filter_by(voucher_string=data['voucherCode']).first()

    if db_voucher is None:
        return jsonify({'status': 'error', 'message': 'Voucher does not exist'}), 400

    if db_voucher.voucher_is_active == 0:
        return jsonify({'status': 'error', 'message': 'Voucher is already used'}), 400

    db_token.amount_of_processing_records_left += int(db_voucher.amount_of_processing_records_to_add)
    db_voucher.voucher_is_active = 0
    db_voucher.voucher_used_by_user_id = db_user.id
    db_voucher.voucher_used_timestamp = datetime.datetime.now()

    record = TransactionHistory()
    record.user_id = db_user.id
    record.token_id = db_token.id
    record.amount_of_processing_records_to_add = db_voucher.amount_of_processing_records_to_add
    record.voucher_id = db_voucher.id
    record.transaction_timestamp = datetime.datetime.now()

    session.add(record)
    session.commit()
    session.close()

    return jsonify({'status': 'success'}), 200