from flask import Blueprint, request, jsonify, Response
from flask_cors import CORS
import pandas as pd
from sklearn.preprocessing import OrdinalEncoder, StandardScaler
from database.db_model import User, Session, Token, ProcessingHistory
import pickle
import datetime
from flask_httpauth import HTTPBasicAuth
from flask_bcrypt import Bcrypt
import json


classification = Blueprint('classification', __name__)
CORS(classification)
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


@classification.route('/api/v1/classification', methods=['POST'])
@auth.login_required
def classify_users():
    if 'file' not in request.files:
        return {'error': 'No file part', 'status': 400}

    file = request.files['file']
    input_df = pd.read_csv(file)

    db_user = session.query(User).filter_by(username=request.form['username']).first()
    if not db_user:
        return Response(status=404, response='A user with provided name was not found.')

    db_token = session.query(Token).filter_by(user_id=db_user.id).first()

    if len(input_df) >= db_token.amount_of_processing_records_left:
        text = f'Quota limit. Your token has: {db_token.amount_of_processing_records_left} quotas, but the file ' \
               f'contains: {len(input_df)}. Payment required.'

        record = ProcessingHistory()
        record.user_id = db_user.id
        record.token_id = db_token.id
        record.amount_of_processed_records = 0
        record.processing_timestamp = datetime.datetime.now()
        record.description = "Failed. Not enough amount of records that can be processed left."

        session.add(record)
        session.commit()
        return jsonify({'data': text}), 402

    input_df = input_df.dropna()

    input_df2 = input_df.copy()

    columns_for_ordinal_encoder = ["consumer_country", "consumer_state", "consumer_city", "consumer_age_label",
                                   "payment_type_set", "product_category_name_english_set", "order_quality_label_set",
                                   "item_price_category_label_set", "payment_label_set", "product_volume_label_set",
                                   "most_frequent_payment_type"]

    ordinal_encoder = OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1)

    df_to_encode = input_df2[columns_for_ordinal_encoder]
    decoded_df = ordinal_encoder.fit_transform(df_to_encode)
    input_df2[columns_for_ordinal_encoder] = decoded_df

    scaler = StandardScaler()
    scaled_input_df = scaler.fit_transform(input_df2)

    with open('/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/src/models/model.pkl', 'rb') as file:
        loaded_model = pickle.load(file)

    input_df['consumer_category'] = loaded_model.predict(scaled_input_df)

    input_df['consumer_category'] = input_df['consumer_category'].replace({0: "Occasional", 1: 'Normal', 2: 'Best'})

    processed_csv = input_df.to_csv(index=False)

    db_token.amount_of_processing_records_left -= len(input_df)

    record = ProcessingHistory()
    record.user_id = db_user.id
    record.token_id = db_token.id
    record.amount_of_processed_records = len(input_df)
    record.processing_timestamp = datetime.datetime.now()
    record.description = "Success"

    session.add(record)
    session.commit()

    return jsonify({'status': 'success', 'data': processed_csv}), 200


@classification.route('/api/v1/global/classification', methods=['POST'])
def global_classify_users():
    data = request.get_json()

    if 'Records' not in data:
        return jsonify({'error': 'No Records found in the request body', 'status': 400}), 400

    if 'PersonalAPIKey' not in data:
        return jsonify({'error': 'No PersonalAPIKey found in the request body', 'status': 400}), 400

    records = data.get('Records', None)
    try:
        input_df = pd.DataFrame(records)
    except Exception as e:
        return jsonify({'error': 'Error while processing records: ' + str(e), 'status': 500}), 500

    db_token = session.query(Token).filter_by(personal_token=data['PersonalAPIKey']).first()
    if not db_token:
        return jsonify({'error': 'Token was not found.', 'status': 404}), 404

    if len(input_df) >= db_token.amount_of_processing_records_left:
        text = f'Quota limit. Your token has: {db_token.amount_of_processing_records_left} quotas, but the file ' \
               f'contains: {len(input_df)}. Payment required.'

        record = ProcessingHistory()
        record.user_id = db_token.user_id
        record.token_id = db_token.id
        record.amount_of_processed_records = 0
        record.processing_timestamp = datetime.datetime.now()
        record.description = "Failed. Quota limit."

        session.add(record)
        session.commit()
        return jsonify({'error': text}), 402

    try:
        input_df = input_df.dropna()

        input_df2 = input_df.copy()

        columns_for_ordinal_encoder = ["consumer_country", "consumer_state", "consumer_city", "consumer_age_label",
                                       "payment_type_set", "product_category_name_english_set",
                                       "order_quality_label_set",
                                       "item_price_category_label_set", "payment_label_set", "product_volume_label_set",
                                       "most_frequent_payment_type"]

        ordinal_encoder = OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1)

        df_to_encode = input_df2[columns_for_ordinal_encoder]
        decoded_df = ordinal_encoder.fit_transform(df_to_encode)
        input_df2[columns_for_ordinal_encoder] = decoded_df

        scaler = StandardScaler()
        scaled_input_df = scaler.fit_transform(input_df2)

        with open('/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/src/models/model.pkl', 'rb') as file:
            loaded_model = pickle.load(file)

        input_df['consumer_category'] = loaded_model.predict(scaled_input_df)

        input_df['consumer_category'] = input_df['consumer_category'].replace({0: "Occasional", 1: 'Normal', 2: 'Best'})
    except Exception as e:
        return jsonify({'error': 'Error while processing records: ' + str(e), 'status': 500}), 500

    processed_data = input_df.to_dict(orient='records')
    db_token.amount_of_processing_records_left -= len(input_df)

    record = ProcessingHistory()
    record.user_id = db_token.user_id
    record.token_id = db_token.id
    record.amount_of_processed_records = len(input_df)
    record.processing_timestamp = datetime.datetime.now()
    record.description = "Success"

    session.add(record)
    session.commit()

    return jsonify({'status': 'success', 'data': processed_data}), 200


@classification.route('/api/v1/processinghistory', methods=['POST'])
@auth.login_required
def get_processing_history():
    data = request.get_json(force=True)

    db_user = session.query(User).filter_by(username=data['username']).first()
    db_token = session.query(Token).filter_by(user_id=db_user.id).first()
    db_history = session.query(ProcessingHistory).filter_by(user_id=db_user.id).order_by(
        ProcessingHistory.processing_timestamp.desc()).all()

    HistoryList = {}
    key = 0
    for i in db_history:
        HistoryList[key] = {'id': i.id, 'username': db_user.username, 'email': db_user.email, 'personal_token': db_token.personal_token, 'amount_of_processed_records': i.amount_of_processed_records, 'processing_timestamp': i.processing_timestamp, 'status': i.description}
        key += 1

    return jsonify({"history": HistoryList})


if __name__ == '__main__':
    classification.run(debug=True)