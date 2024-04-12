from flask import Flask
from waitress import serve
from flask_cors import CORS
from classification import classification
from user import user
from payment import payment

app = Flask(__name__)

app.register_blueprint(classification)
app.register_blueprint(user)
app.register_blueprint(payment)

CORS(app)

serve(app, host='127.0.0.1', port=5000, threads=1)

if __name__ == '__main__':
    app.run()