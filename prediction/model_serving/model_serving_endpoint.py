from os import getenv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, dict_factory
from cassandra.protocol import ProtocolException

from operator import itemgetter

from flask import (render_template as rt,
                   Flask, request, redirect, url_for, session, jsonify)
from flask_cors import CORS

from gevent.pywsgi import WSGIServer

import jwt
import re
from datetime import datetime, timedelta

"""
    Database connector
"""


class Cassandra:
    def __init__(self):
        self.MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
        self.MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
        self.MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
        self.MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')

        self.QUERY = 'SELECT * FROM ga_chp_bq_predictions WHERE client_id = ? LIMIT 1'
        self.CASS_REQ_TIMEOUT = 3600.0

        self.auth_provider = PlainTextAuthProvider(
            username=self.MORPHL_CASSANDRA_USERNAME,
            password=self.MORPHL_CASSANDRA_PASSWORD)
        self.cluster = Cluster(
            [self.MORPHL_SERVER_IP_ADDRESS], auth_provider=self.auth_provider)

        self.session = self.cluster.connect(self.MORPHL_CASSANDRA_KEYSPACE)
        self.session.row_factory = dict_factory
        self.session.default_fetch_size = 100

        self.prepare_statements()

    def prepare_statements(self):
        """
            Prepare statements for database select queries
        """
        self.prep_stmts = {
            'predictions': {}
        }

        template_for_single_row = 'SELECT * FROM ga_chp_bq_predictions WHERE client_id = ? LIMIT 1'

        self.prep_stmts['predictions']['single'] = self.session.prepare(
            template_for_single_row)

    def retrieve_prediction(self, client_id):
        bind_list = [client_id]
        return self.session.execute(self.prep_stmts['predictions']['single'], bind_list, timeout=self.CASS_REQ_TIMEOUT)._current_rows


"""
    API class for verifying credentials and handling JWTs.
"""


class API:
    def __init__(self):
        self.API_DOMAIN = getenv('API_DOMAIN')
        self.MORPHL_API_KEY = getenv('MORPHL_API_KEY')
        self.MORPHL_API_JWT_SECRET = getenv('MORPHL_API_JWT_SECRET')

    def verify_jwt(self, token):
        try:
            decoded = jwt.decode(token, self.MORPHL_API_JWT_SECRET)
        except Exception:
            return False

        return (decoded['iss'] == self.API_DOMAIN and
                decoded['sub'] == self.MORPHL_API_KEY)


app = Flask(__name__)
CORS(app)

# @todo Check request origin for all API requests


@app.route("/churning-bq")
def main():
    return "MorphL Predictions API - Churning Users with BigQuery"


@app.route('/churning-bq/getprediction/<client_id>')
def get_prediction(client_id):
    # Validate authorization header with JWT
    if request.headers.get('Authorization') is None or not app.config['API'].verify_jwt(request.headers['Authorization']):
        return jsonify(status=0, error='Unauthorized request.'), 401

    # Validate client id (alphanumeric with dots)
    if not re.match('^[a-zA-Z0-9.]+$', client_id):
        return jsonify(status=0, error='Invalid client id.')

    p = app.config['CASSANDRA'].retrieve_prediction(client_id)

    if len(p) == 0:
        return jsonify(status=0, error='No associated predictions found for that ID.')

    return jsonify(status=1, prediction={'client_id': client_id, 'prediction': p[0]['prediction']})


if __name__ == '__main__':
    app.config['CASSANDRA'] = Cassandra()
    app.config['API'] = API()
    if getenv('DEBUG'):
        app.config['DEBUG'] = True
        flask_port = 5858
        app.run(host='0.0.0.0', port=flask_port)
    else:
        app.config['DEBUG'] = False
        flask_port = 6868
        WSGIServer(('', flask_port), app).serve_forever()
