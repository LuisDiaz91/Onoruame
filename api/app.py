from flask import Flask, jsonify
from flask_cors import CORS
from core.config import settings

app = Flask(__name__)
CORS(app)

@app.route('/health')
def health():
    return jsonify({
        'status': 'healthy',
        'service': 'onoruame-api'
    })

@app.route('/')
def home():
    return jsonify({
        'name': 'Onoruame API',
        'version': '1.0.0',
        'status': 'running'
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
