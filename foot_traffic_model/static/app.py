from flask import Flask, request, jsonify
import pandas as pd
import sqlite3
from datetime import datetime

app = Flask(__name__)

# SQLite Database Setup
DATABASE = 'retail_data.db'

def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.execute('''CREATE TABLE IF NOT EXISTS locations
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id TEXT NOT NULL,
                  timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                  latitude REAL NOT NULL,
                  longitude REAL NOT NULL,
                  sales_volume REAL NOT NULL,
                  daily_foot_traffic INTEGER,
                  competitors_nearby INTEGER,
                  area_type TEXT CHECK(area_type IN ('retail', 'residential', 'mixed')),
                  nearby_amenities TEXT,
                  has_parking BOOLEAN,
                  is_main_road BOOLEAN)''')
    return conn

@app.route('/submit-location-data', methods=['POST'])
def submit_data():
    required_fields = {
        'user_id': str,
        'latitude': float,
        'longitude': float,
        'sales_volume': float,
        'competitors_nearby': int,
        'area_type': str
    }
    
    try:
        data = request.json
        # Validate input
        for field, field_type in required_fields.items():
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400
            if not isinstance(data[field], field_type):
                return jsonify({"error": f"{field} must be {field_type.__name__}"}), 400
        
        # Additional validation
        if not -90 <= data['latitude'] <= 90:
            return jsonify({"error": "Latitude must be between -90 and 90"}), 400
        
        # Prepare data for storage
        clean_data = {
            'user_id': data['user_id'],
            'latitude': data['latitude'],
            'longitude': data['longitude'],
            'sales_volume': data['sales_volume'],
            'competitors_nearby': data['competitors_nearby'],
            'area_type': data['area_type'],
            'daily_foot_traffic': data.get('daily_foot_traffic'),
            'nearby_amenities': ','.join(data.get('amenities', [])),
            'has_parking': bool(data.get('has_parking', False)),
            'is_main_road': bool(data.get('is_main_road', False))
        }

        # Store in database
        conn = get_db()
        conn.execute('''INSERT INTO locations 
                     (user_id, latitude, longitude, sales_volume, daily_foot_traffic, 
                      competitors_nearby, area_type, nearby_amenities, has_parking, is_main_road)
                     VALUES (?,?,?,?,?,?,?,?,?,?)''',
                     tuple(clean_data.values()))
        conn.commit()
        conn.close()

        return jsonify({"status": "success", "message": "Data submitted successfully"})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/export-data', methods=['GET'])
def export_data():
    """Endpoint to export collected data as CSV"""
    conn = get_db()
    df = pd.read_sql_query("SELECT * FROM locations", conn)
    conn.close()
    return df.to_csv(index=False)

if __name__ == '__main__':
    app.run(debug=True, port=5001)