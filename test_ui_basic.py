"""
Quick start test for the Stream Data Management System UI.

This script tests the basic UI functionality without requiring Kafka to be running.
"""

import os
import sys
import time
import json

# Add project to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ui.app import UIApp
from ui.data_buffer import QueryOutputBuffer, EventBuffer
from datetime import datetime


def test_ui_components():
    """Test UI components in isolation."""
    print("=" * 60)
    print("Testing Stream Data Management System UI Components")
    print("=" * 60)
    
    # Test 1: Event Buffer
    print("\n[Test 1] Event Buffer")
    buffer = EventBuffer(max_size=10)
    
    for i in range(5):
        buffer.add_event({
            "sensor_id": f"s{i}",
            "value": 100.0 + i * 5,
            "timestamp": datetime.utcnow().isoformat()
        })
    
    events = buffer.get_all_events()
    print(f"✓ Added 5 events, buffer contains {len(events)} events")
    print(f"✓ Buffer stats: {buffer.get_stats()}")
    
    # Test 2: Query Output Buffer
    print("\n[Test 2] Query Output Buffer")
    qbuffer = QueryOutputBuffer(buffer_size=50)
    
    for topic in ["pollution_out", "weather_out"]:
        for i in range(3):
            qbuffer.add_event(topic, {
                f"field_{i}": float(i * 10),
                "timestamp": datetime.utcnow().isoformat()
            })
    
    queries = qbuffer.get_all_queries()
    print(f"✓ Tracked {len(queries)} output topics: {queries}")
    
    stats = qbuffer.get_all_stats()
    for topic, stat in stats.items():
        print(f"✓ {topic}: {stat['count']} events")
    
    # Test 3: Flask App
    try:
        app = UIApp()
        print(f"✓ UIApp initialized")
        print(f"✓ Flask app created: {app.flask_app}")
        print(f"✓ Kafka consumer available: {app.kafka_consumer}")
        print(f"✓ Database service available: {app.db_service}")
        
        # Test API endpoints (without running server)
        with app.flask_app.test_client() as client:
            print("\n[Test 5] API Endpoints")
            
            # Test config endpoint
            response = client.get('/api/config')
            assert response.status_code == 200, "Config endpoint failed"
            config = response.get_json()
            print(f"✓ /api/config: {config}")
            
            # Test health endpoint
            response = client.get('/api/health')
            assert response.status_code == 200, "Health endpoint failed"
            health = response.get_json()
            print(f"✓ /api/health: {health}")
            
            # Test queries endpoint (no active schema)
            response = client.get('/api/queries')
            assert response.status_code == 200, "Queries endpoint failed"
            queries_data = response.get_json()
            print(f"✓ /api/queries: {queries_data['total']} queries (expected 0, no active schema)")
            
            # Test dashboard endpoint
            response = client.get('/')
            assert response.status_code == 200, "Dashboard endpoint failed"
            print(f"✓ /: Dashboard HTML served ({len(response.data)} bytes)")
    
    except Exception as e:
        print(f"✗ Error testing Flask app: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n" + "=" * 60)
    print("All UI component tests passed!")
    print("=" * 60)
    return True


def print_usage():
    """Print usage instructions."""
    print("\n" + "=" * 60)
    print("STREAM DATA MANAGEMENT SYSTEM - UI DASHBOARD")
    print("=" * 60)
    print("\nUsage Instructions:")
    print("\n1. Start the CLI:")
    print("   python -m examples.cli --schema schemas/pollution2.json")
    print("\n2. In the CLI, launch the UI:")
    print("   sdms> ui")
    print("   (or specify a custom port: ui --port 8080)")
    print("\n3. The dashboard will open in your browser at:")
    print("   http://localhost:5000")
    print("\n4. In the dashboard:")
    print("   - Select a query from the dropdown")
    print("   - Choose visualization type (time-series, bar, gauge, heatmap)")
    print("   - Toggle between live Kafka data and historical SQLite data")
    print("   - Auto-refresh data with configurable interval")
    print("   - View query info, buffer status, and data table")
    print("\n5. To view schema information:")
    print("   - Click the 'View Schema' button in the dashboard")
    print("\n6. To stop the UI:")
    print("   - Press Ctrl+C in the CLI (UI will stop automatically)")
    print("\n" + "=" * 60)


if __name__ == "__main__":
    success = test_ui_components()
    print_usage()
    sys.exit(0 if success else 1)
