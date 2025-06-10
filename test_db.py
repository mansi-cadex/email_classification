#!/usr/bin/env python3
"""
PostgreSQL Connection Test
Tests the database connection and shows basic information.
"""

import psycopg2
import sys
import os
from datetime import datetime

# Print all environment variables for debugging
print("Environment Variables:")
for key in ['PGHOST', 'PGPORT', 'PGDATABASE', 'PGUSER', 'PGPASSWORD']:
    value = os.getenv(key)
    if value:
        print(f"{key}: {value}")
    else:
        print(f"{key}: Not set")

# Database connection parameters from environment variables
DB_CONFIG = {
    'host': os.getenv('PGHOST', '10.60.0.5'),
    'port': int(os.getenv('PGPORT', '5432')),
    'database': os.getenv('PGDATABASE', 'o2c_dev'),
    'user': os.getenv('PGUSER', 'sanskar_gawande'),
    'password': os.getenv('PGPASSWORD', 'sanskar_gawande')
}

print("\nUsing Database Configuration:")
print(f"Host: {DB_CONFIG['host']}")
print(f"Port: {DB_CONFIG['port']}")
print(f"Database: {DB_CONFIG['database']}")
print(f"User: {DB_CONFIG['user']}")
print(f"Password: {'*' * len(DB_CONFIG['password'])}")

def test_postgres_connection():
    """Test PostgreSQL connection and display basic info."""
    
    print("🔌 Testing PostgreSQL Connection...")
    print(f"📍 Host: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print(f"🗄️  Database: {DB_CONFIG['database']}")
    print(f"👤 User: {DB_CONFIG['user']}")
    print("-" * 50)
    
    connection = None
    cursor = None
    
    try:
        # Attempt to connect
        print("⏳ Connecting to PostgreSQL...")
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        print("✅ Connection successful!")
        print()
        
        # Test 1: Get PostgreSQL version
        print("📊 DATABASE INFORMATION:")
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"   PostgreSQL Version: {version}")
        
        # Test 2: Get current timestamp
        cursor.execute("SELECT NOW();")
        current_time = cursor.fetchone()[0]
        print(f"   Server Time: {current_time}")
        
        # Test 3: Get database size
        cursor.execute("""
            SELECT pg_size_pretty(pg_database_size(%s));
        """, (DB_CONFIG['database'],))
        db_size = cursor.fetchone()[0]
        print(f"   Database Size: {db_size}")
        
        print()
        
        # Test 4: List all schemas
        print("📋 AVAILABLE SCHEMAS:")
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schema_name;
        """)
        schemas = cursor.fetchall()
        for schema in schemas:
            print(f"   • {schema[0]}")
        
        print()
        
        # Test 5: List tables in each schema
        print("📚 TABLES BY SCHEMA:")
        for schema in schemas:
            schema_name = schema[0]
            cursor.execute("""
                SELECT table_name, table_type
                FROM information_schema.tables 
                WHERE table_schema = %s
                ORDER BY table_name;
            """, (schema_name,))
            tables = cursor.fetchall()
            
            if tables:
                print(f"   📂 {schema_name} schema:")
                for table_name, table_type in tables:
                    print(f"      • {table_name} ({table_type.lower()})")
            else:
                print(f"   📂 {schema_name} schema: (no tables)")
        
        print()
        
        # Test 6: Check if we can create and drop a test table
        print("🧪 TESTING WRITE PERMISSIONS:")
        try:
            test_table_name = f"connection_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Create test table
            cursor.execute(f"""
                CREATE TABLE {test_table_name} (
                    id SERIAL PRIMARY KEY,
                    test_message TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            
            # Insert test data
            cursor.execute(f"""
                INSERT INTO {test_table_name} (test_message) 
                VALUES ('Connection test successful!');
            """)
            
            # Read test data
            cursor.execute(f"SELECT test_message, created_at FROM {test_table_name};")
            test_result = cursor.fetchone()
            
            # Drop test table
            cursor.execute(f"DROP TABLE {test_table_name};")
            
            # Commit changes
            connection.commit()
            
            print(f"   ✅ CREATE TABLE: Success")
            print(f"   ✅ INSERT DATA: Success")
            print(f"   ✅ SELECT DATA: {test_result[0]}")
            print(f"   ✅ DROP TABLE: Success")
            print(f"   ✅ Write permissions: WORKING")
            
        except Exception as write_error:
            print(f"   ❌ Write permissions: FAILED - {write_error}")
            connection.rollback()
        
        print()
        print("🎉 CONNECTION TEST COMPLETED SUCCESSFULLY!")
        return True
        
    except psycopg2.OperationalError as e:
        print(f"❌ Connection failed: {e}")
        print()
        print("🔧 TROUBLESHOOTING TIPS:")
        print("   • Check if the host/port is accessible")
        print("   • Verify username and password")
        print("   • Ensure PostgreSQL is running")
        print("   • Check firewall settings")
        return False
        
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        return False
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False
        
    finally:
        # Clean up connections
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            print("🔌 Database connection closed.")

def test_connection_with_connection_string():
    """Test using connection string format."""
    print("\n" + "="*60)
    print("🔗 TESTING WITH CONNECTION STRING FORMAT:")
    print("="*60)
    
    connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    
    try:
        connection = psycopg2.connect(connection_string)
        cursor = connection.cursor()
        
        cursor.execute("SELECT 'Connection string format works!' as message;")
        result = cursor.fetchone()[0]
        print(f"✅ {result}")
        
        cursor.close()
        connection.close()
        return True
        
    except Exception as e:
        print(f"❌ Connection string test failed: {e}")
        return False

if __name__ == "__main__":
    print("PostgreSQL Connection Test")
    print("=" * 60)
    
    # Test regular connection
    success1 = test_postgres_connection()
    
    # Test connection string format
    success2 = test_connection_with_connection_string()
    
    print("\n" + "="*60)
    if success1 and success2:
        print("🎯 ALL TESTS PASSED! Your PostgreSQL connection is working perfectly.")
        sys.exit(0)
    else:
        print("⚠️  Some tests failed. Please check your connection settings.")
        sys.exit(1)