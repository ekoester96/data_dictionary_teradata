import pandas as pd
import teradatasql
import requests
import json
from datetime import datetime

# Ollama configuration
OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "gemma3:4b"  # Change to your preferred model

# Configuration for sample rows
SAMPLE_ROWS = 5  # Adjust this number to change how many rows are pulled per table

def get_database_config():
    """Prompt user for Teradata connection details"""
    print("\n" + "=" * 60)
    print("Teradata Database Connection Setup")
    print("=" * 60)
    config = {}
    config['host'] = input("Enter Teradata host (e.g., teradata.company.com): ").strip()
    config['database'] = input("Enter database name to analyze: ").strip()
    config['user'] = input("Enter username: ").strip()
    
    # Use getpass for password (hides input)
    import getpass
    config['password'] = getpass.getpass("Enter password: ")
    
    print("\n" + "-" * 60)
    print("Connection Details:")
    print(f"  Host: {config['host']}")
    print(f"  Database: {config['database']}")
    print(f"  User: {config['user']}")
    print(f"  Sample Rows per Table: {SAMPLE_ROWS}")
    print("-" * 60)
    
    confirm = input("\nProceed with these settings? (y/n): ").strip().lower()
    if confirm != 'y':
        print("Aborted by user.")
        return None
    
    return config

def connect_to_teradata(db_config):
    """Connect to Teradata database"""
    try:
        # Build connection string
        conn = teradatasql.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        print("✓ Successfully connected to Teradata")
        return conn
    except Exception as e:
        print(f"✗ Error connecting to Teradata: {e}")
        return None

def get_all_tables(conn, database_name):
    """Get list of all user tables in the specified database"""
    query = f"""
        SELECT TableName 
        FROM DBC.TablesV 
        WHERE DatabaseName = '{database_name}'
        AND TableKind = 'T'
        AND TableName NOT LIKE 'SYS%'
        AND TableName NOT LIKE 'DBC%'
        ORDER BY TableName
    """
    try:
        df = pd.read_sql(query, conn)
        tables = df['TableName'].tolist()
        print(f"✓ Found {len(tables)} tables in database '{database_name}'")
        return tables
    except Exception as e:
        print(f"✗ Error getting tables: {e}")
        return []

def get_table_dataframe(conn, database_name, table_name):
    """Get dataframe for a specific table"""
    try:
        query = f'SELECT * FROM "{database_name}"."{table_name}" SAMPLE {SAMPLE_ROWS}'
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print(f"  ✗ Error reading table {table_name}: {e}")
        return None

def get_column_info(conn, database_name, table_name):
    """Get column information from Teradata system tables"""
    query = f"""
        SELECT ColumnName, ColumnType
        FROM DBC.ColumnsV
        WHERE DatabaseName = '{database_name}'
        AND TableName = '{table_name}'
        ORDER BY ColumnId
    """
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print(f"  ✗ Error getting column info for {table_name}: {e}")
        return None

def ask_ollama(prompt):
    """Send prompt to Ollama and get response"""
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False
    }
    
    try:
        response = requests.post(OLLAMA_URL, json=payload)
        if response.status_code == 200:
            return response.json()['response'].strip()
        else:
            return f"Error: {response.status_code}"
    except Exception as e:
        return f"Error calling Ollama: {e}"

def analyze_column_with_llm(table_name, column_name, data_type, sample_values):
    """Use LLM to generate a description of the column"""
    prompt = f"""Based on this database column information, provide a SHORT description (10-15 words max) of what this column contains:

Table: {table_name}
Column: {column_name}
Data Type: {data_type}
Sample Values: {sample_values}

Provide ONLY a brief description, nothing else."""
    
    description = ask_ollama(prompt)
    return description

def generate_data_dictionary(conn, database_name):
    """Generate complete data dictionary for all tables"""
    tables = get_all_tables(conn, database_name)
    
    if not tables:
        print("No tables found!")
        return None
    
    data_dictionary = []
    
    for table_name in tables:
        print(f"\nProcessing table: {table_name}")
        
        # Get dataframe with sample data
        df = get_table_dataframe(conn, database_name, table_name)
        if df is None:
            continue
        
        # Get column info
        column_info = get_column_info(conn, database_name, table_name)
        if column_info is None:
            continue
        
        # Process each column
        for _, col_row in column_info.iterrows():
            column_name = col_row['ColumnName']
            data_type = col_row['ColumnType']
            
            print(f"  Analyzing column: {column_name}")
            
            # Get sample values (non-null, unique)
            if column_name in df.columns:
                sample_vals = df[column_name].dropna().unique()[:3]  # Get up to 3 unique values
                sample_values = ', '.join([str(val) for val in sample_vals])
            else:
                sample_values = "N/A"
            
            # Get LLM description
            description = analyze_column_with_llm(
                table_name, 
                column_name, 
                data_type, 
                sample_values
            )
            
            # Add to dictionary
            data_dictionary.append({
                'database_name': database_name,
                'table_name': table_name,
                'column_name': column_name,
                'data_type': data_type,
                'description': description,
                'sample_values': sample_values
            })
    
    return data_dictionary

def save_to_csv(data_dictionary, database_name, filename=None):
    """Save data dictionary to CSV file"""
    if not data_dictionary:
        print("No data to save!")
        return
    
    try:
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data_dictionary_{database_name}_{timestamp}.csv"
        
        df = pd.DataFrame(data_dictionary)
        df.to_csv(filename, index=False)
        print(f"\n✓ Data dictionary saved to: {filename}")
        print(f"  Total entries: {len(data_dictionary)}")
    except Exception as e:
        print(f"✗ Error saving CSV: {e}")

def main():
    """Main execution function"""
    print("=" * 60)
    print("Teradata Database Data Dictionary Generator")
    print("=" * 60)
    
    # Get database configuration from user
    db_config = get_database_config()
    if not db_config:
        return
    
    # Connect to database
    conn = connect_to_teradata(db_config)
    if not conn:
        return
    
    try:
        # Generate data dictionary
        print("\nGenerating data dictionary...")
        data_dict = generate_data_dictionary(conn, db_config['database'])
        
        # Save to CSV
        if data_dict:
            save_to_csv(data_dict, db_config['database'])
        
    finally:
        conn.close()
        print("\n✓ Teradata connection closed")

if __name__ == "__main__":
    main()
