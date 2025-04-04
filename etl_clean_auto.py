import pandas as pd
from io import StringIO
from connection_firebird import conectar
from connection_postgres import conn as pg_conn

# ----------------- HELPERS -----------------
def round_numeric_columns(df, decimals=2):
    float_cols = df.select_dtypes(include='float')
    for col in float_cols.columns:
        df[col] = df[col].round(decimals)
    return df

def clean_dataframe(df):
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()
            if "nombre" in col.lower():
                df[col] = df[col].str.lower().str.title()
            elif "clave" in col.lower():
                df[col] = df[col].str.upper()

        if "fecha" in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df = df[df[col].notna()]
            df[col] = df[col].dt.floor('S')

        if df[col].dtype in ['float64', 'int64']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df = round_numeric_columns(df)
    return df.dropna(how='all')

def df_to_postgres(df, table_name, conn):
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    columns = ', '.join([f'"{col}"' for col in df.columns])
    cursor = conn.cursor()

    try:
        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        create_table_sql = pd.io.sql.get_schema(df, table_name, con=conn)
        cursor.execute(create_table_sql)
        cursor.copy_expert(f'COPY "{table_name}" ({columns}) FROM STDIN WITH CSV', buffer)
        conn.commit()
        print(f"✅ Loaded into PostgreSQL: {table_name}")
    except Exception as e:
        conn.rollback()
        print(f"❌ Error loading {table_name}: {e}")
    finally:
        cursor.close()

# ----------------- MAIN ETL FUNCTION -----------------
def extract_clean_and_load():
    fb_con = conectar()
    cursor_fb = fb_con.cursor()

    cursor_fb.execute("""
        SELECT RDB$RELATION_NAME 
        FROM RDB$RELATIONS 
        WHERE RDB$SYSTEM_FLAG = 0 AND RDB$VIEW_BLR IS NULL;
    """)
    tables = [row[0].strip() for row in cursor_fb.fetchall()]

    for table in tables:
        print(f"\n🔍 Processing table: {table}")
        try:
            df = pd.read_sql(f'SELECT * FROM "{table}"', fb_con)
        except Exception as e:
            print(f"❌ Could not read {table}: {e}")
            continue

        if df.empty:
            print(f"⚠️ Table {table} is empty. Dropping from Firebird...")
            try:
                cursor_fb.execute(f'DROP TABLE "{table}"')
                fb_con.commit()
                print(f"✅ Table {table} dropped.")
            except Exception as e:
                print(f"❌ Could not drop {table}: {e}")
            continue

        print(f"📋 {len(df)} rows read from {table}")
        df_clean = clean_dataframe(df)
        print(f"🧹 {len(df_clean)} rows after cleaning")

        dest_table = f"{table.lower()}_clean"
        df_to_postgres(df_clean, dest_table, pg_conn)

    fb_con.close()
    pg_conn.close()
    print("\n✅ ETL process completed and data loaded to PostgreSQL.")

# ----------------- ENTRY POINT -----------------
if __name__ == "__main__":
    extract_clean_and_load()