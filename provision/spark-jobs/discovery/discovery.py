import json
import psycopg2
from sshtunnel import SSHTunnelForwarder

def extract_tables(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='public' AND table_type='BASE TABLE';
    """)
    tables = cursor.fetchall()
    result = []

    for (table,) in tables:
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position;
        """, (table,))
        columns = cursor.fetchall()
        result.append({
            "table_name": table,
            "columns": [{"name": c[0], "type": c[1]} for c in columns]
        })

    cursor.close()
    conn.close()
    return result

def discover_postgres(source):
    ssh_cfg = source.get("ssh")  # Peut être None
    db_cfg = source.get("db")
    tables_info = []

    if ssh_cfg:
        # Connexion avec SSH tunnel
        with SSHTunnelForwarder(
            (ssh_cfg["host"], ssh_cfg["port"]),
            ssh_username=ssh_cfg["user"],
            ssh_password=ssh_cfg["password"],
            remote_bind_address=(db_cfg["host"], db_cfg["port"])
        ) as tunnel:
            print(f"Tunnel SSH ouvert sur le port local {tunnel.local_bind_port}")
            conn = psycopg2.connect(
                host="localhost",
                port=tunnel.local_bind_port,
                user=db_cfg["user"],
                password=db_cfg["password"],
                database=db_cfg["database"]
            )
            tables_info = extract_tables(conn)
    else:
        # Connexion directe sans SSH
        conn = psycopg2.connect(
            host=db_cfg["host"],
            port=db_cfg["port"],
            user=db_cfg["user"],
            password=db_cfg["password"],
            database=db_cfg["database"]
        )
        tables_info = extract_tables(conn)

    return tables_info

def main():
    with open("config/data_sources.json") as f:
        sources = json.load(f)

    discovery_report = {}

    for source in sources:
        print(f"\nDécouverte pour {source['name']} ({source['type']})")
        if source["type"] == "postgres":
            try:
                discovery_report[source["name"]] = discover_postgres(source)
            except Exception as e:
                discovery_report[source["name"]] = {"error": str(e)}
                print(f"❌ Erreur : {e}")
        else:
            discovery_report[source["name"]] = "Découverte non implémentée"

    with open("spark-jobs/discovery/discovery_report.json", "w") as f:
        json.dump(discovery_report, f, indent=2)

    print("\n✅ Rapport de découverte généré : discovery_report.json")

if __name__ == "__main__":
    main()
