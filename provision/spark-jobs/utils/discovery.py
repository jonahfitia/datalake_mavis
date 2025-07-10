import json
from sshtunnel import SSHTunnelForwarder
import psycopg2

def discover_postgres(source):
    ssh_cfg = source.get("ssh")
    db_cfg = source.get("db")
    tables_info = []

    with SSHTunnelForwarder(
        (ssh_cfg["host"], ssh_cfg["port"]),
        ssh_username=ssh_cfg["user"],
        ssh_password=ssh_cfg["password"],
        remote_bind_address=(db_cfg["host"], db_cfg["port"])
    ) as tunnel:
        conn = psycopg2.connect(
            host='localhost',
            port=tunnel.local_bind_port,
            user=db_cfg["user"],
            password=db_cfg["password"],
            database=db_cfg["database"]
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='public' AND table_type='BASE TABLE';
        """)
        tables = cursor.fetchall()
        for (table,) in tables:
            cursor.execute(f"""
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_name = %s ORDER BY ordinal_position;
            """, (table,))
            columns = cursor.fetchall()
            tables_info.append({
                "table_name": table,
                "columns": [{"name": c[0], "type": c[1]} for c in columns]
            })
        cursor.close()
        conn.close()

    return tables_info

def main():
    with open("config/data_sources.json") as f:
        sources = json.load(f)

    discovery_report = {}

    for source in sources:
        print(f"Découverte pour {source['name']} ({source['type']})")
        if source["type"] == "postgres":
            discovery_report[source["name"]] = discover_postgres(source)
        else:
            discovery_report[source["name"]] = "Découverte non implémentée"

    with open("discovery_report.json", "w") as f:
        json.dump(discovery_report, f, indent=2)

    print("Rapport de découverte généré dans discovery_report.json")

if __name__ == "__main__":
    main()
