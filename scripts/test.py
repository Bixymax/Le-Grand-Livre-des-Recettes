import duckdb
con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

DATA_PATH = "../data/recipes_parquets"

for table in ["recipes_main", "ingredients_index", "recipes_nutrition_detail"]:
    print(f"\n=== {table} ===")
    cols = con.execute(f"DESCRIBE SELECT * FROM delta_scan('{DATA_PATH}/{table}')").fetchall()
    for c in cols:
        print(f"  {c[0]:<30} {c[1]}")