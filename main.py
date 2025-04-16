import oracledb
import os
from db import connect_to_oracle_db
from rich.console import Console
from rich.progress import Progress
from rich.live import Live
from rich.table import Table

console = Console()

IGNORED_SCHEMAS = [
    "SYS",
    "SYSTEM",
    "OUTLN",
    "DBSNMP",
    "APPQOSSYS",
    "CTXSYS",
    "DVSYS",
    "FLOWS_FILES",
    "MDSYS",
    "ORDDATA",
    "ORDSYS",
    "XDB",
    "WMSYS",
    "PUBLIC",
    "AUDSYS",
    "GSMADMIN_INTERNAL",
    "PERFSTAT",
    "OJVMSYS",
    "EXP_INFO",
]

USER_SCOPE_FOR_TABLES = f"""
SELECT owner, table_name
FROM all_tables
WHERE owner not like '%ORACLE%' and owner NOT IN ({",".join(f"'{schema}'" for schema in IGNORED_SCHEMAS)})
"""


def fetch_tables(cursor) -> list:
    """Fetch all tables within the user scope."""

    cursor.execute(USER_SCOPE_FOR_TABLES)
    return cursor.fetchall()


def fetch_columns(cursor, table_owner, table_name) -> list:
    """Fetch all columns for a given table."""
    cursor.execute(
        """
        SELECT column_name, data_type, nullable
        FROM all_tab_columns
        WHERE owner = :owner
        AND table_name = :table_name
        """,
        [table_owner, table_name],
    )
    return cursor.fetchall()


def generate_rust_struct(table_owner, table_name, columns) -> str:
    """Generate a Rust struct for a given table."""
    struct_name = "".join(word.capitalize() for word in table_name.lower().split("_"))
    struct_lines = [
        f"// Table: {table_owner}.{table_name}",
        "#[derive(Debug, Clone, Serialize, Deserialize, RowValue)]",
        f"pub struct {struct_name} {{",
    ]

    for column_name, data_type, nullable in columns:
        rust_type = map_oracle_type_to_rust(data_type)
        if nullable == "Y":
            rust_type = f"Option<{rust_type}>"
        field_name = column_name.lower()
        struct_lines.append(f"    pub {field_name}: {rust_type},")

    struct_lines.append("}")
    return "\n".join(struct_lines)


def save_rust_structs(rust_structs: list, output_dir: str = "generated_structs"):
    """Save all generated Rust structs to a file."""
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, "db_structs.rs"), "w") as f:
        f.write("\n\n".join(rust_structs))
        f.write("\n")
    console.print("[green]Rust structs generated successfully![/green]")


def map_oracle_type_to_rust(
    oracle_type: str, precision: int = None, scale: int = None
) -> str:
    """Map Oracle data types to Rust data types."""
    type_mapping = {
        "VARCHAR2": "String",
        "CHAR": "String",
        "DATE": "chrono::NaiveDateTime",
        "TIMESTAMP": "chrono::NaiveDateTime",
        "CLOB": "String",
        "BLOB": "Vec<u8>",
    }

    if oracle_type.upper() == "NUMBER":
        if scale == 0:  # Integer type
            if precision is None or precision > 10:
                return "i64"  # Larger integers
            else:
                return "i32"  # Smaller integers
        else:  # Floating-point type
            return "f32" if precision and precision <= 7 else "f64"

    return type_mapping.get(oracle_type.upper(), "String")


def create_rust_structs(conn: oracledb.Connection):
    """Main function to create Rust structs from Oracle tables."""
    cursor = conn.cursor()
    tables = fetch_tables(cursor)
    total_tables = len(tables)

    rust_structs = []
    with Live(console=console, refresh_per_second=10) as live:
        progress = Progress(console=console)
        task = progress.add_task("[magenta]Processing tables...", total=total_tables)

        for idx, (table_owner, table_name) in enumerate(tables, start=1):
            # Update progress and display
            table = Table.grid()
            table.add_row(f"[cyan]Processing {table_owner}.{table_name}")
            table.add_row(progress.get_renderable())
            live.update(table)

            progress.update(task, advance=1)

            # Fetch columns and generate Rust struct
            columns = fetch_columns(cursor, table_owner, table_name)
            rust_struct = generate_rust_struct(table_owner, table_name, columns)
            rust_structs.append(rust_struct)

    save_rust_structs(rust_structs)


if __name__ == "__main__":
    console.print("[bold blue]ora-reflector![/bold blue]")
    console.print("ora-reflector is connecting to the database...")

    conn = connect_to_oracle_db()
    if conn is None:
        console.print(
            "[bold red]----------------------------------------------[/bold blue]"
        )
        console.print(
            "[bold red]There was a problem connecting to the database[/bold blue]"
        )
        exit("Please check the connection details in the .env file")

    create_rust_structs(conn=conn)
