import tkinter as tk
from tkinter import ttk, messagebox
import mysql.connector
import re
import time

# Globale Konfiguration (außerhalb der Funktion)
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = ""
DB_PORT = 3308 # normal ist 3306, anpassen falls nötig

def beautify():
    raw_sql = sql_entry.get("1.0", tk.END)

    keywords = [  # todo more keywords (sqlite db)
        "select", "from", "where", "join", "inner", "left", "right", "on",
        "group by", "order by", "having", "limit", "offset", "insert", "into",
        "values", "update", "set", "delete", "create", "table", "drop", "alter",
        "add", "distinct", "union", "all", "as", "and", "or", "not", "in", "is",
        "null", "like", "between", "exists", "case", "when", "then", "else", "end",
        "sum", "avg", "min", "max", "count", "upper", "lower", "show"
    ]

    def replace_keyword(match):
        return match.group(0).upper()

    for kw in sorted(keywords, key=len, reverse=True):
        pattern = r"\b" + re.escape(kw) + r"\b"
        raw_sql = re.sub(pattern, replace_keyword, raw_sql, flags=re.IGNORECASE)

    sql_entry.delete("1.0", tk.END)
    sql_entry.insert("1.0", raw_sql.strip())

def connect_db(database=None):
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,        
            database=database if database else None
        )
        return conn
    except mysql.connector.Error as err:
        messagebox.showerror("connection error", str(err))
        return None

def load_databases():
    conn = connect_db()
    if not conn:
        return
    cursor = conn.cursor()
    try:
        cursor.execute("SHOW DATABASES")
        dbs = [row[0] for row in cursor.fetchall()]
        db_dropdown["values"] = dbs
        if dbs:
            selected_db.set(dbs[0])
    except mysql.connector.Error as err:
        messagebox.showerror("an error occurred when loading databases", str(err))
    finally:
        cursor.close()
        conn.close()

def execute_query():
    # todo save last 10 queries and let user go back and forward in the list
    query = sql_entry.get("1.0", tk.END).strip()
    if not query:
        messagebox.showwarning("warning", "please enter an SQL query.")
        return

    db_name = selected_db.get()
    if not db_name:
        messagebox.showwarning("warning", "please choose a database.")
        return

    conn = connect_db(db_name)
    if not conn:
        return
    cursor = conn.cursor()

    start_time = time.time()

    try:
        cursor.execute(query)
        duration = time.time() - start_time

        if cursor.description:  # SELECT-artige Abfrage
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            tree.delete(*tree.get_children())
            tree["columns"] = columns
            tree["show"] = "headings"

            for col in columns:
                tree.heading(col, text=col)  # todo make table content copyable or export to excel with right click
                tree.column(col, width=100)

            for row in rows:
                tree.insert("", "end", values=row)

            feedback_label.config(
                text=f"{len(rows)} rows in set ({duration:.3f} sec)"
            )
        else:  # INSERT, UPDATE, DELETE (und DDL)
            conn.commit()
            query_upper = query.upper()
            if "CREATE DATABASE" in query_upper or "DROP DATABASE" in query_upper:
                load_databases() 
            
            feedback_label.config(
                text=f"Query OK, {cursor.rowcount} rows affected ({duration:.3f} sec)"
            )
            messagebox.showinfo("success", f"query ran successfully. {cursor.rowcount} rows affected.")  # todo run select after insert/update
    except mysql.connector.Error as err:
        feedback_label.config(text="")
        error_message = str(err)
        error_message = error_message[(error_message.find(';')+2):]
        show_message_box(error_message)
    finally:
        cursor.close()
        conn.close()

def show_message_box(message):
    message_box = tk.Toplevel(root)
    message_label = tk.Label(message_box, text=message, justify="center")
    message_label.pack(pady=10, padx=10)
    button_frame = tk.Frame(message_box)
    button_frame.pack(pady=(0, 10))
    def copy_to_clipboard():
        root.clipboard_clear()
        root.clipboard_append(message)
        root.update()
        copy_button.config(text="copied!")
    copy_button = tk.Button(button_frame, text="copy", command=copy_to_clipboard)
    copy_button.pack(side="left", padx=10)
    close_button = tk.Button(button_frame, text="close", command=message_box.destroy)
    close_button.pack(side="left", padx=10)

root = tk.Tk()
root.title("sql gui")
root.geometry("900x600")

selected_db = tk.StringVar()

db_frame = tk.Frame(root)
db_frame.pack(fill="x", padx=10, pady=(10, 0))

tk.Label(db_frame, text="choose database:").pack(side="left", padx=(0, 5))

db_dropdown = ttk.Combobox(db_frame, textvariable=selected_db, state="readonly")
db_dropdown.pack(side="left")

sql_entry = tk.Text(root, height=10)
sql_entry.pack(fill="x", padx=10, pady=10)
sql_entry.insert("1.0", "SHOW TABLES")

btn_frame = tk.Frame(root)
btn_frame.pack(pady=5)

btn_execute = tk.Button(btn_frame, text="run SQL", command=execute_query)
btn_execute.pack(side="left", padx=5)

btn_beautify = tk.Button(btn_frame, text="SQL beautify", command=beautify)
btn_beautify.pack(side="left", padx=5)

tree_frame = tk.Frame(root)
tree_frame.pack(expand=True, fill="both", padx=10, pady=5)

tree_scroll = tk.Scrollbar(tree_frame)
tree_scroll.pack(side="right", fill="y")

tree = ttk.Treeview(tree_frame, yscrollcommand=tree_scroll.set)
tree.pack(expand=True, fill="both")

tree_scroll.config(command=tree.yview)

feedback_label = tk.Label(root, text="", anchor="w", fg="gray")
feedback_label.pack(fill="x", padx=10, pady=(0, 10))

load_databases()

root.mainloop()
