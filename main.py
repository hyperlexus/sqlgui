from itertools import tee
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import mysql.connector
import re
import time
import csv

# global config - adjust as needed
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = ""
DB_PORT = 3308 # default mysql port is 3306

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

def copy_table_content(event=None):
    """Kopiert die Spaltennamen und alle sichtbaren Zeilen in die Zwischenablage."""
    
    columns = tree["columns"]
    if not columns:
        messagebox.showinfo("Copy", "No data to copy.")
        return
        
    content = "\t".join(columns) + "\n"
    
    for child in tree.get_children():
        values = tree.item(child, 'values')
        # convert all values to string to avoid issues with None or other types
        content += "\t".join(map(str, values)) + "\n"
        
    root.clipboard_clear()
    root.clipboard_append(content.strip()) # strip removes the last newline
    root.update()
    messagebox.showinfo("Copy", f"{len(tree.get_children())} rows copied to clipboard.")   

def copy_selected_cell(event):
    """Kopiert den Inhalt des Feldes (Zelle), auf das rechts geklickt wurde."""
    try:
        # find the row under the cursor
        item_id = tree.identify_row(event.y)
        if not item_id:
            return

        # find the column under the cursor
        column_id = tree.identify_column(event.x)
        if not column_id:
            return

        # convert column id to index
        column_index = int(column_id.replace('#', '')) - 1

        # take the value of the cell
        values = tree.item(item_id, 'values')
        
        if 0 <= column_index < len(values):
            cell_value = str(values[column_index])
            
            root.clipboard_clear()
            root.clipboard_append(cell_value)
            root.update()

    except Exception as e:
        # ignore if click was not on a cell
        print(f"Copy error: {e}")
        pass    

def export_to_excel():
    """Exportiert alle aktuell angezeigten Datensätze als CSV-Datei."""
    
    columns = tree["columns"]
    if not columns:
        messagebox.showwarning("Export", "No data to export.")
        return

    # open file dialog to choose save location
    filename = filedialog.asksaveasfilename(
        defaultextension=".csv",
        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        title="Save result as CSV (Excel compatible)"
    )
    
    if not filename:
        return # user cancelled
        
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';') # Semicolon as delimiter for Excel compatibility
            
            # 1. write header
            writer.writerow(columns)
            
            # 2. write data rows
            for child in tree.get_children():
                values = tree.item(child, 'values')
                # convert all values to string to avoid issues with None or other types
                writer.writerow(values)
                
        messagebox.showinfo("Export Success", f"Successfully exported {len(tree.get_children())} rows to:\n{filename}")
        
    except Exception as e:
        messagebox.showerror("Export Error", f"An error occurred during export: {e}")    

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

        if cursor.description:  # SELECT-like queries
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            tree.delete(*tree.get_children())
            tree["columns"] = columns
            tree["show"] = "headings"

            for col in columns:
                tree.heading(col, text=col)  
                tree.column(col, width=100)

            for row in rows:
                tree.insert("", "end", values=row)

            feedback_label.config(
                text=f"{len(rows)} rows in set ({duration:.3f} sec)"
            )
        else:  # INSERT, UPDATE, DELETE (and DDL)
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

context_menu = tk.Menu(root, tearoff=0) # menu for right-click

def show_context_menu(event):
    global context_menu
    
    # test where the click happened
    item_id = tree.identify_row(event.y)
    column_id = tree.identify_column(event.x)
    
    # lastly clear the menu to avoid duplicates
    context_menu.delete(0, tk.END) 

    if item_id and column_id:
        # the click was on a cell
        context_menu.add_command(
            label="Copy Selected Cell", 
            command=lambda: copy_selected_cell(event) 
        )
        context_menu.add_separator()

    # always add these options
    context_menu.add_command(label="Copy All Data (Tab separated)", command=copy_table_content)
    context_menu.add_separator()
    context_menu.add_command(label="Export to Excel (CSV)", command=export_to_excel)

    context_menu.post(event.x_root, event.y_root)

# bind right-click
tree.bind("<Button-3>", show_context_menu)
# Windows/Linux is <Button-3>, Mac is <Button-2>

root.mainloop()