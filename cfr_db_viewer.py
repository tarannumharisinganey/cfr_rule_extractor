import sqlite3
import pandas as pd
from tabulate import tabulate
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
import webbrowser
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import seaborn as sns
from typing import List, Dict, Optional
import os

class CFRDatabaseViewerGUI:
    def __init__(self, root):
        """Initialize the GUI application."""
        self.root = root
        self.root.title("CFR Database Explorer - Visual Interface")
        self.root.geometry("1400x900")
        
        # Set style
        self.setup_styles()
        
        # Database connection
        self.db_path = "16crf.db"
        self.conn = None
        self.cursor = None
        
        # Create GUI components
        self.create_widgets()
        
        # Connect to database
        self.connect_to_database()
    
    def setup_styles(self):
        """Configure ttk styles."""
        style = ttk.Style()
        style.theme_use('clam')
        
        # Custom colors
        self.bg_color = "#f0f5ff"
        self.header_color = "#2c3e50"
        self.accent_color = "#3498db"
        self.success_color = "#27ae60"
        self.warning_color = "#f39c12"
        
        style.configure("Header.TLabel", 
                       background=self.header_color, 
                       foreground="white",
                       font=('Arial', 12, 'bold'))
        
        style.configure("Treeview", 
                       background="white",
                       foreground="black",
                       rowheight=25,
                       fieldbackground="white")
        
        style.map('Treeview', background=[('selected', self.accent_color)])
        
        style.configure("Treeview.Heading", 
                       background="#ecf0f1",
                       foreground="black",
                       relief="flat",
                       font=('Arial', 10, 'bold'))
        
        style.configure("Custom.TButton",
                       padding=10,
                       font=('Arial', 10),
                       background=self.accent_color,
                       foreground="white")
        
        self.root.configure(bg=self.bg_color)
    
    def create_widgets(self):
        """Create all GUI widgets."""
        # Create main container
        main_container = ttk.Frame(self.root)
        main_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Header
        header_frame = ttk.Frame(main_container)
        header_frame.pack(fill=tk.X, pady=(0, 10))
        
        header_label = ttk.Label(header_frame, 
                                text="📚 CFR DATABASE EXPLORER", 
                                style="Header.TLabel",
                                font=('Arial', 16, 'bold'))
        header_label.pack(side=tk.LEFT, padx=10)
        
        # Database info label
        self.db_info_label = ttk.Label(header_frame, 
                                      text=f"Database: {self.db_path}", 
                                      font=('Arial', 10))
        self.db_info_label.pack(side=tk.RIGHT, padx=10)
        
        # Main content area (Notebook with tabs)
        self.notebook = ttk.Notebook(main_container)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        # Create tabs
        self.create_database_tab()
        self.create_query_tab()
        self.create_hierarchy_tab()
        self.create_search_tab()
        self.create_visualization_tab()
        self.create_export_tab()
        
        # Status bar
        self.status_bar = ttk.Label(main_container, 
                                   text="Ready", 
                                   relief=tk.SUNKEN, 
                                   anchor=tk.W,
                                   font=('Arial', 9))
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)
    
    def create_database_tab(self):
        """Create database structure tab."""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="🏛️ Database Structure")
        
        # Left panel - Tables list
        left_frame = ttk.Frame(tab)
        left_frame.pack(side=tk.LEFT, fill=tk.Y, padx=5, pady=5)
        
        ttk.Label(left_frame, text="Database Tables", font=('Arial', 11, 'bold')).pack(pady=5)
        
        # Tables listbox
        self.tables_listbox = tk.Listbox(left_frame, height=15, width=25,
                                        font=('Arial', 10),
                                        selectbackground=self.accent_color)
        self.tables_listbox.pack(fill=tk.BOTH, expand=True, pady=5)
        self.tables_listbox.bind('<<ListboxSelect>>', self.on_table_select)
        
        # Refresh button
        ttk.Button(left_frame, text="🔄 Refresh Tables", 
                  command=self.load_tables).pack(pady=5)
        
        # Right panel - Table details
        right_frame = ttk.Frame(tab)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Table info
        self.table_info_label = ttk.Label(right_frame, 
                                         text="Select a table to view details",
                                         font=('Arial', 11, 'bold'))
        self.table_info_label.pack(pady=5)
        
        # Treeview for table schema
        self.schema_tree = ttk.Treeview(right_frame, height=8)
        self.schema_tree.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Configure columns
        self.schema_tree['columns'] = ('Column', 'Type', 'PK', 'Not Null', 'Default')
        self.schema_tree.column('#0', width=0, stretch=tk.NO)
        self.schema_tree.column('Column', width=150)
        self.schema_tree.column('Type', width=100)
        self.schema_tree.column('PK', width=50, anchor=tk.CENTER)
        self.schema_tree.column('Not Null', width=70, anchor=tk.CENTER)
        self.schema_tree.column('Default', width=100)
        
        self.schema_tree.heading('#0', text='')
        self.schema_tree.heading('Column', text='Column Name')
        self.schema_tree.heading('Type', text='Data Type')
        self.schema_tree.heading('PK', text='PK')
        self.schema_tree.heading('Not Null', text='Not Null')
        self.schema_tree.heading('Default', text='Default Value')
        
        # Preview data section
        ttk.Label(right_frame, text="Preview Data", font=('Arial', 11, 'bold')).pack(pady=(10, 5))
        
        # Data preview treeview
        self.preview_tree = ttk.Treeview(right_frame, height=12)
        self.preview_tree.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Add scrollbars
        preview_vsb = ttk.Scrollbar(right_frame, orient="vertical", command=self.preview_tree.yview)
        preview_vsb.pack(side=tk.RIGHT, fill=tk.Y)
        self.preview_tree.configure(yscrollcommand=preview_vsb.set)
        
        preview_hsb = ttk.Scrollbar(right_frame, orient="horizontal", command=self.preview_tree.xview)
        preview_hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.preview_tree.configure(xscrollcommand=preview_hsb.set)
    
    def create_query_tab(self):
        """Create SQL query tab."""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="🔍 SQL Query Editor")
        
        # Query input area
        query_frame = ttk.Frame(tab)
        query_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(query_frame, text="SQL Query:", font=('Arial', 11, 'bold')).pack(anchor=tk.W, pady=(0, 5))
        
        # Query text area with syntax highlighting-like appearance
        self.query_text = scrolledtext.ScrolledText(query_frame, height=8,
                                                   font=('Courier New', 10),
                                                   wrap=tk.WORD)
        self.query_text.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Insert sample queries
        sample_queries = """-- Sample Queries:
-- SELECT * FROM parts LIMIT 10;
-- SELECT part_number, title FROM parts WHERE part_number LIKE '2%';
-- SELECT COUNT(*) as total_sections FROM sections;
-- SELECT p.part_number, COUNT(s.section_id) as section_count 
-- FROM parts p LEFT JOIN sections s ON p.part_id = s.part_id 
-- GROUP BY p.part_number;
"""
        self.query_text.insert('1.0', sample_queries)
        
        # Button panel
        button_frame = ttk.Frame(query_frame)
        button_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(button_frame, text="▶ Execute Query", 
                  command=self.execute_query,
                  style="Custom.TButton").pack(side=tk.LEFT, padx=2)
        
        ttk.Button(button_frame, text="🗑️ Clear", 
                  command=lambda: self.query_text.delete('1.0', tk.END)).pack(side=tk.LEFT, padx=2)
        
        ttk.Button(button_frame, text="📋 Copy", 
                  command=self.copy_query).pack(side=tk.LEFT, padx=2)
        
        # Results area
        results_frame = ttk.Frame(tab)
        results_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        ttk.Label(results_frame, text="Query Results:", font=('Arial', 11, 'bold')).pack(anchor=tk.W, pady=(0, 5))
        
        # Results treeview
        self.results_tree = ttk.Treeview(results_frame)
        self.results_tree.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Add scrollbars
        results_vsb = ttk.Scrollbar(results_frame, orient="vertical", command=self.results_tree.yview)
        results_vsb.pack(side=tk.RIGHT, fill=tk.Y)
        self.results_tree.configure(yscrollcommand=results_vsb.set)
        
        results_hsb = ttk.Scrollbar(results_frame, orient="horizontal", command=self.results_tree.xview)
        results_hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.results_tree.configure(xscrollcommand=results_hsb.set)
        
        # Info label
        self.results_info = ttk.Label(results_frame, text="")
        self.results_info.pack(anchor=tk.W)
    
    def create_hierarchy_tab(self):
        """Create hierarchy visualization tab."""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="🌳 CFR Hierarchy")
        
        # Controls frame
        controls_frame = ttk.Frame(tab)
        controls_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(controls_frame, text="Part Number:").pack(side=tk.LEFT, padx=5)
        self.part_filter = ttk.Entry(controls_frame, width=10)
        self.part_filter.pack(side=tk.LEFT, padx=5)
        
        ttk.Button(controls_frame, text="Load Hierarchy", 
                  command=self.load_hierarchy).pack(side=tk.LEFT, padx=20)
        
        ttk.Button(controls_frame, text="Expand All", 
                  command=self.expand_all_nodes).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(controls_frame, text="Collapse All", 
                  command=self.collapse_all_nodes).pack(side=tk.LEFT, padx=5)
        
        # Treeview for hierarchy
        tree_frame = ttk.Frame(tab)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        self.hierarchy_tree = ttk.Treeview(tree_frame)
        self.hierarchy_tree.pack(fill=tk.BOTH, expand=True)
        
        # Configure columns
        self.hierarchy_tree['columns'] = ('Type', 'Title', 'ID')
        self.hierarchy_tree.column('#0', width=300, minwidth=200)
        self.hierarchy_tree.column('Type', width=100)
        self.hierarchy_tree.column('Title', width=400)
        self.hierarchy_tree.column('ID', width=80)
        
        self.hierarchy_tree.heading('#0', text='Node', anchor=tk.W)
        self.hierarchy_tree.heading('Type', text='Type', anchor=tk.W)
        self.hierarchy_tree.heading('Title', text='Title', anchor=tk.W)
        self.hierarchy_tree.heading('ID', text='ID', anchor=tk.W)
        
        # Add scrollbars
        hierarchy_vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.hierarchy_tree.yview)
        hierarchy_vsb.pack(side=tk.RIGHT, fill=tk.Y)
        self.hierarchy_tree.configure(yscrollcommand=hierarchy_vsb.set)
        
        hierarchy_hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.hierarchy_tree.xview)
        hierarchy_hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.hierarchy_tree.configure(xscrollcommand=hierarchy_hsb.set)
        
        # Bind double-click event
        self.hierarchy_tree.bind('<Double-Button-1>', self.on_hierarchy_double_click)
    
    def create_search_tab(self):
        """Create content search tab."""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="🔎 Advanced Search")
        
        # Search controls
        search_frame = ttk.Frame(tab)
        search_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(search_frame, text="Search Term:").pack(side=tk.LEFT, padx=5)
        self.search_entry = ttk.Entry(search_frame, width=40)
        self.search_entry.pack(side=tk.LEFT, padx=5)
        
        ttk.Label(search_frame, text="Search In:").pack(side=tk.LEFT, padx=(20, 5))
        
        self.search_in_var = tk.StringVar(value="all")
        ttk.Radiobutton(search_frame, text="All", 
                       variable=self.search_in_var, value="all").pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(search_frame, text="Titles", 
                       variable=self.search_in_var, value="titles").pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(search_frame, text="Content", 
                       variable=self.search_in_var, value="content").pack(side=tk.LEFT, padx=5)
        
        ttk.Button(search_frame, text="🔍 Search", 
                  command=self.perform_search).pack(side=tk.LEFT, padx=20)
        
        # Results treeview
        results_frame = ttk.Frame(tab)
        results_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        self.search_tree = ttk.Treeview(results_frame)
        self.search_tree.pack(fill=tk.BOTH, expand=True)
        
        # Configure columns
        self.search_tree['columns'] = ('Type', 'Identifier', 'Snippet')
        self.search_tree.column('#0', width=0, stretch=tk.NO)
        self.search_tree.column('Type', width=100)
        self.search_tree.column('Identifier', width=150)
        self.search_tree.column('Snippet', width=600)
        
        self.search_tree.heading('Type', text='Type', anchor=tk.W)
        self.search_tree.heading('Identifier', text='Identifier', anchor=tk.W)
        self.search_tree.heading('Snippet', text='Snippet', anchor=tk.W)
        
        # Add scrollbars
        search_vsb = ttk.Scrollbar(results_frame, orient="vertical", command=self.search_tree.yview)
        search_vsb.pack(side=tk.RIGHT, fill=tk.Y)
        self.search_tree.configure(yscrollcommand=search_vsb.set)
        
        search_hsb = ttk.Scrollbar(results_frame, orient="horizontal", command=self.search_tree.xview)
        search_hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.search_tree.configure(xscrollcommand=search_hsb.set)
        
        # Bind double-click event
        self.search_tree.bind('<Double-Button-1>', self.on_search_result_double_click)
    
    def create_visualization_tab(self):
        """Create data visualization tab."""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="📊 Visualizations")
        
        # Visualization controls
        controls_frame = ttk.Frame(tab)
        controls_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(controls_frame, text="Chart Type:").pack(side=tk.LEFT, padx=5)
        self.chart_type = ttk.Combobox(controls_frame, 
                                      values=["Bar Chart", "Pie Chart", "Line Chart", "Statistics"],
                                      state="readonly",
                                      width=15)
        self.chart_type.pack(side=tk.LEFT, padx=5)
        self.chart_type.set("Statistics")
        
        ttk.Button(controls_frame, text="Generate Chart", 
                  command=self.generate_chart).pack(side=tk.LEFT, padx=20)
        
        # Chart container
        self.chart_frame = ttk.Frame(tab)
        self.chart_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Statistics text area
        self.stats_text = scrolledtext.ScrolledText(self.chart_frame, height=20,
                                                   font=('Arial', 10),
                                                   wrap=tk.WORD)
        self.stats_text.pack(fill=tk.BOTH, expand=True)
    
    def create_export_tab(self):
        """Create data export tab."""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="💾 Export Data")
        
        # Export options
        options_frame = ttk.Frame(tab)
        options_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(options_frame, text="Export Format:").pack(side=tk.LEFT, padx=5)
        self.export_format = ttk.Combobox(options_frame, 
                                         values=["CSV", "JSON", "Excel", "HTML"],
                                         state="readonly",
                                         width=10)
        self.export_format.pack(side=tk.LEFT, padx=5)
        self.export_format.set("CSV")
        
        ttk.Label(options_frame, text="Table:").pack(side=tk.LEFT, padx=(20, 5))
        self.export_table = ttk.Combobox(options_frame, 
                                        values=["parts", "subparts", "sections", "subsections"],
                                        state="readonly",
                                        width=15)
        self.export_table.pack(side=tk.LEFT, padx=5)
        self.export_table.set("parts")
        
        ttk.Button(options_frame, text="Browse...", 
                  command=self.browse_export_path).pack(side=tk.LEFT, padx=20)
        
        # Export path
        path_frame = ttk.Frame(tab)
        path_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(path_frame, text="Export Path:").pack(side=tk.LEFT, padx=5)
        self.export_path = ttk.Entry(path_frame, width=60)
        self.export_path.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        
        # Export button
        ttk.Button(tab, text="📥 Export Data", 
                  command=self.export_data,
                  style="Custom.TButton").pack(pady=20)
        
        # Log area
        log_frame = ttk.Frame(tab)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        ttk.Label(log_frame, text="Export Log:").pack(anchor=tk.W, pady=(0, 5))
        self.export_log = scrolledtext.ScrolledText(log_frame, height=10,
                                                   font=('Courier New', 9),
                                                   wrap=tk.WORD)
        self.export_log.pack(fill=tk.BOTH, expand=True)
    
    def connect_to_database(self):
        """Connect to the SQLite database."""
        try:
            if not os.path.exists(self.db_path):
                messagebox.showwarning("Database Not Found", 
                                      f"Database file '{self.db_path}' not found.\nPlease select a database file.")
                self.browse_database()
                return
            
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            
            self.update_status(f"Connected to database: {self.db_path}")
            self.load_tables()
            
        except Exception as e:
            messagebox.showerror("Connection Error", f"Cannot connect to database:\n{str(e)}")
            self.update_status("Database connection failed", error=True)
    
    def browse_database(self):
        """Browse for database file."""
        file_path = filedialog.askopenfilename(
            title="Select CFR Database File",
            filetypes=[("SQLite Database", "*.db"), ("All Files", "*.*")]
        )
        
        if file_path:
            self.db_path = file_path
            self.db_info_label.config(text=f"Database: {os.path.basename(self.db_path)}")
            self.connect_to_database()
    
    def load_tables(self):
        """Load database tables into listbox."""
        try:
            self.tables_listbox.delete(0, tk.END)
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = self.cursor.fetchall()
            
            for table in tables:
                # Get row count for each table
                table_name = table['name']
                self.cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                count = self.cursor.fetchone()['count']
                self.tables_listbox.insert(tk.END, f"{table_name} ({count} rows)")
            
            self.update_status(f"Loaded {len(tables)} tables")
            
        except Exception as e:
            messagebox.showerror("Error", f"Cannot load tables:\n{str(e)}")
    
    def on_table_select(self, event):
        """Handle table selection."""
        selection = self.tables_listbox.curselection()
        if not selection:
            return
        
        table_info = self.tables_listbox.get(selection[0])
        table_name = table_info.split()[0]
        
        self.show_table_schema(table_name)
        self.show_table_preview(table_name)
    
    def show_table_schema(self, table_name):
        """Display table schema."""
        # Clear existing items
        for item in self.schema_tree.get_children():
            self.schema_tree.delete(item)
        
        try:
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            columns = self.cursor.fetchall()
            
            self.table_info_label.config(text=f"Table: {table_name} ({len(columns)} columns)")
            
            for i, col in enumerate(columns):
                self.schema_tree.insert('', 'end', 
                                       values=(col['name'], 
                                               col['type'], 
                                               '✓' if col['pk'] else '',
                                               '✓' if col['notnull'] else '',
                                               col['dflt_value'] if col['dflt_value'] else ''))
            
        except Exception as e:
            messagebox.showerror("Error", f"Cannot load table schema:\n{str(e)}")
    
    def show_table_preview(self, table_name, limit=50):
        """Show preview of table data."""
        # Clear existing items
        for item in self.preview_tree.get_children():
            self.preview_tree.delete(item)
        
        # Clear existing columns
        for col in self.preview_tree['columns']:
            self.preview_tree.heading(col, text='')
            self.preview_tree.column(col, width=0)
        
        try:
            self.cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
            rows = self.cursor.fetchall()
            
            if rows:
                # Get column names
                column_names = [desc[0] for desc in self.cursor.description]
                
                # Configure treeview columns
                self.preview_tree['columns'] = column_names
                self.preview_tree.column('#0', width=0, stretch=tk.NO)
                
                for col in column_names:
                    self.preview_tree.heading(col, text=col, anchor=tk.W)
                    self.preview_tree.column(col, width=100, minwidth=50)
                
                # Insert data
                for row in rows:
                    self.preview_tree.insert('', 'end', values=tuple(row))
            
            self.update_status(f"Showing {len(rows)} rows from {table_name}")
            
        except Exception as e:
            messagebox.showerror("Error", f"Cannot load table data:\n{str(e)}")
    
    def execute_query(self):
        """Execute SQL query from text area."""
        query = self.query_text.get('1.0', 'end-1c').strip()
        
        if not query:
            messagebox.showwarning("Empty Query", "Please enter a SQL query.")
            return
        
        # Clear previous results
        for item in self.results_tree.get_children():
            self.results_tree.delete(item)
        
        for col in self.results_tree['columns']:
            self.results_tree.heading(col, text='')
            self.results_tree.column(col, width=0)
        
        try:
            self.cursor.execute(query)
            
            # Check if it's a SELECT query
            if self.cursor.description:
                rows = self.cursor.fetchall()
                column_names = [desc[0] for desc in self.cursor.description]
                
                # Configure treeview
                self.results_tree['columns'] = column_names
                self.results_tree.column('#0', width=0, stretch=tk.NO)
                
                for i, col in enumerate(column_names):
                    self.results_tree.heading(col, text=col, anchor=tk.W)
                    self.results_tree.column(col, width=100, minwidth=50)
                
                # Insert data
                for row in rows:
                    self.results_tree.insert('', 'end', values=tuple(row))
                
                self.results_info.config(
                    text=f"✓ Query executed successfully. {len(rows)} rows returned.")
                self.update_status(f"Query executed: {len(rows)} rows returned")
                
            else:
                # For INSERT, UPDATE, DELETE
                self.conn.commit()
                affected = self.cursor.rowcount
                self.results_info.config(
                    text=f"✓ Query executed successfully. {affected} rows affected.")
                self.update_status(f"Query executed: {affected} rows affected")
                
        except Exception as e:
            self.results_info.config(text=f"✗ Error: {str(e)}")
            self.update_status(f"Query error: {str(e)}", error=True)
    
    def copy_query(self):
        """Copy query to clipboard."""
        query = self.query_text.get('1.0', 'end-1c')
        self.root.clipboard_clear()
        self.root.clipboard_append(query)
        self.update_status("Query copied to clipboard")
    
    def load_hierarchy(self):
        """Load CFR hierarchy."""
        # Clear existing items
        for item in self.hierarchy_tree.get_children():
            self.hierarchy_tree.delete(item)
        
        try:
            part_filter = self.part_filter.get().strip()
            where_clause = f"WHERE p.part_number LIKE '%{part_filter}%'" if part_filter else ""
            
            query = f"""
            SELECT 
                p.part_number,
                p.title as part_title,
                sp.subpart_letter,
                sp.title as subpart_title,
                s.section_number,
                s.title as section_title,
                ss.subsection_label,
                ss.title as subsection_title,
                p.part_id,
                sp.subpart_id,
                s.section_id,
                ss.subsection_id
            FROM parts p
            LEFT JOIN subparts sp ON p.part_id = sp.part_id
            LEFT JOIN sections s ON sp.subpart_id = s.subpart_id
            LEFT JOIN subsections ss ON s.section_id = ss.section_id
            {where_clause}
            ORDER BY p.part_number, sp.subpart_letter, 
                CAST(SUBSTR(s.section_number, INSTR(s.section_number, '.') + 1) AS INTEGER),
                CASE 
                    WHEN ss.subsection_label GLOB '[0-9]*' THEN CAST(ss.subsection_label AS INTEGER)
                    ELSE LOWER(ss.subsection_label)
                END
            """
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            # Build hierarchy
            part_nodes = {}
            subpart_nodes = {}
            section_nodes = {}
            
            for row in rows:
                part_key = f"part_{row['part_id']}"
                subpart_key = f"subpart_{row['subpart_id']}" if row['subpart_id'] else None
                section_key = f"section_{row['section_id']}" if row['section_id'] else None
                
                # Add part node if not exists
                if part_key not in part_nodes:
                    part_nodes[part_key] = self.hierarchy_tree.insert(
                        '', 'end', 
                        text=f"PART {row['part_number']}",
                        values=('Part', row['part_title'][:100], row['part_id']),
                        tags=('part',)
                    )
                
                # Add subpart node if exists
                if row['subpart_id'] and subpart_key not in subpart_nodes:
                    parent = part_nodes[part_key]
                    subpart_nodes[subpart_key] = self.hierarchy_tree.insert(
                        parent, 'end',
                        text=f"Subpart {row['subpart_letter']}",
                        values=('Subpart', row['subpart_title'][:100], row['subpart_id']),
                        tags=('subpart',)
                    )
                
                # Add section node if exists
                if row['section_id'] and section_key not in section_nodes:
                    parent = subpart_nodes[subpart_key] if subpart_key else part_nodes[part_key]
                    section_nodes[section_key] = self.hierarchy_tree.insert(
                        parent, 'end',
                        text=f"§ {row['section_number']}",
                        values=('Section', row['section_title'][:100], row['section_id']),
                        tags=('section',)
                    )
                
                # Add subsection node if exists
                if row['subsection_label']:
                    parent = section_nodes[section_key] if section_key else (subpart_nodes[subpart_key] if subpart_key else part_nodes[part_key])
                    self.hierarchy_tree.insert(
                        parent, 'end',
                        text=f"({row['subsection_label']})",
                        values=('Subsection', row['subsection_title'][:100], row['subsection_id']),
                        tags=('subsection',)
                    )
            
            # Configure tags for colors
            self.hierarchy_tree.tag_configure('part', background='#e8f4fc')
            self.hierarchy_tree.tag_configure('subpart', background='#f0f8ff')
            self.hierarchy_tree.tag_configure('section', background='#f9f9f9')
            self.hierarchy_tree.tag_configure('subsection', background='#ffffff')
            
            self.update_status(f"Hierarchy loaded: {len(part_nodes)} parts, {len(rows)} total nodes")
            
        except Exception as e:
            messagebox.showerror("Error", f"Cannot load hierarchy:\n{str(e)}")
    
    def expand_all_nodes(self):
        """Expand all nodes in hierarchy tree."""
        for item in self.hierarchy_tree.get_children():
            self.hierarchy_tree.item(item, open=True)
            for child in self.hierarchy_tree.get_children(item):
                self.hierarchy_tree.item(child, open=True)
    
    def collapse_all_nodes(self):
        """Collapse all nodes in hierarchy tree."""
        for item in self.hierarchy_tree.get_children():
            self.hierarchy_tree.item(item, open=False)
    
    def on_hierarchy_double_click(self, event):
        """Handle double-click on hierarchy node."""
        item = self.hierarchy_tree.selection()[0]
        item_type = self.hierarchy_tree.item(item, 'values')[0]
        
        if item_type == 'Part':
            part_id = self.hierarchy_tree.item(item, 'values')[2]
            self.query_text.delete('1.0', tk.END)
            self.query_text.insert('1.0', f"SELECT * FROM parts WHERE part_id = {part_id};")
            self.notebook.select(1)  # Switch to query tab
    
    def perform_search(self):
        """Perform search across database."""
        term = self.search_entry.get().strip()
        search_in = self.search_in_var.get()
        
        if not term:
            messagebox.showwarning("Empty Search", "Please enter a search term.")
            return
        
        # Clear existing items
        for item in self.search_tree.get_children():
            self.search_tree.delete(item)
        
        try:
            if search_in in ['all', 'titles']:
                # Search in sections
                query = """
                SELECT 'section' as type, 
                       s.section_number as identifier, 
                       s.title,
                       SUBSTR(s.content, 1, 200) as snippet,
                       s.section_id
                FROM sections s
                WHERE s.title LIKE ? OR s.content LIKE ?
                LIMIT 50
                """
                self.cursor.execute(query, (f'%{term}%', f'%{term}%'))
                sections = self.cursor.fetchall()
                
                for row in sections:
                    self.search_tree.insert('', 'end', 
                                           values=(row['type'], 
                                                   row['identifier'], 
                                                   row['snippet']),
                                           tags=(f"id_{row['section_id']}",))
            
            if search_in in ['all', 'content']:
                # Search in subsections
                query = """
                SELECT 'subsection' as type, 
                       (SELECT section_number FROM sections WHERE section_id = ss.section_id) || 
                       '(' || ss.subsection_label || ')' as identifier,
                       ss.title,
                       SUBSTR(ss.content, 1, 200) as snippet,
                       ss.subsection_id
                FROM subsections ss
                WHERE ss.title LIKE ? OR ss.content LIKE ?
                LIMIT 50
                """
                self.cursor.execute(query, (f'%{term}%', f'%{term}%'))
                subsections = self.cursor.fetchall()
                
                for row in subsections:
                    self.search_tree.insert('', 'end', 
                                           values=(row['type'], 
                                                   row['identifier'], 
                                                   row['snippet']),
                                           tags=(f"id_{row['subsection_id']}",))
            
            total_items = len(self.search_tree.get_children())
            self.update_status(f"Search complete: Found {total_items} results for '{term}'")
            
        except Exception as e:
            messagebox.showerror("Error", f"Cannot perform search:\n{str(e)}")
    
    def on_search_result_double_click(self, event):
        """Handle double-click on search result."""
        item = self.search_tree.selection()[0]
        item_type = self.search_tree.item(item, 'values')[0]
        identifier = self.search_tree.item(item, 'values')[1]
        
        if item_type == 'section':
            self.query_text.delete('1.0', tk.END)
            self.query_text.insert('1.0', f"SELECT * FROM sections WHERE section_number = '{identifier}';")
            self.notebook.select(1)  # Switch to query tab
    
    def generate_chart(self):
        """Generate visualization chart."""
        chart_type = self.chart_type.get()
        
        if chart_type == "Statistics":
            self.show_statistics()
        else:
            self.show_plot(chart_type)
    
    def show_statistics(self):
        """Display database statistics."""
        self.stats_text.delete('1.0', tk.END)
        
        try:
            stats = []
            
            # Get counts for all tables
            tables = ['parts', 'subparts', 'sections', 'subsections']
            for table in tables:
                self.cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                count = self.cursor.fetchone()['count']
                stats.append(f"{table.capitalize():12}: {count:>6} records")
            
            # Get parts distribution
            self.cursor.execute("""
                SELECT part_number, COUNT(section_id) as section_count
                FROM parts p
                LEFT JOIN sections s ON p.part_id = s.part_id
                GROUP BY p.part_number
                ORDER BY section_count DESC
                LIMIT 10
            """)
            top_parts = self.cursor.fetchall()
            
            stats.append("\n📊 Top Parts by Section Count:")
            stats.append("-" * 40)
            for row in top_parts:
                stats.append(f"Part {row['part_number']:6}: {row['section_count']:>4} sections")
            
            # Get recent updates
            self.cursor.execute("""
                SELECT section_number, title, updated_at
                FROM sections
                ORDER BY updated_at DESC
                LIMIT 5
            """)
            recent = self.cursor.fetchall()
            
            stats.append("\n🕒 Recently Updated Sections:")
            stats.append("-" * 40)
            for row in recent:
                stats.append(f"{row['section_number']:10} - {row['title'][:50]}...")
            
            self.stats_text.insert('1.0', '\n'.join(stats))
            self.update_status("Statistics generated")
            
        except Exception as e:
            self.stats_text.insert('1.0', f"Error generating statistics:\n{str(e)}")
    
    def show_plot(self, chart_type):
        """Generate and display plot."""
        # Clear chart frame
        for widget in self.chart_frame.winfo_children():
            widget.destroy()
        
        try:
            # Get data for plotting
            self.cursor.execute("""
                SELECT p.part_number, COUNT(s.section_id) as count
                FROM parts p
                LEFT JOIN sections s ON p.part_id = s.part_id
                GROUP BY p.part_number
                HAVING count > 0
                ORDER BY p.part_number
            """)
            data = self.cursor.fetchall()
            
            if not data:
                self.stats_text = scrolledtext.ScrolledText(self.chart_frame, height=20)
                self.stats_text.pack(fill=tk.BOTH, expand=True)
                self.stats_text.insert('1.0', "No data available for visualization.")
                return
            
            part_numbers = [row['part_number'] for row in data]
            counts = [row['count'] for row in data]
            
            # Create figure
            fig, ax = plt.subplots(figsize=(10, 6))
            
            if chart_type == "Bar Chart":
                bars = ax.bar(part_numbers, counts, color=sns.color_palette("husl", len(part_numbers)))
                ax.set_xlabel('Part Number')
                ax.set_ylabel('Number of Sections')
                ax.set_title('Sections per CFR Part')
                ax.set_xticklabels(part_numbers, rotation=45, ha='right')
                
                # Add value labels on bars
                for bar in bars:
                    height = bar.get_height()
                    ax.annotate(f'{height}',
                              xy=(bar.get_x() + bar.get_width() / 2, height),
                              xytext=(0, 3),
                              textcoords="offset points",
                              ha='center', va='bottom')
            
            elif chart_type == "Pie Chart":
                ax.pie(counts, labels=part_numbers, autopct='%1.1f%%', startangle=90)
                ax.set_title('Distribution of Sections by Part')
                ax.axis('equal')  # Equal aspect ratio ensures pie is drawn as a circle
            
            elif chart_type == "Line Chart":
                ax.plot(part_numbers, counts, marker='o', linewidth=2)
                ax.set_xlabel('Part Number')
                ax.set_ylabel('Number of Sections')
                ax.set_title('Sections per CFR Part')
                ax.set_xticklabels(part_numbers, rotation=45, ha='right')
                ax.grid(True, alpha=0.3)
            
            # Embed in tkinter
            canvas = FigureCanvasTkAgg(fig, self.chart_frame)
            canvas.draw()
            canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
            
            self.update_status(f"{chart_type} generated")
            
        except Exception as e:
            messagebox.showerror("Error", f"Cannot generate chart:\n{str(e)}")
    
    def browse_export_path(self):
        """Browse for export file location."""
        file_types = {
            "CSV": [("CSV files", "*.csv"), ("All files", "*.*")],
            "JSON": [("JSON files", "*.json"), ("All files", "*.*")],
            "Excel": [("Excel files", "*.xlsx"), ("All files", "*.*")],
            "HTML": [("HTML files", "*.html"), ("All files", "*.*")]
        }
        
        export_format = self.export_format.get()
        default_ext = {"CSV": ".csv", "JSON": ".json", "Excel": ".xlsx", "HTML": ".html"}[export_format]
        
        file_path = filedialog.asksaveasfilename(
            title=f"Export as {export_format}",
            defaultextension=default_ext,
            filetypes=file_types[export_format]
        )
        
        if file_path:
            self.export_path.delete(0, tk.END)
            self.export_path.insert(0, file_path)
    
    def export_data(self):
        """Export data to selected format."""
        table = self.export_table.get()
        file_path = self.export_path.get()
        export_format = self.export_format.get()
        
        if not file_path:
            messagebox.showwarning("No File", "Please select an export location.")
            return
        
        try:
            # Query data
            self.cursor.execute(f"SELECT * FROM {table}")
            rows = self.cursor.fetchall()
            column_names = [desc[0] for desc in self.cursor.description]
            
            # Convert to DataFrame
            data = [dict(zip(column_names, row)) for row in rows]
            df = pd.DataFrame(data)
            
            # Export based on format
            if export_format == "CSV":
                df.to_csv(file_path, index=False)
                message = f"Exported {len(df)} rows to CSV"
            
            elif export_format == "JSON":
                df.to_json(file_path, orient='records', indent=2)
                message = f"Exported {len(df)} rows to JSON"
            
            elif export_format == "Excel":
                df.to_excel(file_path, index=False)
                message = f"Exported {len(df)} rows to Excel"
            
            elif export_format == "HTML":
                df.to_html(file_path, index=False)
                message = f"Exported {len(df)} rows to HTML"
            
            # Update log
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_entry = f"[{timestamp}] {message}\n"
            self.export_log.insert('end', log_entry)
            self.export_log.see('end')
            
            self.update_status(message)
            messagebox.showinfo("Export Successful", message)
            
        except Exception as e:
            error_msg = f"Export failed: {str(e)}"
            self.export_log.insert('end', f"[ERROR] {error_msg}\n")
            self.update_status(error_msg, error=True)
            messagebox.showerror("Export Error", error_msg)
    
    def update_status(self, message: str, error: bool = False):
        """Update status bar with message."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        status_text = f"[{timestamp}] {message}"
        self.status_bar.config(text=status_text)
        
        if error:
            self.status_bar.config(foreground='red')
        else:
            self.status_bar.config(foreground='black')
    
    def on_closing(self):
        """Handle application closing."""
        if self.conn:
            self.conn.close()
        self.root.destroy()


def main():
    """Main function to run the application."""
    root = tk.Tk()
    app = CFRDatabaseViewerGUI(root)
    
    # Handle window closing
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    
    # Start the application
    root.mainloop()


if __name__ == "__main__":
    main()