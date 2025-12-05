import sqlite3
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
import os
import json
import xml.etree.ElementTree as ET
import xml.dom.minidom

class CFRDatabaseViewerGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("CFR Database Explorer & Exporter")
        self.root.geometry("1400x900")
        self.db_path = "16.db"
        self.conn = None
        self.cursor = None
        
        self.setup_styles()
        self.create_widgets()
        self.connect_to_database()

    def setup_styles(self):
        style = ttk.Style()
        style.theme_use('clam')
        self.bg_color = "#f0f5ff"
        self.root.configure(bg=self.bg_color)
        
        style.configure("Treeview", 
                        background="white",
                        foreground="black",
                        rowheight=25,
                        fieldbackground="white",
                        font=('Arial', 10))
        style.map('Treeview', background=[('selected', '#0078D7')])
        
        style.configure("TButton", padding=6, font=('Arial', 10))

    def create_widgets(self):
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Tabs
        self.create_hierarchy_tab()
        self.create_query_tab()
        self.create_search_tab()
        self.create_export_tab()  # <--- NEW TAB

    def connect_to_database(self):
        if not os.path.exists(self.db_path):
            messagebox.showerror("Error", f"Database {self.db_path} not found.\nPlease run the database manager script first.")
            return
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.load_hierarchy()

    # -------------------------------------------------------------------------
    # HIERARCHY TAB
    # -------------------------------------------------------------------------
    def create_hierarchy_tab(self):
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="🌳 Hierarchy & Content")
        
        paned = ttk.PanedWindow(tab, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True)
        
        # Left: Tree
        tree_frame = ttk.Frame(paned)
        paned.add(tree_frame, weight=1)
        
        ttk.Label(tree_frame, text="CFR Structure", font=('Arial', 10, 'bold')).pack(anchor=tk.W, padx=5, pady=2)
        self.hierarchy_tree = ttk.Treeview(tree_frame)
        self.hierarchy_tree.pack(fill=tk.BOTH, expand=True)
        self.hierarchy_tree.heading('#0', text='Part > Section > Paragraph', anchor=tk.W)
        
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.hierarchy_tree.yview)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        self.hierarchy_tree.configure(yscrollcommand=vsb.set)
        
        self.hierarchy_tree.bind('<<TreeviewSelect>>', self.on_tree_select)
        
        # Right: Content
        content_frame = ttk.Frame(paned)
        paned.add(content_frame, weight=2)
        
        self.content_title = ttk.Label(content_frame, text="Select an item...", font=("Arial", 12, "bold"), background="#eee", padding=5)
        self.content_title.pack(fill=tk.X)
        
        self.content_text = scrolledtext.ScrolledText(content_frame, wrap=tk.WORD, font=("Georgia", 11), padx=10, pady=10)
        self.content_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

    def load_hierarchy(self):
        if not self.conn: return
        for item in self.hierarchy_tree.get_children():
            self.hierarchy_tree.delete(item)
            
        try:
            parts = self.cursor.execute("SELECT part_id, part_number, title FROM parts").fetchall()
            for part in parts:
                part_node = self.hierarchy_tree.insert('', 'end', 
                                                     text=f"PART {part['part_number']} - {part['title']}", 
                                                     open=True, values=('part', part['part_id']))
                
                subparts = self.cursor.execute("SELECT subpart_id, subpart_letter, title FROM subparts WHERE part_id=?", 
                                             (part['part_id'],)).fetchall()
                for sp in subparts:
                    sp_text = f"Subpart {sp['subpart_letter']}" if sp['subpart_letter'] != "General" else "General Provisions"
                    sp_node = self.hierarchy_tree.insert(part_node, 'end', 
                                                       text=sp_text, 
                                                       open=True, values=('subpart', sp['subpart_id']))
                    
                    sections = self.cursor.execute("SELECT section_id, section_number, title FROM sections WHERE subpart_id=?", 
                                                 (sp['subpart_id'],)).fetchall()
                    for sec in sections:
                        sec_node = self.hierarchy_tree.insert(sp_node, 'end', 
                                                            text=f"§ {sec['section_number']} {sec['title']}", 
                                                            open=False, values=('section', sec['section_id']))
                        self.load_subsections_recursive(sec['section_id'], sec_node)
        except Exception as e:
            print(f"Error loading hierarchy: {e}")

    def load_subsections_recursive(self, section_id, parent_node):
        rows = self.cursor.execute("""
            SELECT subsection_id, subsection_label, title, parent_subsection_id 
            FROM subsections 
            WHERE section_id = ? 
            ORDER BY subsection_id
        """, (section_id,)).fetchall()
        
        id_map = {}
        for row in rows:
            sub_id = row['subsection_id']
            label = row['subsection_label']
            text = row['title'][:60] + "..." if len(row['title']) > 60 else row['title']
            parent_id = row['parent_subsection_id']
            
            tree_parent = parent_node
            if parent_id and parent_id in id_map:
                tree_parent = id_map[parent_id]
            
            new_node = self.hierarchy_tree.insert(tree_parent, 'end', 
                                                text=f"({label}) {text}", 
                                                values=('subsection', sub_id))
            id_map[sub_id] = new_node

    def on_tree_select(self, event):
        selected_items = self.hierarchy_tree.selection()
        if not selected_items: return
        item = selected_items[0]
        values = self.hierarchy_tree.item(item, 'values')
        if not values: return
        
        type_, db_id = values[0], values[1]
        self.content_text.delete('1.0', tk.END)
        self.content_title.config(text="")
        
        if type_ == 'section':
            data = self.cursor.execute("SELECT title, content FROM sections WHERE section_id=?", (db_id,)).fetchone()
            if data:
                self.content_title.config(text=f"§ {data['title']}")
                self.content_text.insert('1.0', data['content'])
        elif type_ == 'subsection':
            data = self.cursor.execute("""
                SELECT s.subsection_label, s.title, s.content, sec.section_number 
                FROM subsections s
                JOIN sections sec ON s.section_id = sec.section_id
                WHERE s.subsection_id=?
            """, (db_id,)).fetchone()
            if data:
                self.content_title.config(text=f"§ {data['section_number']} ({data['subsection_label']})")
                self.content_text.insert('1.0', data['content'])

    # -------------------------------------------------------------------------
    # EXPORT TAB (NEW)
    # -------------------------------------------------------------------------
    def create_export_tab(self):
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="💾 Export Data")
        
        frame = ttk.Frame(tab, padding=20)
        frame.pack(fill=tk.BOTH, expand=True)
        
        ttk.Label(frame, text="Export Database to File", font=("Arial", 14, "bold")).pack(pady=10)
        ttk.Label(frame, text="Generate a structured file containing the full hierarchy (Parts > Sections > Subsections) for external use.").pack(pady=5)
        
        btn_frame = ttk.Frame(frame)
        btn_frame.pack(pady=20)
        
        ttk.Button(btn_frame, text="Export as JSON", command=self.export_json).pack(side=tk.LEFT, padx=10)
        ttk.Button(btn_frame, text="Export as XML", command=self.export_xml).pack(side=tk.LEFT, padx=10)
        
        self.export_status = ttk.Label(frame, text="", foreground="green")
        self.export_status.pack(pady=10)

    def _build_full_hierarchy_dict(self):
        """Helper to build a nested dictionary of the entire DB."""
        data = {"title": "CFR Regulations", "parts": []}
        
        parts = self.cursor.execute("SELECT part_id, part_number, title FROM parts").fetchall()
        for part in parts:
            part_data = {
                "part_number": part["part_number"],
                "title": part["title"],
                "subparts": []
            }
            
            subparts = self.cursor.execute("SELECT subpart_id, subpart_letter, title FROM subparts WHERE part_id=?", (part["part_id"],)).fetchall()
            for sp in subparts:
                sp_data = {
                    "letter": sp["subpart_letter"],
                    "title": sp["title"],
                    "sections": []
                }
                
                sections = self.cursor.execute("SELECT section_id, section_number, title, content FROM sections WHERE subpart_id=?", (sp["subpart_id"],)).fetchall()
                for sec in sections:
                    sec_data = {
                        "number": sec["section_number"],
                        "title": sec["title"],
                        "intro_text": sec["content"],
                        "subsections": self._get_nested_subsections(sec["section_id"])
                    }
                    sp_data["sections"].append(sec_data)
                
                part_data["subparts"].append(sp_data)
            
            data["parts"].append(part_data)
        return data

    def _get_nested_subsections(self, section_id):
        """Build nested subsections for JSON."""
        rows = self.cursor.execute("SELECT subsection_id, subsection_label, content, parent_subsection_id FROM subsections WHERE section_id=? ORDER BY subsection_id", (section_id,)).fetchall()
        
        nodes = {}
        roots = []
        
        # Create all node objects
        for row in rows:
            node = {
                "label": row["subsection_label"],
                "text": row["content"],
                "children": []
            }
            nodes[row["subsection_id"]] = node
            
            if row["parent_subsection_id"]:
                parent = nodes.get(row["parent_subsection_id"])
                if parent:
                    parent["children"].append(node)
                else:
                    roots.append(node) # Orphan fallback
            else:
                roots.append(node)
                
        return roots

    def export_json(self):
        file_path = filedialog.asksaveasfilename(defaultextension=".json", filetypes=[("JSON files", "*.json")])
        if not file_path: return
        
        try:
            data = self._build_full_hierarchy_dict()
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            self.export_status.config(text=f"Successfully exported to {os.path.basename(file_path)}")
            messagebox.showinfo("Success", "JSON Export Complete!")
        except Exception as e:
            messagebox.showerror("Export Error", str(e))

    def export_xml(self):
        file_path = filedialog.asksaveasfilename(defaultextension=".xml", filetypes=[("XML files", "*.xml")])
        if not file_path: return
        
        try:
            data = self._build_full_hierarchy_dict()
            root = ET.Element("regulation")
            
            for part in data["parts"]:
                p_elem = ET.SubElement(root, "part", number=part["part_number"], title=part["title"])
                for sp in part["subparts"]:
                    sp_elem = ET.SubElement(p_elem, "subpart", letter=sp["letter"], title=sp["title"])
                    for sec in sp["sections"]:
                        sec_elem = ET.SubElement(sp_elem, "section", number=sec["number"], title=sec["title"])
                        # Add intro text as CDATA-like text
                        text_elem = ET.SubElement(sec_elem, "text")
                        text_elem.text = sec["intro_text"]
                        
                        # Recursive XML builder for subsections
                        self._build_xml_subsections(sec_elem, sec["subsections"])
            
            # Pretty print
            xml_str = ET.tostring(root, encoding='utf-8')
            parsed = xml.dom.minidom.parseString(xml_str)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(parsed.toprettyxml(indent="  "))
                
            self.export_status.config(text=f"Successfully exported to {os.path.basename(file_path)}")
            messagebox.showinfo("Success", "XML Export Complete!")
        except Exception as e:
            messagebox.showerror("Export Error", str(e))

    def _build_xml_subsections(self, parent_elem, subsections):
        for sub in subsections:
            sub_elem = ET.SubElement(parent_elem, "paragraph", label=sub["label"])
            text_elem = ET.SubElement(sub_elem, "text")
            text_elem.text = sub["text"]
            if sub["children"]:
                self._build_xml_subsections(sub_elem, sub["children"])

    # -------------------------------------------------------------------------
    # QUERY & SEARCH TABS (Existing)
    # -------------------------------------------------------------------------
    def create_query_tab(self):
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="🔍 SQL Query")
        self.query_text = scrolledtext.ScrolledText(tab, height=5)
        self.query_text.pack(fill=tk.X, padx=5, pady=5)
        self.query_text.insert('1.0', "SELECT * FROM subsections WHERE level = 1 LIMIT 20;")
        ttk.Button(tab, text="Execute", command=self.execute_query).pack(pady=5)
        self.results_tree = ttk.Treeview(tab)
        self.results_tree.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

    def execute_query(self):
        query = self.query_text.get('1.0', tk.END).strip()
        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            if not rows:
                messagebox.showinfo("Result", "No rows returned.")
                return
            cols = [d[0] for d in self.cursor.description]
            self.results_tree.delete(*self.results_tree.get_children())
            self.results_tree['columns'] = cols
            self.results_tree.column("#0", width=0, stretch=tk.NO)
            for col in cols: self.results_tree.heading(col, text=col)
            for row in rows: self.results_tree.insert("", "end", values=list(row))
        except Exception as e: messagebox.showerror("SQL Error", str(e))

    def create_search_tab(self):
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="🔎 Search Text")
        frame = ttk.Frame(tab)
        frame.pack(fill=tk.X, padx=5, pady=5)
        self.search_entry = ttk.Entry(frame, width=50)
        self.search_entry.pack(side=tk.LEFT, padx=5)
        ttk.Button(frame, text="Search", command=self.perform_search).pack(side=tk.LEFT)
        self.search_results_list = tk.Listbox(tab)
        self.search_results_list.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

    def perform_search(self):
        term = self.search_entry.get()
        self.search_results_list.delete(0, tk.END)
        subs = self.cursor.execute("SELECT subsection_label, content FROM subsections WHERE content LIKE ?", (f'%{term}%',)).fetchall()
        for s in subs:
            self.search_results_list.insert(tk.END, f"({s['subsection_label']}): {s['content'][:80]}...")

def main():
    root = tk.Tk()
    app = CFRDatabaseViewerGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()