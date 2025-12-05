import sqlite3
import re
import os
from typing import List, Dict, Optional, Any

class CFRDatabaseManager:
    def __init__(self, db_path: str = "16.db"):
        """Initialize the database manager with schema creation."""
        self.db_path = db_path
        # Delete old db to ensure fresh schema and data
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
                print(f"Removed old database: {db_path}")
            except:
                pass
                
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.create_schema()
    
    def create_schema(self):
        """Create the database schema for CFR document hierarchy."""
        
        # Parts table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS parts (
                part_id INTEGER PRIMARY KEY AUTOINCREMENT,
                part_number TEXT NOT NULL,
                title TEXT NOT NULL,
                UNIQUE(part_number)
            )
        """)
        
        # Subparts table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS subparts (
                subpart_id INTEGER PRIMARY KEY AUTOINCREMENT,
                part_id INTEGER NOT NULL,
                subpart_letter TEXT,
                title TEXT NOT NULL,
                FOREIGN KEY (part_id) REFERENCES parts(part_id),
                UNIQUE(part_id, subpart_letter)
            )
        """)
        
        # Sections table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS sections (
                section_id INTEGER PRIMARY KEY AUTOINCREMENT,
                subpart_id INTEGER NOT NULL,
                section_number TEXT NOT NULL,
                title TEXT NOT NULL,
                content TEXT,
                FOREIGN KEY (subpart_id) REFERENCES subparts(subpart_id),
                UNIQUE(section_number)
            )
        """)
        
        # Subsections table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS subsections (
                subsection_id INTEGER PRIMARY KEY AUTOINCREMENT,
                section_id INTEGER NOT NULL,
                subsection_label TEXT NOT NULL,
                title TEXT,
                content TEXT,
                level INTEGER DEFAULT 1,
                parent_subsection_id INTEGER,
                FOREIGN KEY (section_id) REFERENCES sections(section_id),
                FOREIGN KEY (parent_subsection_id) REFERENCES subsections(subsection_id)
            )
        """)
        
        self.conn.commit()
    
    def clean_text(self, text: str) -> str:
        """Remove markdown links, html tags, and formatting."""
        # Remove HTML tags (e.g., <span id...>)
        text = re.sub(r'<[^>]+>', '', text)
        # Remove Markdown links [text](url) -> text
        text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
        # Remove Bold **text** -> text
        text = text.replace('**', '')
        return text.strip()

    def parse_markdown_file(self, md_file_path: str):
        """Parse the markdown file and extract CFR structure."""
        print(f"Reading file: {md_file_path}")
        with open(md_file_path, 'r', encoding='utf-8') as f:
            raw_content = f.read()
        
        # Clean the content globally first
        content = self.clean_text(raw_content)
        
        print("Parsing document structure...")
        
        # 1. Find Part
        # Matches "PART 312"
        part_match = re.search(r'PART\s+(\d+)[—\-](.+)', content, re.IGNORECASE)
        
        if part_match:
            part_number = part_match.group(1).strip()
            part_title = part_match.group(2).strip()
            
            print(f"Found Part {part_number}: {part_title}")
            part_id = self.insert_part(part_number, part_title)
            
            if part_id:
                # 2. Try to find Subparts
                subparts_found = self.parse_subparts(content, part_id)
                
                # 3. If no subparts found or just sections, parse sections
                if subparts_found == 0:
                    print("No explicit subparts found. Creating default 'General' subpart.")
                    default_subpart_id = self.insert_subpart(part_id, "General", "General Provisions")
                    self.parse_sections(content, default_subpart_id)
        else:
            print("Error: Could not find 'PART' header in the document.")

    def insert_part(self, part_number: str, title: str) -> int:
        self.cursor.execute("INSERT OR REPLACE INTO parts (part_number, title) VALUES (?, ?)", 
                          (part_number, title))
        self.conn.commit()
        self.cursor.execute("SELECT part_id FROM parts WHERE part_number = ?", (part_number,))
        return self.cursor.fetchone()[0]

    def insert_subpart(self, part_id: int, subpart_letter: str, title: str) -> int:
        self.cursor.execute("INSERT OR REPLACE INTO subparts (part_id, subpart_letter, title) VALUES (?, ?, ?)", 
                          (part_id, subpart_letter, title))
        self.conn.commit()
        self.cursor.execute("SELECT subpart_id FROM subparts WHERE part_id = ? AND subpart_letter = ?", 
                          (part_id, subpart_letter))
        return self.cursor.fetchone()[0]

    def parse_subparts(self, content: str, part_id: int) -> int:
        """Parse subparts. Returns number of subparts found."""
        # In this specific file, subparts aren't explicitly defined as "Subpart A".
        # If your file *does* have them later, this regex finds them.
        subpart_matches = list(re.finditer(r'Subpart\s+([A-Z])', content))
        
        if not subpart_matches:
            return 0
            
        print(f"Found {len(subpart_matches)} subparts")
        for i, match in enumerate(subpart_matches):
            subpart_letter = match.group(1)
            
            line_end = content.find('\n', match.end())
            title = content[match.end():line_end].strip()
            
            subpart_id = self.insert_subpart(part_id, subpart_letter, title)
            
            start_pos = match.end()
            end_pos = subpart_matches[i+1].start() if i + 1 < len(subpart_matches) else len(content)
            subpart_content = content[start_pos:end_pos]
            
            self.parse_sections(subpart_content, subpart_id)
            
        return len(subpart_matches)

    def parse_sections(self, content: str, subpart_id: int):
        """Parse sections (e.g., § 312.1)."""
        # Regex for section headers: § 312.5 Title.
        # Handles newlines immediately after section number
        section_pattern = r'§\s*(\d+\.\d+)\s+(.+?)(?=\n|§|$)'
        
        matches = list(re.finditer(section_pattern, content))
        print(f"  Found {len(matches)} sections")
        
        for i, match in enumerate(matches):
            sec_num = match.group(1).strip()
            sec_title = match.group(2).strip()
            
            start_pos = match.end()
            end_pos = matches[i+1].start() if i + 1 < len(matches) else len(content)
            
            # Stop if we hit a new Part header
            next_big_header = re.search(r'PART\s+\d+', content[start_pos:])
            if next_big_header:
                end_pos = min(end_pos, start_pos + next_big_header.start())
            
            sec_content = content[start_pos:end_pos].strip()
            
            self.cursor.execute("INSERT OR REPLACE INTO sections (subpart_id, section_number, title, content) VALUES (?, ?, ?, ?)",
                              (subpart_id, sec_num, sec_title, sec_content))
            self.conn.commit()
            
            section_id = self.cursor.lastrowid
            self.parse_subsections(sec_content, section_id)

    def parse_subsections(self, content: str, section_id: int):
        """
        Parse nested subsections.
        Strict Hierarchy: (a) -> (1) -> (i)
        """
        lines = content.split('\n')
        
        # Regex to find list items: 
        # Optional bullet [-*]
        # Open paren \(
        # Label [stuff]
        # Close paren \)
        item_pattern = r'^\s*(?:[-*]\s+)?\(([a-zA-Z0-9]+)\)\s*(.*)'
        
        # Track the ID of the last item at each level
        # Level 1: (a) -> id
        # Level 2: (1) -> id
        last_ids = {0: None} 
        
        for line in lines:
            line = line.strip()
            if not line: continue
                
            match = re.match(item_pattern, line)
            
            if match:
                label = match.group(1)
                text = match.group(2).strip()
                
                # Determine level based on the strict rule
                level = self.determine_level(label)
                
                if level > 0:
                    # Determine Parent
                    # Ideally, parent is at level - 1
                    # If level 2 (number), parent is level 1 (letter)
                    parent_id = None
                    
                    # Look for the immediate parent level
                    if (level - 1) in last_ids:
                        parent_id = last_ids[level - 1]
                    # Fallback: if structure skips a level (rare but possible in bad formatting)
                    elif (level - 2) in last_ids:
                         parent_id = last_ids[level - 2]

                    # Insert
                    self.cursor.execute("""
                        INSERT INTO subsections (section_id, subsection_label, title, content, level, parent_subsection_id)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (section_id, label, text[:50], text, level, parent_id))
                    
                    inserted_id = self.cursor.lastrowid
                    
                    # Update tracking
                    last_ids[level] = inserted_id
                    
                    # Clear deeper levels (if we start a new (a), clear (1) and (i) history)
                    for k in list(last_ids.keys()):
                        if k > level:
                            del last_ids[k]
                            
                else:
                    # Label didn't match strict rules, treat as continuation text
                    self.append_to_last(last_ids, line)
            else:
                # No label, append to last active subsection
                self.append_to_last(last_ids, line)
                
        self.conn.commit()

    def append_to_last(self, last_ids, text):
        """Appends text to the deepest active subsection."""
        if not last_ids: return
        
        # Get deepest level
        current_level = max(last_ids.keys())
        current_id = last_ids[current_level]
        
        if current_id is None: return 

        self.cursor.execute("SELECT content FROM subsections WHERE subsection_id = ?", (current_id,))
        res = self.cursor.fetchone()
        if res:
            new_content = res[0] + "\n" + text
            self.cursor.execute("UPDATE subsections SET content = ? WHERE subsection_id = ?", 
                              (new_content, current_id))

    def determine_level(self, label: str) -> int:
        """
        Strict CFR Hierarchy Logic:
        Level 1: (a), (b), (c) ... (Standard CFR skips 'i' in Level 1)
        Level 2: (1), (2), (3) ...
        Level 3: (i), (ii), (iii), (iv) ...
        Level 4: (A), (B), (C) ...
        """
        # 1. Check for Numbers -> Level 2
        if re.match(r'^\d+$', label):
            return 2

        # 2. Check for Roman Numerals -> Level 3
        # We explicitly assume 'i', 'v', 'x' are Romans here because CFR Level 1 skips 'i'
        if re.match(r'^(i|ii|iii|iv|v|vi|vii|viii|ix|x|xi|xii|xiii|xiv|xv)$', label, re.IGNORECASE):
            return 3

        # 3. Check for Lowercase Letters -> Level 1
        if re.match(r'^[a-z]$', label):
            # Double check it's not a roman numeral masquerading (handled above, but 'v' is tricky)
            # In CFR, 'v' is a Level 1 letter ONLY if we are way down the alphabet, but usually it's Roman.
            # Given user constraint (a)->(1)->(i), we prioritize Roman for i, v.
            if label in ['i', 'v', 'x']:
                return 3
            return 1

        # 4. Check for Uppercase Letters -> Level 4
        if re.match(r'^[A-Z]$', label):
            # Exclude I, V, X if standard uppercase roman usage, but usually CFR uses (A), (B)
            return 4
        
        return 0

    def close(self):
        self.conn.close()

if __name__ == "__main__":
    db_manager = CFRDatabaseManager("16.db")
    # Assuming '16crf.md' is in the current directory
    if os.path.exists("16crf.md"):
        db_manager.parse_markdown_file("16crf.md")
        db_manager.close()
        print("Database created successfully: 16.db")
    else:
        print("File 16crf.md not found.")