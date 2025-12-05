import sqlite3
import re
import os
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Any
import json

class CFRDatabaseManager:
    def __init__(self, db_path: str = "cfr_regulations.db"):
        """Initialize the database manager with schema creation."""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.create_schema()
    
    def create_schema(self):
        """Create the database schema for CFR document hierarchy."""
        
        # Main Parts table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS parts (
                part_id INTEGER PRIMARY KEY AUTOINCREMENT,
                part_number TEXT NOT NULL,
                title TEXT NOT NULL,
                authority TEXT,
                source TEXT,
                effective_date TEXT,
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
                FOREIGN KEY (parent_subsection_id) REFERENCES subsections(subsection_id),
                UNIQUE(section_id, subsection_label)
            )
        """)
        
        # Create indexes for better query performance
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_sections_subpart ON sections(subpart_id)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_subsections_section ON subsections(section_id)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_subsections_parent ON subsections(parent_subsection_id)")
        
        self.conn.commit()
    
    def parse_markdown_file(self, md_file_path: str):
        """Parse the markdown file and extract CFR structure."""
        print(f"Reading file: {md_file_path}")
        with open(md_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        print("Parsing document structure...")
        
        # Parse Part information - look for PART header
        part_match = re.search(r'#\s*\*\*PART\s+(\d+)[—\-](.+?)\*\*', content)
        if not part_match:
            # Try alternative pattern
            part_match = re.search(r'PART\s+(\d+)[—\-]\s*REGULATIONS', content, re.IGNORECASE)
        
        if part_match:
            part_number = part_match.group(1)
            part_title = part_match.group(2).strip() if len(part_match.groups()) > 1 else "Regulations S-P, S-AM, and S-ID"
            print(f"Found Part {part_number}: {part_title}")
            
            # Extract authority
            authority_match = re.search(r'\*\*Authority:\*\*\s*(.+?)(?=\*\*Source:|\n\*\*|\n#)', content, re.DOTALL | re.IGNORECASE)
            authority = authority_match.group(1).strip() if authority_match else None
            
            # Extract source
            source_match = re.search(r'\*\*Source:\*\*\s*(.+?)(?=\*\*Editorial|\n\*\*|\n#)', content, re.DOTALL | re.IGNORECASE)
            source = source_match.group(1).strip() if source_match else None
            
            part_id = self.insert_part(part_number, part_title, authority, source)
            
            if part_id:
                # Parse Subparts
                self.parse_subparts(content, part_id)
        else:
            print("Warning: No part header found. Creating generic part.")
            part_id = self.insert_part("248", "Regulations S-P, S-AM, and S-ID", None, None)
            if part_id:
                self.parse_subparts(content, part_id)
    
    def insert_part(self, part_number: str, title: str, authority: Optional[str] = None, 
                    source: Optional[str] = None) -> int:
        """Insert a part and return its ID."""
        try:
            self.cursor.execute("""
                INSERT OR REPLACE INTO parts (part_number, title, authority, source)
                VALUES (?, ?, ?, ?)
            """, (part_number, title, authority, source))
            self.conn.commit()
            
            self.cursor.execute("SELECT part_id FROM parts WHERE part_number = ?", (part_number,))
            result = self.cursor.fetchone()
            part_id = result[0] if result else None
            print(f"  Inserted Part {part_number} with ID: {part_id}")
            return part_id
        except Exception as e:
            print(f"Error inserting part {part_number}: {e}")
            return None
    
    def parse_subparts(self, content: str, part_id: int):
        """Parse subparts from the content."""
        # Look for subpart patterns
        subpart_patterns = [
            r'\*\*Subpart\s+([A-Z])\*\*\s*\[?(.+?)\]?(?=\n|\*\*§|\*\*\s*\*\*|$)',
            r'##\s*\*\*Subpart\s+([A-Z])\*\*\s*\[?(.+?)\]?(?=\n|##|\*\*§)',
            r'Subpart\s+([A-Z])\s*[—\-]\s*(.+?)(?=\n|Subpart|\*\*§)'
        ]
        
        subparts_found = []
        
        for pattern in subpart_patterns:
            matches = list(re.finditer(pattern, content, re.DOTALL | re.IGNORECASE))
            if matches:
                subparts_found = matches
                break
        
        print(f"Found {len(subparts_found)} subparts")
        
        for i, match in enumerate(subparts_found):
            subpart_letter = match.group(1)
            subpart_title = match.group(2).strip()
            
            # Clean up title
            subpart_title = re.sub(r'\[.*?\]', '', subpart_title).strip()
            subpart_title = re.sub(r'\*\*', '', subpart_title).strip()
            
            print(f"  Parsing Subpart {subpart_letter}: {subpart_title}")
            
            subpart_id = self.insert_subpart(part_id, subpart_letter, subpart_title)
            
            if subpart_id:
                # Find the content between this subpart and the next
                start_pos = match.end()
                if i + 1 < len(subparts_found):
                    next_match = subparts_found[i + 1]
                    end_pos = next_match.start()
                else:
                    # Look for end of document
                    end_pos = len(content)
                
                subpart_content = content[start_pos:end_pos]
                
                # Parse sections within this subpart
                sections_count = self.parse_sections(subpart_content, subpart_id)
                print(f"    Found {sections_count} sections in Subpart {subpart_letter}")
    
    def insert_subpart(self, part_id: int, subpart_letter: str, title: str) -> int:
        """Insert a subpart and return its ID."""
        try:
            self.cursor.execute("""
                INSERT OR REPLACE INTO subparts (part_id, subpart_letter, title)
                VALUES (?, ?, ?)
            """, (part_id, subpart_letter, title))
            self.conn.commit()
            
            self.cursor.execute("""
                SELECT subpart_id FROM subparts 
                WHERE part_id = ? AND subpart_letter = ?
            """, (part_id, subpart_letter))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            print(f"Error inserting subpart {subpart_letter}: {e}")
            return None
    
    def parse_sections(self, content: str, subpart_id: int) -> int:
        """Parse sections from subpart content. Returns count of sections found."""
        # Pattern for sections - handle various formats
        section_patterns = [
            r'\*\*§\s*([\d\.]+)\*\*\s*\[?(.+?)\]?(?=[\.\n])',
            r'§\s*([\d\.]+)\s*[—\-]\s*(.+?)(?=\n|\(a\)|\*\*§)',
            r'###\s*\*\*§\s*([\d\.]+)\*\*\s*(.+?)(?=\n|###)',
            r'\*\*§\s*([\d\.]+)\*\*\s*(.+?)(?=\n|\(a\)|\*\*§)'
        ]
        
        sections = []
        
        for pattern in section_patterns:
            matches = list(re.finditer(pattern, content, re.DOTALL))
            if matches:
                sections = matches
                break
        
        for i, match in enumerate(sections):
            section_number = match.group(1).strip()
            section_title = match.group(2).strip()
            
            # Clean up title
            section_title = re.sub(r'\[.*?\]', '', section_title).strip()
            section_title = re.sub(r'\*\*', '', section_title).strip()
            
            # Find content between this section and the next
            start_pos = match.end()
            if i + 1 < len(sections):
                next_match = sections[i + 1]
                end_pos = next_match.start()
            else:
                # Look for next major heading or end of subpart
                next_major = re.search(r'\n#{1,3}\s+\*\*', content[start_pos:])
                end_pos = start_pos + (next_major.start() if next_major else 0)
            
            section_content = content[start_pos:end_pos].strip()
            
            # Insert the section
            section_id = self.insert_section(subpart_id, section_number, section_title, section_content)
            
            if section_id:
                # Parse subsections
                self.parse_subsections(section_content, section_id)
        
        return len(sections)
    
    def insert_section(self, subpart_id: int, section_number: str, title: str, 
                       content: str) -> Optional[int]:
        """Insert a section and return its ID."""
        try:
            self.cursor.execute("""
                INSERT OR REPLACE INTO sections (subpart_id, section_number, title, content)
                VALUES (?, ?, ?, ?)
            """, (subpart_id, section_number, title, content))
            self.conn.commit()
            
            self.cursor.execute("SELECT section_id FROM sections WHERE section_number = ?", 
                              (section_number,))
            result = self.cursor.fetchone()
            if result:
                print(f"    Inserted § {section_number}: {title[:50]}...")
                return result[0]
            return None
        except Exception as e:
            print(f"Error inserting section {section_number}: {e}")
            return None
    
    def parse_subsections(self, content: str, section_id: int):
        """Parse subsections from section content."""
        # Pattern for subsections like "(a) Scope." or "(1) General rule."
        # Handle various numbering formats: (a), (1), (i), (A)
        subsection_pattern = r'\(([a-z0-9]+|[ivxlcdm]+)\)\s+\*?([^\.\n\(]+?)(?:\.|\*)(?:\s+(.+?))?(?=\n\([a-z0-9]+\)|\n\s*\*?\*?\s*\(|\Z)'
        
        matches = list(re.finditer(subsection_pattern, content, re.DOTALL | re.IGNORECASE))
        
        for match in matches:
            label = match.group(1).strip()
            title_part = match.group(2).strip() if match.group(2) else ""
            content_part = match.group(3).strip() if match.group(3) else ""
            
            # Clean up title
            title = title_part
            if title.endswith('.'):
                title = title[:-1]
            
            # Clean up content
            subsection_content = content_part
            
            # Insert the subsection
            if title:  # Only insert if we have a title
                self.insert_subsection(section_id, label, title, subsection_content)
    
    def insert_subsection(self, section_id: int, label: str, title: str, 
                         content: str, level: int = 1, parent_id: Optional[int] = None):
        """Insert a subsection."""
        try:
            self.cursor.execute("""
                INSERT OR REPLACE INTO subsections 
                (section_id, subsection_label, title, content, level, parent_subsection_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (section_id, label, title, content, level, parent_id))
            self.conn.commit()
        except Exception as e:
            print(f"Error inserting subsection ({label}): {e}")
    
    def query_sections_by_subpart(self, subpart_letter: str) -> List[Dict]:
        """Query all sections in a subpart."""
        query = """
            SELECT s.section_number, s.title, s.content
            FROM sections s
            JOIN subparts sp ON s.subpart_id = sp.subpart_id
            WHERE sp.subpart_letter = ?
            ORDER BY 
                CAST(SUBSTR(s.section_number, INSTR(s.section_number, '.') + 1) AS INTEGER)
        """
        self.cursor.execute(query, (subpart_letter,))
        columns = [desc[0] for desc in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
    
    def query_subsections_by_section(self, section_number: str) -> List[Dict]:
        """Query all subsections of a section."""
        query = """
            SELECT subsection_label, title, content, level
            FROM subsections
            WHERE section_id = (
                SELECT section_id FROM sections WHERE section_number = ?
            )
            ORDER BY 
                CASE 
                    WHEN subsection_label GLOB '[0-9]*' THEN CAST(subsection_label AS INTEGER)
                    ELSE LOWER(subsection_label)
                END
        """
        self.cursor.execute(query, (section_number,))
        columns = [desc[0] for desc in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
    
    def search_content(self, search_term: str) -> List[Dict]:
        """Search for content across all tables."""
        results = []
        
        # Search in sections
        self.cursor.execute("""
            SELECT 'section' as type, section_number as identifier, title, 
                   SUBSTR(content, 1, 200) as snippet
            FROM sections
            WHERE title LIKE ? OR content LIKE ?
            LIMIT 20
        """, (f'%{search_term}%', f'%{search_term}%'))
        
        columns = [desc[0] for desc in self.cursor.description]
        results.extend([dict(zip(columns, row)) for row in self.cursor.fetchall()])
        
        # Search in subsections
        self.cursor.execute("""
            SELECT 'subsection' as type, 
                   (SELECT section_number FROM sections WHERE section_id = ss.section_id) || 
                   '(' || ss.subsection_label || ')' as identifier,
                   ss.title, 
                   SUBSTR(ss.content, 1, 200) as snippet
            FROM subsections ss
            WHERE ss.title LIKE ? OR ss.content LIKE ?
            LIMIT 20
        """, (f'%{search_term}%', f'%{search_term}%'))
        
        results.extend([dict(zip(columns, row)) for row in self.cursor.fetchall()])
        
        return results
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics."""
        stats = {}
        
        self.cursor.execute("SELECT COUNT(*) FROM parts")
        stats['parts'] = self.cursor.fetchone()[0]
        
        self.cursor.execute("SELECT COUNT(*) FROM subparts")
        stats['subparts'] = self.cursor.fetchone()[0]
        
        self.cursor.execute("SELECT COUNT(*) FROM sections")
        stats['sections'] = self.cursor.fetchone()[0]
        
        self.cursor.execute("SELECT COUNT(*) FROM subsections")
        stats['subsections'] = self.cursor.fetchone()[0]
        
        return stats
    
    def get_hierarchy_view(self) -> str:
        """Get a formatted view of the entire hierarchy."""
        output = []
        
        self.cursor.execute("SELECT part_number, title FROM parts")
        parts = self.cursor.fetchall()
        
        for part in parts:
            part_number, part_title = part
            output.append(f"\n{'='*80}")
            output.append(f"PART {part_number}: {part_title}")
            output.append(f"{'='*80}")
            
            self.cursor.execute("""
                SELECT subpart_letter, title 
                FROM subparts 
                WHERE part_id = (SELECT part_id FROM parts WHERE part_number = ?)
                ORDER BY subpart_letter
            """, (part_number,))
            subparts = self.cursor.fetchall()
            
            for subpart_letter, subpart_title in subparts:
                output.append(f"\n  SUBPART {subpart_letter}: {subpart_title}")
                
                self.cursor.execute("""
                    SELECT section_number, title 
                    FROM sections 
                    WHERE subpart_id = (
                        SELECT subpart_id FROM subparts 
                        WHERE part_id = (SELECT part_id FROM parts WHERE part_number = ?) 
                        AND subpart_letter = ?
                    )
                    ORDER BY 
                        CAST(SUBSTR(section_number, INSTR(section_number, '.') + 1) AS INTEGER)
                """, (part_number, subpart_letter))
                sections = self.cursor.fetchall()
                
                for section_number, section_title in sections:
                    output.append(f"    § {section_number} - {section_title}")
                    
                    self.cursor.execute("""
                        SELECT subsection_label, title 
                        FROM subsections 
                        WHERE section_id = (
                            SELECT section_id FROM sections WHERE section_number = ?
                        )
                        ORDER BY 
                            CASE 
                                WHEN subsection_label GLOB '[0-9]*' THEN CAST(subsection_label AS INTEGER)
                                ELSE LOWER(subsection_label)
                            END
                        LIMIT 5
                    """, (section_number,))
                    subsections = self.cursor.fetchall()
                    
                    for subsection_label, subsection_title in subsections:
                        output.append(f"      ({subsection_label}) {subsection_title}")
                    
                    if subsections:
                        self.cursor.execute("""
                            SELECT COUNT(*) FROM subsections 
                            WHERE section_id = (
                                SELECT section_id FROM sections WHERE section_number = ?
                            )
                        """, (section_number,))
                        count = self.cursor.fetchone()[0]
                        if count > 5:
                            output.append(f"      ... and {count - 5} more subsections")
        
        return '\n'.join(output)
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


def main():
    """Main execution function."""
    print("=" * 80)
    print("CFR DATABASE MANAGER")
    print("=" * 80)
    
    # Ask for markdown file
    print("\nPlease provide the path to your markdown file:")
    print("(Example: doc1.md or /path/to/your/file.md)")
    md_file = input("Markdown file path: ").strip()
    
    if not os.path.exists(md_file):
        print(f"\nError: File '{md_file}' not found!")
        print("Please make sure the file exists and try again.")
        return
    
    # Ask for database file
    print("\nPlease provide the path for the database file:")
    print("(Press Enter for default: 'cfr_regulations.db')")
    db_file = input("Database file path: ").strip()
    
    if not db_file:
        db_file = "cfr_regulations.db"
    
    try:
        # Initialize database
        print(f"\nCreating/opening database at: {db_file}")
        db = CFRDatabaseManager(db_file)
        
        # Parse the markdown file
        print(f"\nParsing markdown file: {md_file}")
        db.parse_markdown_file(md_file)
        
        # Get statistics
        stats = db.get_statistics()
        print("\n" + "=" * 80)
        print("DATABASE STATISTICS")
        print("=" * 80)
        print(f"Parts: {stats['parts']}")
        print(f"Subparts: {stats['subparts']}")
        print(f"Sections: {stats['sections']}")
        print(f"Subsections: {stats['subsections']}")
        
        # Display hierarchy
        print("\n" + "=" * 80)
        print("DOCUMENT HIERARCHY OVERVIEW")
        print("=" * 80)
        print(db.get_hierarchy_view())
        
        # Example queries
        print("\n" + "=" * 80)
        print("EXAMPLE QUERIES")
        print("=" * 80)
        
        # Query sections in Subpart C
        print("\n1. Sections in Subpart C:")
        sections = db.query_sections_by_subpart('C')
        if sections:
            for section in sections:
                print(f"   § {section['section_number']} - {section['title'][:60]}...")
        else:
            print("   No sections found in Subpart C")
        
        # Query subsections of a specific section
        print("\n2. Subsections of § 248.202:")
        subsections = db.query_subsections_by_section('248.202')
        if subsections:
            for subsec in subsections:
                title_display = subsec['title'][:40] + "..." if len(subsec['title']) > 40 else subsec['title']
                print(f"   ({subsec['subsection_label']}) {title_display}")
        else:
            print("   No subsections found for § 248.202")
        
        # Test search
        print("\n3. Searching for 'privacy' (first 5 results):")
        results = db.search_content('privacy')[:5]
        for result in results:
            print(f"   [{result['type']}] {result['identifier']}: {result['title'][:50]}...")
        
        db.close()
        
        print("\n" + "=" * 80)
        print("DATABASE CREATION COMPLETE!")
        print("=" * 80)
        print(f"\nDatabase saved to: {os.path.abspath(db_file)}")
        print(f"File size: {os.path.getsize(db_file) / 1024:.1f} KB")
        
        print("\nYou can now use the database for queries:")
        print("1. Open the database with SQLite browser or command line")
        print("2. Use the Python interface for programmatic access")
        print("3. Run queries like:")
        print("   - SELECT * FROM sections WHERE title LIKE '%privacy%'")
        print("   - SELECT * FROM subsections WHERE section_id = (SELECT section_id FROM sections WHERE section_number = '248.202')")
        print("\nTo view the data in a GUI, you can use SQLite Browser or similar tools.")
        
    except Exception as e:
        print(f"\nError during database creation: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()