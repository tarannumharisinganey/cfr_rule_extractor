"""
FINRA Rules S3 to PostgreSQL Vector Parser - FIXED VERSION
Key Changes:
1. rule_number is now PRIMARY KEY (not id)
2. Sections use rule_number as foreign key
3. Improved section extraction with flexible # pattern matching
4. Captures FULL content between section labels
5. Properly handles (a), (b), (c)... while ignoring (i), (v), (x)...
"""

import re
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
from sentence_transformers import SentenceTransformer
from typing import List, Dict, Optional, Tuple
from datetime import datetime

class S3PostgresVectorParser:
    def __init__(self, pg_config: dict, aws_config: dict):
        """Initialize S3, PostgreSQL connection and embedding model"""
        print("Connecting to S3...")
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_config.get('aws_access_key_id'),
            aws_secret_access_key=aws_config.get('aws_secret_access_key'),
            region_name=aws_config.get('region_name', 'ap-south-1')
        )
        self.bucket_name = aws_config['bucket_name']
        self.folder_prefix = aws_config['folder_prefix']
        print(f"✓ Connected to S3 bucket: {self.bucket_name}")
        
        print("\nConnecting to PostgreSQL...")
        self.conn = psycopg2.connect(**pg_config)
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        print("✓ Connected to PostgreSQL")
        
        print("\nLoading embedding model...")
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        print("✓ Embedding model loaded (384-dimensional vectors)")
        
        self.setup_database()
    
    def setup_database(self):
        """Create PostgreSQL schema with rule_number as PRIMARY KEY"""
        print("\n" + "="*80)
        print("SETTING UP VECTOR DATABASE")
        print("="*80)
        
        print("\n1. Enabling pgvector extension...")
        self.cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        print("   ✓ pgvector extension enabled")
        
        print("\n2. Dropping old tables if they exist...")
        self.cursor.execute("DROP TABLE IF EXISTS supplementary_materials CASCADE;")
        self.cursor.execute("DROP TABLE IF EXISTS sections CASCADE;")
        self.cursor.execute("DROP TABLE IF EXISTS rules CASCADE;")
        print("   ✓ Old tables dropped")
        
        print("\n3. Creating tables with rule_number as PRIMARY KEY...")
        
        # Rules table - rule_number is PRIMARY KEY
        self.cursor.execute("""
            CREATE TABLE rules (
                rule_number VARCHAR(20) PRIMARY KEY,
                title TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        print("   ✓ Rules table created (rule_number is PK)")
        
        # Sections table - references rule_number
        self.cursor.execute("""
            CREATE TABLE sections (
                id SERIAL PRIMARY KEY,
                rule_number VARCHAR(20) NOT NULL REFERENCES rules(rule_number) ON DELETE CASCADE,
                section_label VARCHAR(10) NOT NULL,
                content TEXT NOT NULL,
                embedding vector(384) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_rule_section UNIQUE(rule_number, section_label)
            );
        """)
        print("   ✓ Sections table created (FK: rule_number)")
        
        # Supplementary materials table - references rule_number
        self.cursor.execute("""
            CREATE TABLE supplementary_materials (
                id SERIAL PRIMARY KEY,
                rule_number VARCHAR(20) NOT NULL REFERENCES rules(rule_number) ON DELETE CASCADE,
                material_number VARCHAR(10) NOT NULL,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                embedding vector(384) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_rule_material UNIQUE(rule_number, material_number)
            );
        """)
        print("   ✓ Supplementary materials table created (FK: rule_number)")
        
        self.conn.commit()
        
        print("\n4. Creating vector indexes...")
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS sections_embedding_idx 
            ON sections USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
        """)
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS supplementary_embedding_idx 
            ON supplementary_materials USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
        """)
        print("   ✓ Vector indexes created")
        
        print("\n5. Creating foreign key indexes...")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS sections_rule_number_idx ON sections(rule_number);")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS supplementary_rule_number_idx ON supplementary_materials(rule_number);")
        print("   ✓ Foreign key indexes created")
        
        self.conn.commit()
        print("\n✓ Database schema ready!")
        print("="*80 + "\n")
    
    def generate_embedding(self, text: str) -> List[float]:
        """Generate vector embedding for text"""
        if not text or text.strip() == "":
            return [0.0] * 384
        text_snippet = text[:1000]
        embedding = self.embedding_model.encode(text_snippet, convert_to_numpy=True)
        return embedding.tolist()
    
    def list_s3_files(self) -> List[str]:
        """List all markdown files in S3 bucket folder"""
        print(f"Listing files in s3://{self.bucket_name}/{self.folder_prefix}")
        files = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=self.folder_prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.endswith('.md'):
                        files.append(key)
        print(f"✓ Found {len(files)} markdown files\n")
        return files
    
    def read_s3_file(self, file_key: str) -> str:
        """Read markdown file from S3"""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
            content = response['Body'].read().decode('utf-8')
            return content
        except Exception as e:
            print(f"✗ Error reading {file_key}: {e}")
            return None
    
    def extract_rule_info(self, content: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract rule number and title"""
        patterns = [
            r'^#\s+(\d+)\.([^\n]+)',      # # 1220.RegistrationCategories
            r'^##\s+(\d+)\.([^\n]+)',     # ## 1210.RegistrationRequirements
            r'^(\d{4})\.([^\n]+)',        # 5130.Restrictions...
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content, re.MULTILINE)
            if match:
                rule_number = match.group(1).strip()
                title = match.group(2).strip()
                title = re.sub(r'\*\*', '', title)
                title = re.sub(r'([a-z])([A-Z])', r'\1 \2', title)
                return rule_number, title.strip()
        
        return None, None
    
    def parse_markdown_content(self, content: str, file_name: str):
        """Parse FINRA rule markdown"""
        print("=" * 80)
        print(f"PARSING: {file_name}")
        print("=" * 80)
        
        rule_number, rule_title = self.extract_rule_info(content)
        
        if not rule_number:
            filename_match = re.search(r'(\d{4})\.\s*(.+?)\s*(?:_|\.)', file_name)
            if filename_match:
                rule_number = filename_match.group(1)
                rule_title = filename_match.group(2).replace('_', ' ')
            else:
                print("✗ Could not extract rule number\n")
                return
        
        print(f"\n✓ Rule {rule_number}: {rule_title}")
        
        if self.insert_rule(rule_number, rule_title):
            self.parse_sections(content, rule_number)
            self.parse_supplementary_materials(content, rule_number)
        
        print(f"\n✓ Rule {rule_number} complete!\n")
    
    def insert_rule(self, rule_number: str, title: str) -> bool:
        """Insert or update a rule - rule_number is PK"""
        try:
            self.cursor.execute("""
                INSERT INTO rules (rule_number, title)
                VALUES (%s, %s)
                ON CONFLICT (rule_number) DO UPDATE 
                SET title = EXCLUDED.title, updated_at = CURRENT_TIMESTAMP;
            """, (rule_number, title))
            
            self.conn.commit()
            print(f"  ✓ Inserted Rule {rule_number}")
            return True
        except Exception as e:
            self.conn.rollback()
            print(f"  ✗ Error inserting rule: {e}")
            return False
    
    def find_supplementary_start(self, content: str) -> Optional[int]:
        """Find where supplementary material starts"""
        supp_patterns = [
            r'#{2,3}\s+•\s*•\s*•\s*Supplementary Material:\s*-*',
            r'•\s*•\s*•\s*Supplementary Material:\s*-*',
        ]
        
        for pattern in supp_patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                return match.start()
        
        return None
    
    def find_content_boundaries(self, content: str) -> Tuple[int, int]:
        """Find where main content starts and ends (BEFORE supplementary)"""
        # Find where supplementary material starts
        supp_start = self.find_supplementary_start(content)
        
        if supp_start is not None:
            end_pos = supp_start
        else:
            # If no supplementary found, look for metadata markers
            end_pos = len(content)
            meta_markers = [r'Amended by SR-FINRA', r'Selected Notices?:', r'^VERSIONS', r'^Disclaimer:']
            for marker in meta_markers:
                match = re.search(marker, content, re.IGNORECASE | re.MULTILINE)
                if match and match.start() < end_pos:
                    end_pos = match.start()
        
        # Find where content starts (after rule title)
        title_match = re.search(r'^#{1,2}\s+\d+\.', content, re.MULTILINE)
        start_pos = title_match.end() if title_match else 0
        
        return start_pos, end_pos
    
    def is_roman_numeral(self, label: str) -> bool:
        """Check if label is a Roman numeral like (i), (v), (x)"""
        roman_pattern = r'^(i|ii|iii|iv|v|vi|vii|viii|ix|x|xi|xii|xiii|xiv|xv|xvi|xvii|xviii|xix|xx)$'
        return re.match(roman_pattern, label.lower()) is not None
    
    def parse_sections(self, content: str, rule_number: str):
        """Parse sections with flexible # pattern - captures FULL content"""
        print("\n  Parsing sections...")
        
        # Get main content boundaries
        start_pos, end_pos = self.find_content_boundaries(content)
        main_content = content[start_pos:end_pos]
        
        print(f"    Main content area: {start_pos} to {end_pos} ({len(main_content)} chars)")
        
        # FLEXIBLE PATTERN: Matches any number of # followed by (lowercase letter)
        # Examples: # (a), ## (a), ### (a), #### (a)
        section_pattern = r'^#{1,}\s*\(([a-z])\)'
        
        matches = list(re.finditer(section_pattern, main_content, re.MULTILINE))
        
        # Filter out Roman numerals
        sections = []
        for m in matches:
            label = m.group(1)
            if not self.is_roman_numeral(label):
                sections.append({
                    'label': label,
                    'start': m.start(),
                    'end': m.end()
                })
        
        if not sections:
            print("    No labeled sections found - creating section with '-'")
            section_content = main_content.strip()
            if section_content:
                self.insert_section(rule_number, '-', section_content)
            return
        
        print(f"    Found {len(sections)} labeled sections: {[s['label'] for s in sections]}")
        
        # Extract FULL content for each section
        for i, section in enumerate(sections):
            label = section['label']
            content_start = section['end']
            
            # Find where this section ends (start of next section or end of main_content)
            if i + 1 < len(sections):
                content_end = sections[i + 1]['start']
            else:
                content_end = len(main_content)
            
            # Extract the section title and content
            section_content = main_content[content_start:content_end].strip()
            
            if section_content:
                # Extract title (first line after the label)
                lines = section_content.split('\n', 1)
                if lines:
                    title = lines[0].strip()
                    body = lines[1].strip() if len(lines) > 1 else ''
                    
                    # Combine title and body
                    full_content = f"{title}\n\n{body}".strip() if body else title
                    
                    self.insert_section(rule_number, label, full_content)
    
    def insert_section(self, rule_number: str, section_label: str, content: str) -> Optional[int]:
        """Insert a section with embedding"""
        try:
            # Generate embedding from content
            embedding = self.generate_embedding(content)
            
            self.cursor.execute("""
                INSERT INTO sections (rule_number, section_label, content, embedding)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (rule_number, section_label) DO UPDATE
                SET content = EXCLUDED.content, embedding = EXCLUDED.embedding
                RETURNING id;
            """, (rule_number, section_label, content, embedding))
            
            section_id = self.cursor.fetchone()['id']
            self.conn.commit()
            
            preview = content[:100].replace('\n', ' ')
            print(f"      ✓ ({section_label}) {len(content)} chars")
            print(f"         {preview}...")
            return section_id
        except Exception as e:
            self.conn.rollback()
            print(f"      ✗ Error inserting section ({section_label}): {e}")
            return None
    
    def parse_supplementary_materials(self, content: str, rule_number: str):
        """Parse supplementary materials (.01, .02, .03, etc.)"""
        print("\n  Parsing supplementary materials...")
        
        # Find supplementary section START
        supp_start = self.find_supplementary_start(content)
        
        if supp_start is None:
            print("    No supplementary materials section found")
            return
        
        # Find the actual content start (after the header line)
        supp_patterns = [
            r'#{2,3}\s+•\s*•\s*•\s*Supplementary Material:\s*-*',
            r'•\s*•\s*•\s*Supplementary Material:\s*-*',
        ]
        
        for pattern in supp_patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                supp_start = match.end()
                break
        
        # Find END of supplementary section
        end_markers = [r'Amended by SR-FINRA', r'Selected Notices?:', r'^VERSIONS', r'^Disclaimer:', r'^\[']
        supp_end = len(content)
        for marker in end_markers:
            match = re.search(marker, content[supp_start:], re.IGNORECASE | re.MULTILINE)
            if match and (supp_start + match.start()) < supp_end:
                supp_end = supp_start + match.start()
        
        supp_content = content[supp_start:supp_end].strip()
        
        print(f"    Supplementary area: {supp_start} to {supp_end} ({len(supp_content)} chars)")
        
        if not supp_content:
            print("    Supplementary section is empty")
            return
        
        # Pattern: .01 Title, .02 Title
        material_pattern = r'\.(\d{2})\s+([A-Z][^\n.]+?)\.(?:\s|$)'
        materials = list(re.finditer(material_pattern, supp_content))
        
        if not materials:
            print("    No numbered supplementary materials found")
            return
        
        print(f"    Found {len(materials)} supplementary materials")
        
        for i, match in enumerate(materials):
            material_number = match.group(1)
            material_title = match.group(2).strip()
            material_title = re.sub(r'\*\*', '', material_title).strip()
            
            content_start = match.end()
            content_end = materials[i + 1].start() if i + 1 < len(materials) else len(supp_content)
            
            material_content = supp_content[content_start:content_end].strip()
            
            if material_content:
                self.insert_supplementary_material(rule_number, material_number, material_title, material_content)
    
    def insert_supplementary_material(self, rule_number: str, material_number: str, title: str, content: str) -> Optional[int]:
        """Insert supplementary material with embedding"""
        try:
            embed_text = f"{title}. {content}"
            embedding = self.generate_embedding(embed_text)
            
            self.cursor.execute("""
                INSERT INTO supplementary_materials (rule_number, material_number, title, content, embedding)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (rule_number, material_number) DO UPDATE
                SET title = EXCLUDED.title, content = EXCLUDED.content, embedding = EXCLUDED.embedding
                RETURNING id;
            """, (rule_number, material_number, title, content, embedding))
            
            material_id = self.cursor.fetchone()['id']
            self.conn.commit()
            
            preview = content[:60].replace('\n', ' ')
            print(f"      ✓ .{material_number}: {title[:30]}")
            print(f"         {len(content)} chars | {preview}...")
            return material_id
        except Exception as e:
            self.conn.rollback()
            print(f"      ✗ Error inserting .{material_number}: {e}")
            return None
    
    def process_all_files(self):
        """Process all markdown files from S3"""
        files = self.list_s3_files()
        
        if not files:
            print("No files found")
            return
        
        print(f"Processing {len(files)} files...\n")
        
        for i, file_key in enumerate(files, 1):
            print(f"\n[{i}/{len(files)}] {file_key}")
            content = self.read_s3_file(file_key)
            if content:
                self.parse_markdown_content(content, file_key)
        
        print("\n" + "="*80)
        print("ALL FILES PROCESSED!")
        print("="*80)
    
    def get_statistics(self):
        """Get database statistics"""
        print("\n" + "=" * 80)
        print("DATABASE STATISTICS")
        print("=" * 80)
        
        self.cursor.execute("SELECT COUNT(*) as count FROM rules;")
        print(f"\nRules: {self.cursor.fetchone()['count']}")
        
        self.cursor.execute("SELECT COUNT(*) as count FROM sections;")
        print(f"Sections (with embeddings): {self.cursor.fetchone()['count']}")
        
        self.cursor.execute("SELECT COUNT(*) as count FROM supplementary_materials;")
        print(f"Supplementary Materials (with embeddings): {self.cursor.fetchone()['count']}")
        
        print("\n" + "-" * 80)
        print("SAMPLE STRUCTURES:")
        print("-" * 80)
        
        # Show sample rules
        self.cursor.execute("""
            SELECT r.rule_number, r.title, 
                   COUNT(DISTINCT s.id) as sections,
                   COUNT(DISTINCT sm.id) as supp_materials
            FROM rules r
            LEFT JOIN sections s ON r.rule_number = s.rule_number
            LEFT JOIN supplementary_materials sm ON r.rule_number = sm.rule_number
            GROUP BY r.rule_number, r.title
            ORDER BY r.rule_number
            LIMIT 5;
        """)
        
        for row in self.cursor.fetchall():
            print(f"\nRule {row['rule_number']}: {row['title']}")
            print(f"  → {row['sections']} sections | {row['supp_materials']} supp materials")
        
        # Show detailed example for Rule 5131
        print("\n" + "-" * 80)
        print("DETAILED EXAMPLE (Rule 5131):")
        print("-" * 80)
        
        self.cursor.execute("SELECT * FROM rules WHERE rule_number = '5131' LIMIT 1;")
        rule = self.cursor.fetchone()
        if rule:
            print(f"\nRule: {rule['rule_number']} - {rule['title']}")
            
            self.cursor.execute("""
                SELECT section_label, LENGTH(content) as len, LEFT(content, 150) as preview
                FROM sections WHERE rule_number = %s ORDER BY section_label;
            """, (rule['rule_number'],))
            
            print("\n  Sections:")
            for s in self.cursor.fetchall():
                print(f"    ({s['section_label']}) {s['len']} chars")
                print(f"       {s['preview']}...")
            
            self.cursor.execute("""
                SELECT material_number, title, LENGTH(content) as len
                FROM supplementary_materials WHERE rule_number = %s ORDER BY material_number;
            """, (rule['rule_number'],))
            
            supp = self.cursor.fetchall()
            if supp:
                print("\n  Supplementary Materials:")
                for sm in supp:
                    print(f"    .{sm['material_number']} {sm['title'][:50]} - {sm['len']} chars")
        
        print("\n" + "=" * 80)
        print("✓ Ready for vector search!")
        print("  - rule_number is PRIMARY KEY")
        print("  - Sections reference rule_number as FK")
        print("  - Flexible section parsing (# (a), ## (a), ### (a))")
        print("  - Full content extraction between labels")
        print("  - Roman numerals ignored")
        print("=" * 80 + "\n")
    
    def close(self):
        """Close database connection"""
        self.cursor.close()
        self.conn.close()


def main():
    """Main execution"""
    print("\n" + "=" * 80)
    print("FINRA RULES PARSER - FIXED VERSION")
    print("✓ rule_number as PRIMARY KEY")
    print("✓ Flexible section parsing: #(a), ##(a), ###(a)")
    print("✓ Full content extraction between labels")
    print("✓ Roman numerals ignored: (i), (v), (x)")
    print("=" * 80 + "\n")
    
    print("PostgreSQL Configuration:")
    pg_config = {
        'host': input("Host [localhost]: ").strip() or 'localhost',
        'database': input("Database [finra_rules]: ").strip() or 'finra_rules',
        'user': input("Username [postgres]: ").strip() or 'postgres',
        'password': input("Password: ").strip(),
        'port': int(input("Port [5432]: ").strip() or '5432')
    }
    
    print("\nAWS S3 Configuration:")
    aws_config = {
        'bucket_name': input("Bucket name [tarannumpdf]: ").strip() or 'tarannumpdf',
        'folder_prefix': input("Folder prefix [outputs/]: ").strip() or 'outputs/',
        'aws_access_key_id': input("AWS Access Key ID: ").strip(),
        'aws_secret_access_key': input("AWS Secret Access Key: ").strip(),
        'region_name': input("AWS Region [us-east-1]: ").strip() or 'us-east-1'
    }
    
    try:
        parser = S3PostgresVectorParser(pg_config, aws_config)
        parser.process_all_files()
        parser.get_statistics()
        parser.close()
        
        print("\n✓ ALL OPERATIONS COMPLETE!")
        print("✓ Database ready for vector search\n")
        
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()