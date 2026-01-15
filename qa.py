"""
FINRA Rules Q&A System - Vector Search Based
Answers questions by finding the most relevant rule sections using embeddings
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from sentence_transformers import SentenceTransformer
from typing import List, Dict, Optional
import numpy as np

class FINRAQuestionAnswering:
    def __init__(self, pg_config: dict):
        """Initialize PostgreSQL connection and embedding model"""
        print("Connecting to PostgreSQL...")
        self.conn = psycopg2.connect(**pg_config)
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        print("✓ Connected to PostgreSQL")
        
        print("\nLoading embedding model...")
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        print("✓ Embedding model loaded")
        
        # Verify database has data
        self.cursor.execute("SELECT COUNT(*) as count FROM rules;")
        rule_count = self.cursor.fetchone()['count']
        self.cursor.execute("SELECT COUNT(*) as count FROM sections;")
        section_count = self.cursor.fetchone()['count']
        
        print(f"\n✓ Database loaded: {rule_count} rules, {section_count} sections\n")
    
    def generate_embedding(self, text: str) -> List[float]:
        """Generate vector embedding for query"""
        embedding = self.embedding_model.encode(text, convert_to_numpy=True)
        return embedding.tolist()
    
    def search_sections(self, query: str, top_k: int = 5) -> List[Dict]:
        """
        Search for most relevant sections using vector similarity
        Returns top_k most similar sections
        """
        print(f"Searching for: '{query}'")
        print("Generating query embedding...")
        
        # Generate embedding for the query
        query_embedding = self.generate_embedding(query)
        
        # Perform vector similarity search using cosine distance
        self.cursor.execute("""
            SELECT 
                s.id,
                s.rule_number,
                s.section_label,
                s.content,
                r.title as rule_title,
                1 - (s.embedding <=> %s::vector) as similarity
            FROM sections s
            JOIN rules r ON s.rule_number = r.rule_number
            ORDER BY s.embedding <=> %s::vector
            LIMIT %s;
        """, (query_embedding, query_embedding, top_k))
        
        results = self.cursor.fetchall()
        return results
    
    def search_supplementary(self, query: str, top_k: int = 3) -> List[Dict]:
        """
        Search supplementary materials using vector similarity
        """
        query_embedding = self.generate_embedding(query)
        
        self.cursor.execute("""
            SELECT 
                sm.id,
                sm.rule_number,
                sm.material_number,
                sm.title,
                sm.content,
                r.title as rule_title,
                1 - (sm.embedding <=> %s::vector) as similarity
            FROM supplementary_materials sm
            JOIN rules r ON sm.rule_number = r.rule_number
            ORDER BY sm.embedding <=> %s::vector
            LIMIT %s;
        """, (query_embedding, query_embedding, top_k))
        
        results = self.cursor.fetchall()
        return results
    
    def search_combined(self, query: str, section_k: int = 3, supp_k: int = 2) -> Dict:
        """
        Search both sections and supplementary materials
        Returns combined results
        """
        sections = self.search_sections(query, section_k)
        supplementary = self.search_supplementary(query, supp_k)
        
        return {
            'sections': sections,
            'supplementary': supplementary
        }
    
    def format_answer(self, results: Dict, show_scores: bool = True) -> str:
        """
        Format search results into a readable answer
        """
        answer = "\n" + "="*80 + "\n"
        answer += "ANSWER\n"
        answer += "="*80 + "\n"
        
        # Format sections
        if results['sections']:
            answer += "\n📋 RELEVANT RULE SECTIONS:\n"
            answer += "-"*80 + "\n"
            
            for i, section in enumerate(results['sections'], 1):
                similarity_pct = section['similarity'] * 100
                
                answer += f"\n{i}. Rule {section['rule_number']}: {section['rule_title']}\n"
                answer += f"   Section ({section['section_label']})"
                
                if show_scores:
                    answer += f" | Relevance: {similarity_pct:.1f}%"
                
                answer += "\n\n"
                
                # Show content (truncate if too long)
                content = section['content']
                if len(content) > 500:
                    content = content[:500] + "..."
                
                answer += f"   {content}\n"
                answer += "-"*80 + "\n"
        
        # Format supplementary materials
        if results['supplementary']:
            answer += "\n📚 SUPPLEMENTARY MATERIALS:\n"
            answer += "-"*80 + "\n"
            
            for i, supp in enumerate(results['supplementary'], 1):
                similarity_pct = supp['similarity'] * 100
                
                answer += f"\n{i}. Rule {supp['rule_number']}.{supp['material_number']}: {supp['title']}"
                
                if show_scores:
                    answer += f" | Relevance: {similarity_pct:.1f}%"
                
                answer += "\n\n"
                
                # Show content (truncate if too long)
                content = supp['content']
                if len(content) > 400:
                    content = content[:400] + "..."
                
                answer += f"   {content}\n"
                answer += "-"*80 + "\n"
        
        return answer
    
    def ask(self, question: str, section_k: int = 3, supp_k: int = 2, show_scores: bool = True) -> str:
        """
        Main Q&A method - ask a question and get formatted answer
        
        Args:
            question: The question to ask
            section_k: Number of relevant sections to retrieve
            supp_k: Number of supplementary materials to retrieve
            show_scores: Whether to show similarity scores
        """
        results = self.search_combined(question, section_k, supp_k)
        answer = self.format_answer(results, show_scores)
        return answer
    
    def get_rule_details(self, rule_number: str) -> Dict:
        """
        Get complete details for a specific rule
        """
        # Get rule info
        self.cursor.execute("""
            SELECT * FROM rules WHERE rule_number = %s;
        """, (rule_number,))
        rule = self.cursor.fetchone()
        
        if not rule:
            return None
        
        # Get sections
        self.cursor.execute("""
            SELECT section_label, content 
            FROM sections 
            WHERE rule_number = %s 
            ORDER BY section_label;
        """, (rule_number,))
        sections = self.cursor.fetchall()
        
        # Get supplementary materials
        self.cursor.execute("""
            SELECT material_number, title, content 
            FROM supplementary_materials 
            WHERE rule_number = %s 
            ORDER BY material_number;
        """, (rule_number,))
        supplementary = self.cursor.fetchall()
        
        return {
            'rule': rule,
            'sections': sections,
            'supplementary': supplementary
        }
    
    def interactive_mode(self):
        """
        Run interactive Q&A session
        """
        print("\n" + "="*80)
        print("FINRA RULES Q&A SYSTEM - INTERACTIVE MODE")
        print("="*80)
        print("\nCommands:")
        print("  - Type your question to search")
        print("  - 'rule XXXX' to get full details of a rule (e.g., 'rule 5131')")
        print("  - 'exit' or 'quit' to exit")
        print("="*80 + "\n")
        
        while True:
            try:
                user_input = input("\n🔍 Your question: ").strip()
                
                if not user_input:
                    continue
                
                if user_input.lower() in ['exit', 'quit', 'q']:
                    print("\n👋 Goodbye!\n")
                    break
                
                # Check if user wants full rule details
                if user_input.lower().startswith('rule '):
                    rule_number = user_input[5:].strip()
                    details = self.get_rule_details(rule_number)
                    
                    if details:
                        print(f"\n📜 Rule {rule_number}: {details['rule']['title']}")
                        print("="*80)
                        
                        print("\nSections:")
                        for section in details['sections']:
                            print(f"\n({section['section_label']}):")
                            print(f"{section['content'][:300]}...")
                        
                        if details['supplementary']:
                            print("\nSupplementary Materials:")
                            for supp in details['supplementary']:
                                print(f"\n.{supp['material_number']} {supp['title']}")
                                print(f"{supp['content'][:200]}...")
                    else:
                        print(f"\n❌ Rule {rule_number} not found")
                    
                    continue
                
                # Regular question
                answer = self.ask(user_input, section_k=3, supp_k=2, show_scores=True)
                print(answer)
                
            except KeyboardInterrupt:
                print("\n\n👋 Goodbye!\n")
                break
            except Exception as e:
                print(f"\n❌ Error: {e}")
    
    def close(self):
        """Close database connection"""
        self.cursor.close()
        self.conn.close()


def main():
    """Main execution with example queries"""
    print("\n" + "="*80)
    print("FINRA RULES Q&A SYSTEM")
    print("="*80 + "\n")
    
    print("PostgreSQL Configuration:")
    pg_config = {
        'host': input("Host [localhost]: ").strip() or 'localhost',
        'database': input("Database [finra_rules]: ").strip() or 'finra_rules',
        'user': input("Username [postgres]: ").strip() or 'postgres',
        'password': input("Password: ").strip(),
        'port': int(input("Port [5432]: ").strip() or '5432')
    }
    
    try:
        qa = FINRAQuestionAnswering(pg_config)
        
        # Example usage mode
        print("\n" + "="*80)
        print("Choose mode:")
        print("1. Interactive Q&A")
        print("2. Run example queries")
        print("="*80)
        
        mode = input("\nSelect mode (1 or 2) [1]: ").strip() or '1'
        
        if mode == '1':
            qa.interactive_mode()
        else:
            # Example queries
            example_questions = [
                "What are the best execution requirements?",
                "What information can FINRA request during an investigation?",
                "What is disclosed through BrokerCheck?",
                "What are the rules about new issue allocations?",
                "What is spinning in the context of IPOs?"
            ]
            
            print("\n" + "="*80)
            print("RUNNING EXAMPLE QUERIES")
            print("="*80)
            
            for question in example_questions:
                print(f"\n\n{'='*80}")
                print(f"QUESTION: {question}")
                print('='*80)
                
                answer = qa.ask(question, section_k=2, supp_k=1, show_scores=True)
                print(answer)
                
                input("\nPress Enter for next question...")
        
        qa.close()
        print("\n✓ Session ended\n")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()