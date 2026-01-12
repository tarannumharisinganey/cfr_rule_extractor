# FINRA Rules Vector Database System

A complete pipeline for converting FINRA PDF rulebooks into searchable markdown documents and storing them in a PostgreSQL vector database for semantic search.

## 🏗️ Architecture Overview

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐      ┌──────────────┐
│   S3 Bucket │      │  AWS EC2     │      │  S3 Bucket  │      │  PostgreSQL  │
│   /finra/   │─────▶│  c6i.4xlarge │─────▶│  /output/   │─────▶│  + pgvector  │
│  (PDFs)     │      │  (Conversion)│      │  (Markdown) │      │  (Search DB) │
└─────────────┘      └──────────────┘      └─────────────┘      └──────────────┘
```

### Workflow:
1. **PDF Storage**: FINRA PDF rulebooks stored in S3 bucket (`tarannumpdf/finra/`)
2. **EC2 Processing**: AWS EC2 instance (c6i.4xlarge) runs the conversion notebook
3. **Markdown Output**: Converted markdown files stored in S3 bucket (`tarannumpdf/output/`)
4. **Vector Database**: Python parser reads markdown files and populates PostgreSQL with embeddings

---

## 📋 Prerequisites

### AWS Requirements
- **EC2 Instance**: `c6i.4xlarge` (16 vCPU, 32 GB RAM)
- **S3 Bucket**: `tarannumpdf` with folders:
  - `finra/` - Input PDF files
  - `output/` - Output markdown files
- **IAM Credentials**: Access key and secret key with S3 read/write permissions

### Database Requirements
- **PostgreSQL**: Version 12+ with `pgvector` extension
- **Database Name**: `finra_rules` (or custom)
- **Storage**: ~1GB+ depending on number of rules

---

## 🚀 Installation

### 1. EC2 Instance Setup (PDF to Markdown Conversion)

```bash
# Connect to your EC2 instance
ssh -i your-key.pem ec2-user@your-ec2-ip

# Install Python and dependencies
sudo apt-get update
sudo apt-get install python3-pip jupyter -y

# Clone repository
git clone <your-repo-url>
cd <repo-name>

# Install conversion dependencies
pip install marker-pdf boto3

# Remove conflicting libraries (if present)
pip uninstall -y jax jaxlib tensorflow chex flax
```

### 2. Local Machine Setup (Vector Database Population)

```bash
# Install PostgreSQL vector database dependencies
pip install psycopg2-binary sentence-transformers boto3

# Install pgvector extension in PostgreSQL
# Connect to your PostgreSQL database and run:
CREATE EXTENSION vector;
```

---

## 📝 Configuration

### AWS Credentials

Edit `down.ipynb` with your AWS credentials:

```python
aws_access_key = "YOUR_AWS_ACCESS_KEY"
aws_secret_key = "YOUR_AWS_SECRET_KEY"
aws_region = "ap-south-1"  # or your region

BUCKET_NAME = 'tarannumpdf'
INPUT_FOLDER = 'finra/'
OUTPUT_FOLDER = 'output/'
```

Edit `awspg.py` for database population (credentials prompted at runtime):

```python
# You'll be prompted for:
# - PostgreSQL host, database, user, password, port
# - AWS access key, secret key, region
# - S3 bucket name and folder prefix
```

---

## 🔄 Usage

### Step 1: Convert PDFs to Markdown (Run on EC2)

```bash
# Start Jupyter on EC2
jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser

# Or run directly with:
jupyter nbconvert --execute down.ipynb --to notebook
```

The notebook will:
- ✅ Check existing markdown files in `output/` to avoid reprocessing
- ✅ Download PDFs from `finra/` folder
- ✅ Convert each PDF to markdown using `marker-pdf`
- ✅ Upload converted `.md` files to `output/` folder
- ✅ Display progress with ETA for remaining files

**Features:**
- **Smart Resume**: Automatically skips files already converted
- **Progress Tracking**: Shows `[X/Y]` progress with estimated time remaining
- **Error Handling**: Logs failed conversions for review
- **Cleanup**: Removes temporary files to save disk space

### Step 2: Populate Vector Database (Run Locally)

```bash
# Run the parser script
python awspg.py

# You'll be prompted for:
# PostgreSQL Configuration:
#   Host [localhost]: 
#   Database [finra_rules]: 
#   Username [postgres]: 
#   Password: 
#   Port [5432]: 
#
# AWS S3 Configuration:
#   Bucket name [tarannumpdf]: 
#   Folder prefix [outputs/]: 
#   AWS Access Key ID: 
#   AWS Secret Access Key: 
#   AWS Region [us-east-1]: 
```

The script will:
- ✅ Connect to S3 and list all markdown files
- ✅ Create/update PostgreSQL schema with vector support
- ✅ Parse each markdown file to extract:
  - Rule metadata (number, title)
  - Labeled sections: `(a)`, `(b)`, `(c)`, etc.
  - Supplementary materials: `.01`, `.02`, `.03`, etc.
- ✅ Generate 384-dimensional vector embeddings using `all-MiniLM-L6-v2`
- ✅ Store everything in structured tables with foreign key relationships
- ✅ Display comprehensive statistics at completion

---

## 🗄️ Database Schema

### Tables Overview

```sql
rules (parent table)
├── sections (child table with embeddings)
└── supplementary_materials (child table with embeddings)
```

### 1. **`rules`** - Rule Metadata

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PRIMARY KEY | Auto-incrementing unique identifier |
| `rule_number` | VARCHAR(20) UNIQUE | FINRA rule number (e.g., "1210", "5130") |
| `title` | TEXT | Full title of the rule |
| `created_at` | TIMESTAMP | When rule was first inserted |
| `updated_at` | TIMESTAMP | When rule was last updated |

**Example:**
```sql
id | rule_number | title                              | created_at | updated_at
---+-------------+------------------------------------+------------+------------
1  | 1210        | Registration Requirements          | 2025-01-12 | 2025-01-12
2  | 5130        | Restrictions on Communications     | 2025-01-12 | 2025-01-12
```

### 2. **`sections`** - Rule Sections with Embeddings

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PRIMARY KEY | Auto-incrementing unique identifier |
| `rule_id` | INTEGER (FK) | Foreign key to `rules.id` |
| `section_label` | VARCHAR(10) | Section label: "a", "b", "c", or "-" for unlabeled |
| `content` | TEXT | Full section content (no title truncation) |
| `embedding` | vector(384) | 384-dimensional semantic vector embedding |
| `created_at` | TIMESTAMP | When section was inserted |

**Constraints:**
- `UNIQUE(rule_id, section_label)` - Prevents duplicate sections per rule
- `ON DELETE CASCADE` - Deletes sections when parent rule is deleted

**Example:**
```sql
id | rule_id | section_label | content                          | embedding | created_at
---+---------+---------------+----------------------------------+-----------+------------
1  | 1      | a             | Each person engaged in the...    | [0.1,0.2] | 2025-01-12
2  | 1      | b             | Applications must include...     | [0.3,0.4] | 2025-01-12
3  | 2      | -             | No member shall publish...       | [0.5,0.6] | 2025-01-12
```

**Note:** Section label is `-` when no labeled sections like `(a)`, `(b)`, `(c)` are found.

### 3. **`supplementary_materials`** - Additional Materials with Embeddings

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PRIMARY KEY | Auto-incrementing unique identifier |
| `rule_id` | INTEGER (FK) | Foreign key to `rules.id` |
| `material_number` | VARCHAR(10) | Material number: "01", "02", "03", etc. |
| `title` | TEXT | Title of the supplementary material |
| `content` | TEXT | Full content of the supplementary material |
| `embedding` | vector(384) | 384-dimensional semantic vector embedding |
| `created_at` | TIMESTAMP | When material was inserted |

**Constraints:**
- `UNIQUE(rule_id, material_number)` - Prevents duplicate materials per rule
- `ON DELETE CASCADE` - Deletes materials when parent rule is deleted

**Example:**
```sql
id | rule_id | material_number | title                  | content              | embedding | created_at
---+---------+-----------------+------------------------+----------------------+-----------+------------
1  | 1      | 01              | Definition of Terms    | For purposes of...   | [0.7,0.8] | 2025-01-12
2  | 1      | 02              | Filing Requirements    | Members must file... | [0.9,1.0] | 2025-01-12
```

### Vector Indexes

```sql
-- For fast similarity search on sections
CREATE INDEX sections_embedding_idx 
ON sections USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- For fast similarity search on supplementary materials
CREATE INDEX supplementary_embedding_idx 
ON supplementary_materials USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
```

### Foreign Key Indexes

```sql
-- For fast joins between rules and sections
CREATE INDEX sections_rule_id_idx ON sections(rule_id);

-- For fast joins between rules and supplementary materials
CREATE INDEX supplementary_rule_id_idx ON supplementary_materials(rule_id);
```

---

## 🔍 Key Features

### PDF Conversion (`down.ipynb`)
- **Smart Resume Logic**: Checks S3 output folder and only processes new PDFs
- **Batch Processing**: Handles multiple PDFs sequentially with progress tracking
- **Robust Error Handling**: Captures and logs conversion failures
- **Resource Optimization**: Cleans up temporary files after each conversion
- **ETA Calculation**: Shows estimated time remaining based on average processing time

### Vector Database Parser (`awspg.py`)
- **Flexible Section Parsing**: Handles various markdown formats (`#(a)`, `##(a)`, `###(a)`, `-(a)`)
- **Roman Numeral Filtering**: Ignores subsections like `(i)`, `(ii)`, `(iii)` to avoid false positives
- **Clear Content Boundaries**: Separates main rule sections from supplementary materials
- **Full Content Storage**: Preserves complete section content without truncation
- **Semantic Embeddings**: Uses `all-MiniLM-L6-v2` for 384-dimensional vectors
- **Foreign Key Relationships**: Maintains data integrity with CASCADE deletes

---

