# Vector Search Guide

Vector search enables semantic similarity search using embeddings from machine learning models. This guide covers strategies, best practices, and patterns for implementing vector search with ArcadeDB.

## Overview

Vector search transforms your data into high-dimensional vectors (embeddings) and finds similar items using distance metrics. Perfect for:

- **Semantic Search**: Find documents by meaning, not just keywords
- **Recommendation Systems**: Find similar products, users, or content
- **Image Search**: Find similar images using visual embeddings
- **Question Answering**: Match questions to relevant answers
- **Anomaly Detection**: Find outliers in vector space

**How It Works:**

1. Generate embeddings using ML models (Sentence Transformers, OpenAI, etc.)
2. Store vectors in ArcadeDB with HNSW indexing
3. Query with new vectors to find nearest neighbors
4. Get results ranked by similarity

## Quick Start

### 1. Install Dependencies

```bash
pip install arcadedb-embedded sentence-transformers
```

### 2. Create Vector Index

```python
import arcadedb_embedded as arcadedb
from arcadedb_embedded import to_java_float_array

# Create database and schema
db = arcadedb.create_database("./vector_demo")

with db.transaction():
    db.command("sql", "CREATE VERTEX TYPE Document")
    db.command("sql", "CREATE PROPERTY Document.text STRING")
    db.command("sql", "CREATE PROPERTY Document.embedding ARRAY_OF_FLOATS")

# Create vector index
index = db.create_vector_index(
    vertex_type="Document",
    vector_property="embedding",
    dimensions=384,  # Match your model
    distance_function="cosine"
)
```

### 3. Index Documents

```python
from sentence_transformers import SentenceTransformer

model = SentenceTransformer('all-MiniLM-L6-v2')

documents = [
    "Python is a programming language",
    "ArcadeDB is a graph database",
    "Machine learning uses neural networks"
]

with db.transaction():
    for doc_text in documents:
        # Generate embedding
        embedding = model.encode(doc_text)

        # Create vertex
        vertex = db.new_vertex("Document")
        vertex.set("text", doc_text)
        vertex.set("embedding", to_java_float_array(embedding))
        vertex.save()

        # Add to index
        index.add_vertex(vertex)
```

### 4. Search

```python
query = "What is Python?"
query_embedding = model.encode(query)

results = index.find_nearest(query_embedding, k=3)

for vertex, distance in results:
    text = vertex.get("text")
    similarity = 1 - distance  # Convert distance to similarity
    print(f"[{similarity:.3f}] {text}")
```

## Embedding Models

### Sentence Transformers

Best for text similarity:

```python
from sentence_transformers import SentenceTransformer

# All-purpose (384 dimensions)
model = SentenceTransformer('all-MiniLM-L6-v2')

# High quality (768 dimensions)
model = SentenceTransformer('all-mpnet-base-v2')

# Multilingual (768 dimensions)
model = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2')

# Generate embedding
embedding = model.encode("Your text here")
print(embedding.shape)  # (384,) or (768,)
```

**Installation:**
```bash
pip install sentence-transformers
```

---

### OpenAI Embeddings

Commercial API with high quality:

```python
from openai import OpenAI

client = OpenAI(api_key="your-api-key")

def get_embedding(text, model="text-embedding-3-small"):
    response = client.embeddings.create(
        input=text,
        model=model
    )
    return response.data[0].embedding

# text-embedding-3-small: 1536 dimensions
# text-embedding-3-large: 3072 dimensions
embedding = get_embedding("Your text here")
```

**Installation:**
```bash
pip install openai
```

---

### Hugging Face Transformers

For custom models:

```python
from transformers import AutoTokenizer, AutoModel
import torch

tokenizer = AutoTokenizer.from_pretrained('bert-base-uncased')
model = AutoModel.from_pretrained('bert-base-uncased')

def get_embedding(text):
    inputs = tokenizer(text, return_tensors='pt', truncation=True, max_length=512)
    with torch.no_grad():
        outputs = model(**inputs)
    # Use [CLS] token embedding
    embedding = outputs.last_hidden_state[0][0].numpy()
    return embedding

embedding = get_embedding("Your text here")
```

---

### CLIP (Images + Text)

For multimodal search:

```python
from transformers import CLIPProcessor, CLIPModel
from PIL import Image

model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32")
processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")

# Image embedding
image = Image.open("photo.jpg")
inputs = processor(images=image, return_tensors="pt")
image_embedding = model.get_image_features(**inputs)[0].detach().numpy()

# Text embedding
inputs = processor(text=["a photo of a cat"], return_tensors="pt")
text_embedding = model.get_text_features(**inputs)[0].detach().numpy()
```

## Distance Functions

### Cosine Distance

**Best for:** Text embeddings, normalized vectors

**Formula:** `1 - (A · B) / (||A|| * ||B||)`

**Range:** [0, 2], lower is more similar

**Usage:**
```python
index = db.create_vector_index(
    vertex_type="Document",
    vector_property="embedding",
    dimensions=384,
    distance_function="cosine"  # Default
)
```

**When to Use:**
- Text embeddings (Sentence Transformers, OpenAI)
- When direction matters more than magnitude
- Most common choice for semantic search

---

### Euclidean Distance

**Best for:** Image embeddings, spatial data

**Formula:** `sqrt(Σ(Ai - Bi)²)`

**Range:** [0, ∞), lower is more similar

**Usage:**
```python
index = db.create_vector_index(
    vertex_type="Image",
    vector_property="features",
    dimensions=512,
    distance_function="euclidean"
)
```

**When to Use:**
- Image embeddings (ResNet, VGG)
- When absolute distance matters
- Spatial or geometric data

---

### Inner Product

**Best for:** Collaborative filtering, recommendations

**Formula:** `-(A · B)`

**Range:** (-∞, ∞), **higher** is more similar (note: inverted!)

**Usage:**
```python
index = db.create_vector_index(
    vertex_type="User",
    vector_property="preferences",
    dimensions=128,
    distance_function="inner_product"
)
```

**When to Use:**
- Recommendation systems
- When vectors aren't normalized
- When magnitude carries information

## HNSW Parameters

### M Parameter

Controls connections per node in the graph.

```python
index = db.create_vector_index(
    vertex_type="Doc",
    vector_property="embedding",
    dimensions=384,
    m=16  # Number of connections
)
```

**Trade-offs:**

| M Value | Recall | Memory | Build Speed | Search Speed |
|---------|--------|--------|-------------|--------------|
| 8-12    | Lower  | Low    | Fast        | Fast         |
| 16-24   | Good   | Medium | Medium      | Medium       |
| 32-48   | High   | High   | Slow        | Slow         |

**Recommendations:**
- **Small datasets (<100K)**: M=16
- **Medium datasets (100K-1M)**: M=24
- **Large datasets (>1M)**: M=32-48

---

### ef Parameter

Controls search quality vs speed.

```python
index = db.create_vector_index(
    vertex_type="Doc",
    vector_property="embedding",
    dimensions=384,
    ef=128  # Search candidate list size
)
```

**Trade-offs:**

| ef Value | Recall | Search Speed |
|----------|--------|--------------|
| 50-100   | Lower  | Fast         |
| 128-200  | Good   | Medium       |
| 200-400  | High   | Slow         |

**Recommendations:**
- **Fast search**: ef=50-100
- **Balanced**: ef=128-200
- **High accuracy**: ef=200-400

---

### ef_construction Parameter

Controls index build quality.

```python
index = db.create_vector_index(
    vertex_type="Doc",
    vector_property="embedding",
    dimensions=384,
    ef_construction=128  # Build candidate list size
)
```

**Trade-offs:**

| ef_construction | Index Quality | Build Time |
|-----------------|---------------|------------|
| 100-150         | Lower         | Fast       |
| 128-256         | Good          | Medium     |
| 300-500         | High          | Slow       |

**Recommendations:**
- **Fast iteration**: ef_construction=100
- **Production**: ef_construction=200
- **Maximum quality**: ef_construction=400

**Note:** Higher ef_construction improves recall but only affects index building, not search.

## Schema Design

### Basic Schema

```python
with db.transaction():
    # Vertex type for documents
    db.command("sql", "CREATE VERTEX TYPE Document")
    db.command("sql", "CREATE PROPERTY Document.id STRING")
    db.command("sql", "CREATE PROPERTY Document.text STRING")
    db.command("sql", "CREATE PROPERTY Document.embedding ARRAY_OF_FLOATS")
    db.command("sql", "CREATE INDEX ON Document (id) UNIQUE")

# Vector index
index = db.create_vector_index(
    vertex_type="Document",
    vector_property="embedding",
    dimensions=384
)
```

---

### With Metadata

```python
with db.transaction():
    db.command("sql", "CREATE VERTEX TYPE Article")
    db.command("sql", "CREATE PROPERTY Article.id STRING")
    db.command("sql", "CREATE PROPERTY Article.title STRING")
    db.command("sql", "CREATE PROPERTY Article.content STRING")
    db.command("sql", "CREATE PROPERTY Article.category STRING")
    db.command("sql", "CREATE PROPERTY Article.created_at DATETIME")
    db.command("sql", "CREATE PROPERTY Article.embedding ARRAY_OF_FLOATS")

    # Regular indexes for filtering
    db.command("sql", "CREATE INDEX ON Article (id) UNIQUE")
    db.command("sql", "CREATE INDEX ON Article (category) NOTUNIQUE")

# Vector index
index = db.create_vector_index(
    vertex_type="Article",
    vector_property="embedding",
    dimensions=384
)
```

---

### Multiple Vector Types

```python
# Products with text and image embeddings
with db.transaction():
    db.command("sql", "CREATE VERTEX TYPE Product")
    db.command("sql", "CREATE PROPERTY Product.name STRING")
    db.command("sql", "CREATE PROPERTY Product.description STRING")
    db.command("sql", "CREATE PROPERTY Product.text_embedding ARRAY_OF_FLOATS")
    db.command("sql", "CREATE PROPERTY Product.image_embedding ARRAY_OF_FLOATS")

# Separate indexes for each embedding type
text_index = db.create_vector_index(
    vertex_type="Product",
    vector_property="text_embedding",
    dimensions=384,
    distance_function="cosine"
)

image_index = db.create_vector_index(
    vertex_type="Product",
    vector_property="image_embedding",
    dimensions=512,
    distance_function="euclidean"
)
```

## Search Patterns

### Basic Similarity Search

```python
query_embedding = model.encode("machine learning")
results = index.find_nearest(query_embedding, k=10)

for vertex, distance in results:
    text = vertex.get("text")
    print(f"{text} (distance: {distance:.4f})")
```

---

### Hybrid Search (Vector + Filters)

Combine vector similarity with metadata filters:

```python
# Get candidates from vector search
query_embedding = model.encode("python tutorial")
candidates = index.find_nearest(query_embedding, k=100)

# Filter by metadata
filtered_results = []
for vertex, distance in candidates:
    category = vertex.get("category")
    created_at = vertex.get("created_at")

    # Apply filters
    if category == "Programming" and distance < 0.5:
        filtered_results.append((vertex, distance))

    if len(filtered_results) >= 10:
        break

# Display results
for vertex, distance in filtered_results:
    title = vertex.get("title")
    print(f"{title} - {distance:.4f}")
```

---

### Re-ranking with Multiple Embeddings

```python
# First pass: Text search
text_query = "red sports car"
text_embedding = text_model.encode(text_query)
text_results = text_index.find_nearest(text_embedding, k=50)

# Second pass: Image search
image_embedding = image_model.encode(query_image)
image_results = image_index.find_nearest(image_embedding, k=50)

# Combine scores
combined_scores = {}
for vertex, distance in text_results:
    rid = vertex.get("@rid")
    combined_scores[rid] = {"vertex": vertex, "text_dist": distance, "image_dist": None}

for vertex, distance in image_results:
    rid = vertex.get("@rid")
    if rid in combined_scores:
        combined_scores[rid]["image_dist"] = distance
    else:
        combined_scores[rid] = {"vertex": vertex, "text_dist": None, "image_dist": distance}

# Weighted combination
final_results = []
for rid, data in combined_scores.items():
    text_dist = data["text_dist"] or 1.0
    image_dist = data["image_dist"] or 1.0

    # Weighted average
    combined_dist = 0.6 * text_dist + 0.4 * image_dist
    final_results.append((data["vertex"], combined_dist))

# Sort by combined score
final_results.sort(key=lambda x: x[1])

for vertex, score in final_results[:10]:
    name = vertex.get("name")
    print(f"{name} - combined score: {score:.4f}")
```

---

### Pagination

```python
def paginated_search(query_embedding, page_size=10, page=0):
    """Search with pagination."""
    # Get more results than needed
    k = (page + 1) * page_size + 10
    results = index.find_nearest(query_embedding, k=k)

    # Slice for current page
    start = page * page_size
    end = start + page_size

    page_results = list(results)[start:end]
    return page_results

# Page 0
page1 = paginated_search(query_embedding, page_size=10, page=0)

# Page 1
page2 = paginated_search(query_embedding, page_size=10, page=1)
```

## Performance Optimization

### Batch Indexing

```python
# Efficient: Batch in single transaction
batch_size = 1000
documents = load_documents()  # Your data source

with db.transaction():
    for i, doc in enumerate(documents):
        embedding = model.encode(doc['text'])

        vertex = db.new_vertex("Document")
        vertex.set("text", doc['text'])
        vertex.set("embedding", to_java_float_array(embedding))
        vertex.save()

        index.add_vertex(vertex)

        # Commit every batch_size records
        if (i + 1) % batch_size == 0:
            db.commit()
            db.begin()

    # Commit remaining
    db.commit()
```

---

### Embedding Caching

```python
import pickle
from pathlib import Path

class EmbeddingCache:
    def __init__(self, cache_dir="./embedding_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)

    def get_cache_path(self, text):
        import hashlib
        key = hashlib.md5(text.encode()).hexdigest()
        return self.cache_dir / f"{key}.pkl"

    def get(self, text):
        cache_path = self.get_cache_path(text)
        if cache_path.exists():
            with open(cache_path, 'rb') as f:
                return pickle.load(f)
        return None

    def set(self, text, embedding):
        cache_path = self.get_cache_path(text)
        with open(cache_path, 'wb') as f:
            pickle.dump(embedding, f)

# Usage
cache = EmbeddingCache()

def get_embedding_cached(text, model):
    embedding = cache.get(text)
    if embedding is None:
        embedding = model.encode(text)
        cache.set(text, embedding)
    return embedding

# Much faster on repeated queries
embedding = get_embedding_cached("python tutorial", model)
```

---

### Incremental Updates

```python
def add_document(db, index, model, doc_data):
    """Add single document efficiently."""
    with db.transaction():
        # Generate embedding
        embedding = model.encode(doc_data['text'])

        # Create vertex
        vertex = db.new_vertex("Document")
        vertex.set("id", doc_data['id'])
        vertex.set("text", doc_data['text'])
        vertex.set("embedding", to_java_float_array(embedding))
        vertex.save()

        # Add to index
        index.add_vertex(vertex)

    return vertex

def update_document(db, index, model, doc_id, new_text):
    """Update document and re-index."""
    with db.transaction():
        # Find existing
        result = db.query("sql", f"SELECT FROM Document WHERE id = '{doc_id}'")
        if not result.has_next():
            raise ValueError(f"Document {doc_id} not found")

        vertex = result.next()

        # Remove from index
        index.remove_vertex(doc_id)

        # Update
        new_embedding = model.encode(new_text)
        vertex.set("text", new_text)
        vertex.set("embedding", to_java_float_array(new_embedding))
        vertex.save()

        # Re-add to index
        index.add_vertex(vertex)
```

---

### Memory Management

```python
# For large datasets, process in chunks
def index_large_dataset(db, index, model, data_source, chunk_size=1000):
    """Index large dataset with memory management."""
    import gc

    chunk_count = 0

    for chunk in data_source.iter_chunks(chunk_size):
        # Generate embeddings for chunk
        texts = [item['text'] for item in chunk]
        embeddings = model.encode(texts, batch_size=32, show_progress_bar=True)

        # Index chunk
        with db.transaction():
            for item, embedding in zip(chunk, embeddings):
                vertex = db.new_vertex("Document")
                vertex.set("text", item['text'])
                vertex.set("embedding", to_java_float_array(embedding))
                vertex.save()

                index.add_vertex(vertex)

        chunk_count += 1
        print(f"Indexed chunk {chunk_count} ({chunk_size} items)")

        # Force garbage collection
        gc.collect()
```

## Production Patterns

### Configuration Management

```python
import os
from dataclasses import dataclass

@dataclass
class VectorSearchConfig:
    model_name: str = "all-MiniLM-L6-v2"
    dimensions: int = 384
    distance_function: str = "cosine"
    m: int = 16
    ef: int = 128
    ef_construction: int = 128
    max_items: int = 1000000

# Load from environment
config = VectorSearchConfig(
    model_name=os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2"),
    dimensions=int(os.getenv("VECTOR_DIMENSIONS", "384")),
    m=int(os.getenv("HNSW_M", "16")),
    ef=int(os.getenv("HNSW_EF", "128"))
)

# Use config
from sentence_transformers import SentenceTransformer
model = SentenceTransformer(config.model_name)

index = db.create_vector_index(
    vertex_type="Document",
    vector_property="embedding",
    dimensions=config.dimensions,
    distance_function=config.distance_function,
    m=config.m,
    ef=config.ef,
    ef_construction=config.ef_construction
)
```

---

### Monitoring and Logging

```python
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VectorSearchMetrics:
    def __init__(self):
        self.search_times = []
        self.index_times = []

    def log_search(self, query, results, elapsed):
        self.search_times.append(elapsed)
        avg_time = sum(self.search_times) / len(self.search_times)

        logger.info(
            f"Search: {len(results)} results in {elapsed:.3f}s "
            f"(avg: {avg_time:.3f}s)"
        )

    def log_index(self, count, elapsed):
        self.index_times.append(elapsed)
        rate = count / elapsed if elapsed > 0 else 0

        logger.info(
            f"Indexed: {count} items in {elapsed:.2f}s "
            f"({rate:.0f} items/sec)"
        )

# Usage
metrics = VectorSearchMetrics()

# Search with timing
start = time.time()
results = index.find_nearest(query_embedding, k=10)
elapsed = time.time() - start
metrics.log_search("user query", results, elapsed)

# Indexing with timing
start = time.time()
# ... index documents ...
elapsed = time.time() - start
metrics.log_index(100, elapsed)
```

---

### Error Handling

```python
from arcadedb_embedded import ArcadeDBError

def safe_vector_search(index, query_embedding, k=10, retries=3):
    """Vector search with retry logic."""
    for attempt in range(retries):
        try:
            results = index.find_nearest(query_embedding, k=k)
            return results

        except ArcadeDBError as e:
            if attempt < retries - 1:
                logger.warning(f"Search failed (attempt {attempt + 1}): {e}")
                time.sleep(0.1 * (attempt + 1))
                continue
            else:
                logger.error(f"Search failed after {retries} attempts: {e}")
                return []

    return []

def safe_vector_index(db, index, model, document):
    """Index with error handling."""
    try:
        with db.transaction():
            embedding = model.encode(document['text'])

            vertex = db.new_vertex("Document")
            vertex.set("text", document['text'])
            vertex.set("embedding", to_java_float_array(embedding))
            vertex.save()

            index.add_vertex(vertex)

            return True

    except ArcadeDBError as e:
        logger.error(f"Indexing failed for document {document.get('id')}: {e}")
        return False
```

## Common Use Cases

### Semantic Document Search

```python
# Index documents
documents = [
    {"id": "doc1", "title": "Python Tutorial", "content": "Learn Python..."},
    {"id": "doc2", "title": "ML Guide", "content": "Machine learning..."},
]

for doc in documents:
    # Combine title and content for embedding
    text = f"{doc['title']}. {doc['content']}"
    embedding = model.encode(text)

    with db.transaction():
        vertex = db.new_vertex("Document")
        vertex.set("id", doc['id'])
        vertex.set("title", doc['title'])
        vertex.set("content", doc['content'])
        vertex.set("embedding", to_java_float_array(embedding))
        vertex.save()

        index.add_vertex(vertex)

# Search
query = "How do I learn programming?"
query_embedding = model.encode(query)
results = index.find_nearest(query_embedding, k=5)

for vertex, distance in results:
    print(f"[{1-distance:.2f}] {vertex.get('title')}")
```

---

### Question Answering

```python
# Index Q&A pairs
qa_pairs = [
    {"q": "What is Python?", "a": "Python is a programming language..."},
    {"q": "How to install Python?", "a": "Download from python.org..."},
]

for qa in qa_pairs:
    # Embed the question
    embedding = model.encode(qa['q'])

    with db.transaction():
        vertex = db.new_vertex("QA")
        vertex.set("question", qa['q'])
        vertex.set("answer", qa['a'])
        vertex.set("embedding", to_java_float_array(embedding))
        vertex.save()

        index.add_vertex(vertex)

# Find answer for new question
new_question = "Where can I get Python?"
query_embedding = model.encode(new_question)
results = index.find_nearest(query_embedding, k=1)

if results:
    vertex, distance = results[0]
    print(f"Q: {vertex.get('question')}")
    print(f"A: {vertex.get('answer')}")
    print(f"Confidence: {1-distance:.2%}")
```

---

### Product Recommendations

```python
# Index products with description embeddings
products = [
    {"sku": "PROD1", "name": "Laptop", "desc": "High performance laptop"},
    {"sku": "PROD2", "name": "Mouse", "desc": "Wireless mouse"},
]

for prod in products:
    embedding = model.encode(prod['desc'])

    with db.transaction():
        vertex = db.new_vertex("Product")
        vertex.set("sku", prod['sku'])
        vertex.set("name", prod['name'])
        vertex.set("description", prod['desc'])
        vertex.set("embedding", to_java_float_array(embedding))
        vertex.save()

        index.add_vertex(vertex)

# Find similar products
target_product_desc = "portable computer"
query_embedding = model.encode(target_product_desc)
similar = index.find_nearest(query_embedding, k=5)

print("Similar products:")
for vertex, distance in similar:
    print(f"  {vertex.get('name')} - {1-distance:.2%} match")
```

## See Also

- [Vector API Reference](../api/vector.md) - Complete API documentation
- [Vector Examples](../examples/vectors.md) - Practical code examples
- [Database API](../api/database.md) - Database operations
- [HNSW Paper](https://arxiv.org/abs/1603.09320) - Original HNSW algorithm
