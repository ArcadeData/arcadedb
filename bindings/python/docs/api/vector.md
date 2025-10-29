# Vector API

Vector search capabilities in ArcadeDB use HNSW (Hierarchical Navigable Small World) indexing for fast approximate nearest neighbor search. Perfect for semantic search, recommendation systems, and similarity-based queries.

## Overview

ArcadeDB's vector support enables:

- **Semantic Search**: Find similar documents, images, or any embedded content
- **Recommendation Systems**: Find similar items based on feature vectors
- **Clustering**: Group similar vectors together
- **Anomaly Detection**: Find outliers in vector space

**Key Features:**

- HNSW indexing for O(log N) search performance
- Multiple distance metrics (cosine, euclidean, inner product)
- Native NumPy integration (optional)
- Configurable precision/performance trade-offs

## Module Functions

Utility functions for converting between Python and Java vector representations:

### `to_java_float_array(vector)`

Convert a Python array-like object to a Java float array compatible with ArcadeDB's vector indexing.

**Parameters:**

- `vector`: Array-like object containing float values
  - Python list: `[0.1, 0.2, 0.3]`
  - NumPy array: `np.array([0.1, 0.2, 0.3])`
  - Any iterable: `(0.1, 0.2, 0.3)`

**Returns:**

- Java float array (`JArray<JFloat>`)

**Example:**

```python
import arcadedb_embedded as arcadedb
from arcadedb_embedded import to_java_float_array
import numpy as np

# From Python list
vec_list = [0.1, 0.2, 0.3, 0.4]
java_vec = to_java_float_array(vec_list)

# From NumPy array (if NumPy installed)
vec_np = np.array([0.5, 0.6, 0.7, 0.8], dtype=np.float32)
java_vec = to_java_float_array(vec_np)

# Use with vertex
vertex = db.new_vertex("Document")
vertex.set("embedding", java_vec)
vertex.save()
```

---

### `to_python_array(java_vector, use_numpy=True)`

Convert a Java array or ArrayList to a Python array.

**Parameters:**

- `java_vector`: Java array or ArrayList of floats
- `use_numpy` (bool): Return NumPy array if available (default: `True`)
  - If `True` and NumPy is installed: returns `np.ndarray`
  - If `False` or NumPy unavailable: returns Python `list`

**Returns:**

- `np.ndarray` (if `use_numpy=True` and NumPy available)
- `list` (otherwise)

**Example:**

```python
from arcadedb_embedded import to_python_array

# Get vector from vertex
vertex = result_set.next()
java_vec = vertex.get("embedding")

# Convert to NumPy array
np_vec = to_python_array(java_vec, use_numpy=True)
print(type(np_vec))  # <class 'numpy.ndarray'>

# Convert to Python list
py_list = to_python_array(java_vec, use_numpy=False)
print(type(py_list))  # <class 'list'>
```

---

## VectorIndex Class

Wrapper for ArcadeDB's HNSW vector index, providing similarity search capabilities.

### Creation via Database

Vector indexes are created using the `Database.create_vector_index()` method:

**Signature:**

```python
db.create_vector_index(
    vertex_type: str,
    vector_property: str,
    dimensions: int,
    id_property: str = "id",
    edge_type: str = "VectorProximity",
    deleted_property: str = "deleted",
    distance_function: str = "cosine",
    m: int = 16,
    ef: int = 128,
    ef_construction: int = 128,
    max_items: int = 10000
) -> VectorIndex
```

**Parameters:**

- `vertex_type` (str): Vertex type containing vectors
- `vector_property` (str): Property name storing vector arrays
- `dimensions` (int): Vector dimensionality (must match your embeddings)
- `id_property` (str): Property used as unique ID (default: `"id"`)
- `edge_type` (str): Edge type for proximity graph (default: `"VectorProximity"`)
- `deleted_property` (str): Property marking deleted items (default: `"deleted"`)
- `distance_function` (str): Distance metric (default: `"cosine"`)
  - `"cosine"`: Cosine distance (1 - cosine similarity)
  - `"euclidean"`: Euclidean distance (L2 norm)
  - `"inner_product"`: Negative inner product
- `m` (int): HNSW M parameter - bidirectional links per node (default: 16)
  - Higher = better recall, more memory
  - Typical range: 12-48
- `ef` (int): Search candidate list size (default: 128)
  - Higher = better recall, slower search
  - Typical range: 100-400
- `ef_construction` (int): Build candidate list size (default: 128)
  - Higher = better quality index, slower build
  - Typical range: 100-400
- `max_items` (int): Maximum indexed items (default: 10000)

**Returns:**

- `VectorIndex`: Index object for searching

**Example:**

```python
import arcadedb_embedded as arcadedb
from arcadedb_embedded import to_java_float_array
import numpy as np

# Create database and schema
db = arcadedb.create_database("./vector_db")

with db.transaction():
    db.command("sql", "CREATE VERTEX TYPE Document")
    db.command("sql", "CREATE PROPERTY Document.id STRING")
    db.command("sql", "CREATE PROPERTY Document.text STRING")
    db.command("sql", "CREATE PROPERTY Document.embedding ARRAY_OF_FLOATS")
    db.command("sql", "CREATE INDEX ON Document (id) UNIQUE")

# Create vector index
index = db.create_vector_index(
    vertex_type="Document",
    vector_property="embedding",
    dimensions=384,  # Match your embedding model
    distance_function="cosine",
    m=16,
    ef=128
)

print(f"Created vector index: {index}")
```

---

### `VectorIndex.find_nearest(query_vector, k=10, use_numpy=True)`

Find k-nearest neighbors to the query vector.

**Parameters:**

- `query_vector`: Query vector as:
  - Python list: `[0.1, 0.2, ...]`
  - NumPy array: `np.array([0.1, 0.2, ...])`
  - Any array-like iterable
- `k` (int): Number of neighbors to return (default: 10)
- `use_numpy` (bool): Return vectors as NumPy if available (default: `True`)

**Returns:**

- `List[Tuple[vertex, float]]`: List of `(vertex, distance)` tuples
  - `vertex`: Matched vertex object (MutableVertex)
  - `distance`: Similarity score (float)
    - Lower = more similar
    - Range depends on distance function

**Example:**

```python
# Generate query vector (in practice, from your embedding model)
query_text = "machine learning tutorial"
query_vector = generate_embedding(query_text)  # Your embedding function

# Search for 5 most similar documents
neighbors = index.find_nearest(query_vector, k=5)

for vertex, distance in neighbors:
    doc_id = vertex.get("id")
    text = vertex.get("text")
    print(f"Distance: {distance:.4f} | ID: {doc_id}")
    print(f"  Text: {text[:100]}...")
```

**Distance Interpretation:**

| Function | Range | Lower = More Similar |
|----------|-------|---------------------|
| cosine | [0, 2] | ✓ (0 = identical) |
| euclidean | [0, ∞) | ✓ (0 = identical) |
| inner_product | (-∞, ∞) | ✗ (higher = more similar) |

---

### `VectorIndex.add_vertex(vertex)`

Add a single vertex to the index.

**Parameters:**

- `vertex`: Vertex object with vector property set

**Raises:**

- `ArcadeDBError`: If vertex cannot be added

**Example:**

```python
# Add during vertex creation
with db.transaction():
    doc = db.new_vertex("Document")
    doc.set("id", "doc_001")
    doc.set("text", "Introduction to vector search")
    doc.set("embedding", to_java_float_array(embedding))
    doc.save()

    # Add to index
    index.add_vertex(doc)
```

**Important:**

- Vertex must have the vector property populated
- Vector dimensionality must match index dimensions
- Call within a transaction for consistency

---

### `VectorIndex.remove_vertex(vertex_id)`

Remove a vertex from the index.

**Parameters:**

- `vertex_id`: ID of the vertex to remove (typically string or int)

**Raises:**

- `ArcadeDBError`: If removal fails

**Example:**

```python
# Remove by ID
vertex_id = "doc_001"
index.remove_vertex(vertex_id)
```

**Note:** This removes from the vector index only, not from the database. To fully delete:

```python
with db.transaction():
    # Remove from index
    index.remove_vertex(doc_id)

    # Delete from database
    db.command("sql", f"DELETE FROM Document WHERE id = '{doc_id}'")
```

---

## Complete Examples

### Semantic Search with Sentence Transformers

```python
import arcadedb_embedded as arcadedb
from arcadedb_embedded import to_java_float_array
from sentence_transformers import SentenceTransformer
import numpy as np

# Load embedding model
model = SentenceTransformer('all-MiniLM-L6-v2')  # 384 dimensions

# Create database and schema
db = arcadedb.create_database("./semantic_search")

with db.transaction():
    db.command("sql", "CREATE VERTEX TYPE Document")
    db.command("sql", "CREATE PROPERTY Document.id STRING")
    db.command("sql", "CREATE PROPERTY Document.title STRING")
    db.command("sql", "CREATE PROPERTY Document.content STRING")
    db.command("sql", "CREATE PROPERTY Document.embedding ARRAY_OF_FLOATS")
    db.command("sql", "CREATE INDEX ON Document (id) UNIQUE")

# Create vector index (384 dimensions for all-MiniLM-L6-v2)
index = db.create_vector_index(
    vertex_type="Document",
    vector_property="embedding",
    dimensions=384,
    distance_function="cosine",
    m=16,
    ef=200  # Higher for better recall
)

# Sample documents
documents = [
    {"id": "doc1", "title": "Python Tutorial",
     "content": "Learn Python programming basics"},
    {"id": "doc2", "title": "Machine Learning Guide",
     "content": "Introduction to ML algorithms"},
    {"id": "doc3", "title": "Database Systems",
     "content": "Understanding relational databases"},
]

# Index documents
print("Indexing documents...")
with db.transaction():
    for doc in documents:
        # Generate embedding
        text = f"{doc['title']} {doc['content']}"
        embedding = model.encode(text)

        # Create vertex
        vertex = db.new_vertex("Document")
        vertex.set("id", doc["id"])
        vertex.set("title", doc["title"])
        vertex.set("content", doc["content"])
        vertex.set("embedding", to_java_float_array(embedding))
        vertex.save()

        # Add to vector index
        index.add_vertex(vertex)

print(f"Indexed {len(documents)} documents")

# Search
query = "How to learn programming"
query_embedding = model.encode(query)

print(f"\nQuery: '{query}'")
results = index.find_nearest(query_embedding, k=3)

for vertex, distance in results:
    print(f"\nDistance: {distance:.4f}")
    print(f"Title: {vertex.get('title')}")
    print(f"Content: {vertex.get('content')}")

db.close()
```

---

### Hybrid Search (Vector + Filters)

Combine vector similarity with property filters using SQL:

```python
import arcadedb_embedded as arcadedb
from arcadedb_embedded import to_java_float_array
import numpy as np

db = arcadedb.open_database("./products_db")

# Create schema
with db.transaction():
    db.command("sql", "CREATE VERTEX TYPE Product")
    db.command("sql", "CREATE PROPERTY Product.id STRING")
    db.command("sql", "CREATE PROPERTY Product.name STRING")
    db.command("sql", "CREATE PROPERTY Product.category STRING")
    db.command("sql", "CREATE PROPERTY Product.price DECIMAL")
    db.command("sql", "CREATE PROPERTY Product.features ARRAY_OF_FLOATS")
    db.command("sql", "CREATE INDEX ON Product (category) NOTUNIQUE")

# Create vector index
index = db.create_vector_index(
    vertex_type="Product",
    vector_property="features",
    dimensions=128
)

# Add products with feature vectors
products = [
    {"id": "p1", "name": "Laptop", "category": "Electronics",
     "price": 999.99, "features": np.random.rand(128)},
    {"id": "p2", "name": "Mouse", "category": "Electronics",
     "price": 29.99, "features": np.random.rand(128)},
    {"id": "p3", "name": "Desk", "category": "Furniture",
     "price": 299.99, "features": np.random.rand(128)},
]

with db.transaction():
    for prod in products:
        v = db.new_vertex("Product")
        v.set("id", prod["id"])
        v.set("name", prod["name"])
        v.set("category", prod["category"])
        v.set("price", prod["price"])
        v.set("features", to_java_float_array(prod["features"]))
        v.save()
        index.add_vertex(v)

# Hybrid search: vector similarity + filters
query_features = np.random.rand(128)
candidates = index.find_nearest(query_features, k=100)  # Get many candidates

# Filter by category and price
filtered_results = []
for vertex, distance in candidates:
    category = vertex.get("category")
    price = float(vertex.get("price"))

    if category == "Electronics" and price < 500:
        filtered_results.append((vertex, distance))

    if len(filtered_results) >= 5:  # Want top 5 after filtering
        break

print("Filtered Results:")
for vertex, distance in filtered_results:
    print(f"{vertex.get('name')} - ${vertex.get('price')} - {distance:.4f}")

db.close()
```

---

### Image Similarity Search

```python
import arcadedb_embedded as arcadedb
from arcadedb_embedded import to_java_float_array
from PIL import Image
import numpy as np

# Assuming you have a function to generate image embeddings
def get_image_embedding(image_path):
    """
    Generate embedding for image using your model
    (e.g., ResNet, CLIP, etc.)
    """
    # Placeholder - use your actual embedding model
    return np.random.rand(512)  # Example: 512-dim embedding

db = arcadedb.create_database("./image_search")

# Schema
with db.transaction():
    db.command("sql", "CREATE VERTEX TYPE Image")
    db.command("sql", "CREATE PROPERTY Image.id STRING")
    db.command("sql", "CREATE PROPERTY Image.filename STRING")
    db.command("sql", "CREATE PROPERTY Image.path STRING")
    db.command("sql", "CREATE PROPERTY Image.embedding ARRAY_OF_FLOATS")

# Create index
index = db.create_vector_index(
    vertex_type="Image",
    vector_property="embedding",
    dimensions=512,
    distance_function="cosine",
    m=24,  # Higher for image search
    ef=200
)

# Index images
image_files = ["img1.jpg", "img2.jpg", "img3.jpg"]

with db.transaction():
    for idx, img_file in enumerate(image_files):
        embedding = get_image_embedding(img_file)

        v = db.new_vertex("Image")
        v.set("id", f"img_{idx}")
        v.set("filename", img_file)
        v.set("path", f"/images/{img_file}")
        v.set("embedding", to_java_float_array(embedding))
        v.save()

        index.add_vertex(v)

# Search for similar images
query_image = "query.jpg"
query_embedding = get_image_embedding(query_image)

similar_images = index.find_nearest(query_embedding, k=5)

print(f"Similar images to {query_image}:")
for vertex, distance in similar_images:
    print(f"  {vertex.get('filename')} - similarity: {1 - distance:.4f}")

db.close()
```

---

## Performance Tuning

### HNSW Parameters

**M (connections per node):**

- **Lower (8-12)**: Faster build, less memory, lower recall
- **Medium (16-24)**: Balanced (recommended)
- **Higher (32-48)**: Better recall, more memory, slower build

**ef (search size):**

- **Lower (50-100)**: Faster search, lower recall
- **Medium (128-200)**: Balanced (recommended)
- **Higher (200-400)**: Better recall, slower search

**ef_construction:**

- **Lower (100-150)**: Faster build, lower quality
- **Medium (128-256)**: Balanced
- **Higher (300-500)**: Better quality, slower build

### Distance Functions

**Cosine Distance:**

- Best for: Text embeddings, normalized vectors
- Range: [0, 2], lower is better
- Use when: Direction matters more than magnitude

**Euclidean Distance:**

- Best for: Image embeddings, spatial data
- Range: [0, ∞), lower is better
- Use when: Absolute distance matters

**Inner Product:**

- Best for: Collaborative filtering, when vectors aren't normalized
- Range: (-∞, ∞), higher is better (note: inverted!)
- Use when: Magnitude information is important

### Memory Considerations

Approximate memory per vertex:

```
memory_per_vertex = dimensions * 4 bytes + M * 8 bytes + overhead
```

Example for 384-dim vectors with M=16:

```
384 * 4 + 16 * 8 + ~100 bytes ≈ 1.8 KB per vertex
```

For 1 million vectors: ~1.8 GB RAM

---

## Error Handling

```python
from arcadedb_embedded import ArcadeDBError, to_java_float_array
import numpy as np

try:
    # Dimension mismatch
    index = db.create_vector_index("Doc", "emb", dimensions=384)

    v = db.new_vertex("Doc")
    v.set("emb", to_java_float_array(np.random.rand(512)))  # Wrong size!
    v.save()
    index.add_vertex(v)  # Will fail

except ArcadeDBError as e:
    print(f"Error: {e}")
    # Handle dimension mismatch

try:
    # Missing vector property
    v = db.new_vertex("Doc")
    v.set("id", "doc1")
    # Forgot to set embedding!
    v.save()
    index.add_vertex(v)  # Will fail

except ArcadeDBError as e:
    print(f"Error: {e}")
    # Handle missing property
```

---

## See Also

- [Vector Search Guide](../guide/vectors.md) - Comprehensive vector search strategies
- [Vector Examples](../examples/vectors.md) - More practical examples
- [Database API](database.md) - Database operations
- [Query Guide](../guide/core/queries.md) - Combining vectors with queries
