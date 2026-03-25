from sentence_transformers import SentenceTransformer
import chromadb

# Initialize ChromaDB client
client = chromadb.PersistentClient(path="./chroma_db/")
col = client.get_collection("geo_audit_1739828886") # I will just query the most recent collection or re-embed

# Actually it's better to just use sentence transformers locally
model = SentenceTransformer("intfloat/multilingual-e5-large", cache_folder="./hf_cache")

# Client content
c_text = "Roma a Domicilio è l'App con cui consultare, ordinare e comprare dai migliori negozi di Roma. Consegniamo cibo caldo e spesa a domicilio."

# Competitor / Gaps
ents = [
    "Uber Eats",
    "Tracciamento in tempo reale del rider",
    "Selezione di alta qualità nei ristoranti partner",
    "Modelli economici di sostenibilità a lungo termine",
    "Packaging ecologico integrato"
]

c_vec = model.encode([c_text])
e_vecs = model.encode(ents)

print("Distances against Client Content snippet:")
from scipy.spatial.distance import cosine
for i, e in enumerate(ents):
    dist = cosine(c_vec[0], e_vecs[i])
    print(f"[{e}]: {dist:.4f}")

