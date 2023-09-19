from typing import Optional

import bw2data
import numpy as np

from bw2data.backends import Activity
from sentence_transformers import SentenceTransformer
from sklearn.manifold import TSNE
from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import Session
from tqdm import tqdm

from enbios2.base.db_models import EcoinventDataset
from enbios2.bw2 import set_bw_current_project
from enbios2.experiment.bw_vector_db.psql_vectorDB import engine, Document, reset_vector_db

set_bw_current_project(EcoinventDataset._SM_CUTOFF, EcoinventDataset._V391)


# print(bw2data.projects.current)


class VectorDBConfig:
    """
    class to store static model
    """
    model = None

    @classmethod
    def get_model(clzz, model_name: str = 'all-MiniLM-L6-v2'):
        """
        load model if not loaded
        :param model_name:
        :return:
        """
        if not clzz.model:
            print("loading model")
            clzz.model = SentenceTransformer(model_name)
        return clzz.model


def insert_db_my_batch(db_name: str, batch_size: int = 1000):
    def process_batch(batch: list[Activity]):
        embeddings = VectorDBConfig.get_model().encode([act["name"] for act in batch])
        with Session(engine) as session:
            for i, embedding in enumerate(embeddings):
                item = Document(content=batch[i]["name"], ext_ids=batch[i]["id"], embedding=embedding.tolist())
                session.add(item)
            session.commit()

    db = bw2data.Database(db_name)

    batch = []
    db_size = len(db)
    for act in tqdm(db, total=db_size):
        act['name']
        # print(act)
        batch.append(act)
        if len(batch) == batch_size:
            process_batch(batch)
            batch = []

    if len(batch) > 0:
        process_batch(batch)


def add_content(content: str, id: int = -1):
    embedding = VectorDBConfig.get_model().encode(content)
    with Session(engine) as session:
        item = Document(content=content, ext_id=id, embedding=embedding.tolist())
        session.add(item)
        session.commit()


def insert_db(db_name: str, normalize: bool = False):
    db = bw2data.Database(db_name)
    contents2ids: dict[str, list[int]] = {}
    for doc in db:
        contents2ids.setdefault(doc["name"], []).append(doc["id"])
    model = VectorDBConfig.get_model()
    contents = list(contents2ids.keys())
    print(f"{len(db)} docs ({len(contents)} names)")
    embeddings = model.encode(contents, show_progress_bar=True)
    if normalize:
        norm_embeddings = VectorDBConfig.get_model().encode(contents, show_progress_bar=True, normalize_embeddings=True)
    with Session(engine) as session:
        for i, embedding in enumerate(embeddings):
            item = Document(content=contents[i], ext_ids=contents2ids[contents[i]],
                            embedding=embedding.tolist())
            if normalize:
                item.norm_embedding = norm_embeddings[i].tolist()
            session.add(item)
        print("committing")
        session.commit()


def query(query_str: str, limit: int = 5):
    """
    make a query to the vector db
    :param query_str:
    :param limit:
    :return:
    """
    embedding = VectorDBConfig.get_model().encode(query_str)
    with Session(engine) as session:
        neighbors = session.query(Document).order_by(
            Document.embedding.cosine_distance(embedding)).limit(limit).all()
        return [(n.content, n.ext_ids) for n in neighbors]


def tsne_docs(docs: list[Document]):
    tsne_ak_2d = TSNE(perplexity=30, n_components=2, init='pca', n_iter=3500, random_state=64, n_jobs=-1)
    embeddings_ak_2d = tsne_ak_2d.fit_transform(np.array(np.array([doc.embedding for doc in docs])))
    return embeddings_ak_2d


def umap_docs(docs: list[Document], use_norm: bool = False, assign_to: Optional[str] = None):
    import umap.umap_ as umap
    reducer = umap.UMAP(random_state=42)
    embeddings = [doc.norm_embedding if use_norm else doc.embedding for doc in docs]
    umap_res = reducer.fit_transform(embeddings)
    if assign_to:
        with Session(engine) as session:
            for i, doc in enumerate(docs):
                setattr(doc, assign_to, umap_res[i])
                flag_modified(doc, assign_to)
                session.add(doc)
            session.commit()
    return umap_res


def get_all_vector_docs() -> list[Document]:
    with Session(engine) as session:
        return session.query(Document).all()

# Call the function with your data
# plot_embeddings(reduced_embeddings, labels)

# uu = umap_docs(get_all(),False, "embedding_2d")
# uu = umap_docs(get_all(),True, "norm_embedding_2d")
# docs = get_all()
# tsne_html(np.array([doc.embedding_2d for doc in docs]),
#           [doc.content for doc in docs], "umap.html")
#
# tsne_html(np.array([doc.norm_embedding_2d for doc in docs]),
#           [doc.content for doc in docs], "umap_norm.html")

# hierarchical_clustering(docs)
# print("done")

if __name__ == "__main__":
    reset_vector_db()
    insert_db("cutoff391")
    while True:
        text = input("Enter text: ")
        res = query(text)
        print(res)
