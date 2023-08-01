from scipy.cluster.hierarchy import linkage, fcluster
from sklearn.cluster import KMeans

from enbios2.experiment.bw_vector_db.create_vectors import get_all_vector_docs
from enbios2.experiment.bw_vector_db.psql_vectorDB import Document


def kmeans(docs: list[Document], k: int = 35):
    kmeans = KMeans(n_clusters=k, random_state=0).fit([doc.embedding for doc in docs])
    return kmeans

def hierarchical_clustering(docs: list[Document]):
    Z = linkage([doc.embedding for doc in docs], method='ward')
    # Plotting dendrogram
    # plt.figure(figsize=(25, 10))
    # plt.title('Hierarchical Clustering Dendrogram')
    # plt.xlabel('sample index')
    # plt.ylabel('distance')
    # dendrogram(
    #     Z,
    #     leaf_rotation=90.,  # rotates the x axis labels
    #     leaf_font_size=8.,  # font size for the x axis labels
    # )
    # plt.show()
    # return Z
    import plotly.figure_factory as ff
    fig = ff.create_dendrogram(Z)
    fig.update_layout(width=800, height=500)
    fig.write_html("dendogram.html")
    # fig.show()
    return Z


def hierarchical(Z):
    max_d = 1  # max_d as in max_distance
    clusters = fcluster(Z, max_d, criterion='distance')

    # Extract keywords for each cluster
    # vectorizer = TfidfVectorizer()
    # unique_clusters = set(clusters)
    # for i in unique_clusters:
    #     cluster_sentences = [sent for sent, cluster in zip(sentences, clusters) if cluster == i]
    #     X = vectorizer.fit_transform(cluster_sentences)
    #     keywords = vectorizer.get_feature_names_out()[X.sum(axis=0).argmax()]
    #     print(f"Cluster {i} keywords: {keywords}")
    return clusters


if __name__ == "__main__":
    print("Running clustering.py")
    docs = get_all_vector_docs()
    k500 = kmeans(docs, 500)
