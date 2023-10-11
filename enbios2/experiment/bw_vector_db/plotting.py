

def plot_tsne(docs: list[Document], projections: ndarray = None):
    """
        didnt work?!
    :param docs:
    :param projections:
    :return:
    """
    if projections is None:
        projections = tsne_docs(docs)
    fig = px.scatter(
        projections, x=0, y=1, hover_name=[doc.content for doc in docs],
        color=["black" for doc in docs], labels={'color': 'name'}
        # color=[doc.content for doc in docs], labels={'color': 'name'}
    )
    fig.show()
    plt.show()
    return fig


def tsne_plot_similar_words(title: str, docs: list[Document], embedding_clusters: ndarray):
    """
    didnt work?!
    :param title:
    :param docs:
    :param embedding_clusters:
    :return:
    """
    figsize = (30, 20) if (matplotlib.get_backend() == 'nbAgg') else (20, 12)  # interactive plot should be smaller
    plt.figure(figsize=(figsize))
    # colors = cm.rainbow(np.linspace(0, 1, len(labels)))
    x = embedding_clusters[:, 0]
    y = embedding_clusters[:, 1]
    plt.scatter(x, y, c="red", alpha=0.5, label="")
    for i, doc in enumerate(docs):
        plt.annotate(doc.content, alpha=0.5, xy=(x[i], y[i]), xytext=(5, 2),
                     textcoords='offset points', ha='right', va='bottom', size=8)
    plt.legend(loc=4)
    plt.title(title)
    plt.grid(True)
    # plt.show()


def tsne_html(embeddings_2d: ndarray, labels: list[str], file_name: str):
    fig = go.Figure(data=go.Scattergl(
        x=embeddings_2d[:, 0],
        y=embeddings_2d[:, 1],
        mode='markers',
        text=labels,  # This adds markers' labels
        marker=dict(
            color=np.random.randn(len(labels)),  # set color equal to a variable
            colorscale='Viridis',  # one of plotly colorscales
            line_width=1
        )
    ))

    # Create HTML file
    fig.write_html(file_name)

