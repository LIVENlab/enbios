from sentence_transformers import SentenceTransformer


class NLPModels:
    """
    class to store static model
    """
    sentence_model = None

    @classmethod
    def get_sentence_model(clzz, model_name: str = 'all-MiniLM-L6-v2'):
        """
        load model if not loaded
        :param model_name:
        :return:
        """
        if not clzz.sentence_model:
            print("loading model")
            clzz.sentence_model = SentenceTransformer(model_name)
        return clzz.sentence_model
