class ColumnsError(Exception):
    """
    Exeption raised when columns are not the expected
    """
    def __init__(self, message):
        super().__init__(f'Error {message}')
        pass




