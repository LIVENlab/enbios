class ColumnsError(Exception):
    """
    Exeption raised when columns are not the expected
    """
    def __init__(self, message):
        super().__init__(f'Error {message}')
        pass



class ColumnError(KeyError):
    def __init__(self,message,key):
        super().__init__(message,key)
