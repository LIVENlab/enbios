"""
An authenticator is in charge of receiving some credentials to LOGIN
As a result, some demonstration of the success is obtained
It can be an OAuth2 Bearer Token
Or an API Key ("X-api-key" header; provided by Backend)
Or some other

"""
from abc import ABCMeta, abstractmethod

from nexinfosys.common.helper import create_dictionary


class IAuthenticator(metaclass=ABCMeta):
    @abstractmethod
    def get_name(self) -> str:
        """ Authenticator name """
        pass

    @abstractmethod
    def check(self, request) -> str:
        """ Checks Cookies or Headers for existing "passport" or similar.
            If existent, checks for the validity.
            If valid, obtains data allowing to match with some identity
        :param request: The Request object as received by Flask
        :return Information on what happened (if the passport is present or not;
        if it is valid; the information to match with identity)
        """
        pass

    def elaborate_from_credentials(self, credentials):
        """
        Elaborate passport from credentials
        TODO Is the passport stored in some standard place, or return just the structure
        :param credentials:
        :return:
        """


class AuthenticatorsManager:
    def __init__(self):
        self.registry = create_dictionary()

    def check(self, request) -> str:
        for s in self.registry:
            d = self.registry[s].check(request)
            if d.get("exists", False):
                # Continue
                pass
            else:
                d = None
                break
        return d

    