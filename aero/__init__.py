from aero.config import Config
from aero.globus.client import GlobusClient

_client = None


class ClientProxy:
    def __getattr__(self, name):
        if _client is None:
            raise RuntimeError("Application client has not been initialized")
        return getattr(_client, name)


GLOBUS_CLIENT = ClientProxy()


def init_client():
    global _client
    _client = GlobusClient(search_index=Config.SEARCH_INDEX)
