import logging
import os

from contextlib import asynccontextmanager
from fastapi import FastAPI

from aero import GLOBUS_CLIENT
from aero import init_client
from aero.config import Config
from aero.routers import data
from aero.routers import flow
from aero.routers import provenance

from aero.database import create_db_and_tables

# uvicorn configures only its own loggers ("uvicorn", "uvicorn.error",
# "uvicorn.access") and leaves the root logger without a handler, so anything the
# app logs below WARNING is silently dropped. Give root a handler so
# `logging.getLogger(__name__)` in any module reaches the container's output.
# Its log config sets disable_existing_loggers=False, so this survives.
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()

    if Config.SEARCH_ENABLED:
        if GLOBUS_CLIENT.search_index is None:
            GLOBUS_CLIENT._create_search_idx()
    else:
        logging.getLogger(__name__).warning(
            "Globus Search is disabled (AERO_SEARCH_ENABLED); versions will not be "
            "indexed and GET /data/search will not return them."
        )

    yield


# app = FastAPI(dependencies=[Depends(get_query_token)])
if GLOBUS_CLIENT is None:
    init_client()

app = FastAPI(lifespan=lifespan)

app.include_router(data.router)
app.include_router(data.webhook_router)
app.include_router(flow.router)
app.include_router(provenance.router)
