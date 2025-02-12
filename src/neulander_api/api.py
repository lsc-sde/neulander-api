import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from enum import Enum
from typing import Dict

from fastapi import Depends, FastAPI, HTTPException
from faststream.rabbit import RabbitBroker, RabbitQueue
from faststream.rabbit.fastapi import Logger, RabbitRouter
from neulander_core.config import WorkerQueues
from neulander_core.config import settings as cfg
from neulander_core.schema.core import AzureBlobDocIn, PublishResponse
from pamqp.commands import Basic
from typing_extensions import Annotated

from neulander_api import __version__
from neulander_api.auth import get_api_key

rabbit_router = RabbitRouter(cfg.rabbitmq_connection_string)


def broker():
    """
    Retrieve the broker instance from the rabbit_router.

    Returns:
        Broker: The broker instance used for messaging.
    """
    return rabbit_router.broker


# Setup RabbitMQ Queues

q_medcat_14 = WorkerQueues(worker_name="medcat_14")


async def publish(
    broker: RabbitBroker, queue: RabbitQueue, message: Dict
) -> PublishResponse:
    """
    Publish a message to a RabbitMQ queue.
    Args:
        broker (RabbitBroker): The RabbitMQ broker instance to use for publishing.
        queue (RabbitQueue): The RabbitMQ queue to which the message will be published.
        message (Dict): The message to be published, represented as a dictionary.
    Returns:
        PublishResponse: An object containing details about the publish operation, including
        the document ID, response status, queue name, and submission timestamp.
    Raises:
        Exception: If the publish operation fails.
    """

    response: Basic.Ack | Basic.Nack = await broker.publish(
        message=message,
        queue=queue,
        mandatory=True,
        immediate=False,
    )  # type: ignore

    out = PublishResponse(
        docid=message["docid"],
        response=response.name,
        queue_name=queue.name,
        submission_ts=datetime.now(),
    )

    return out


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Context manager for the FastAPI application lifespan.

    This context manager ensures that RabbitMQ queues are created if they don't already exist
    when the FastAPI application starts, and performs any necessary cleanup when the application stops.
    """
    # Check if rabbitmq broker is available
    # If not, try 2 more times before raising an exception
    retries = 3
    for attempt in range(retries):
        try:
            await rabbit_router.broker.connect()
            break
        except Exception as e:
            if attempt < retries - 1:
                backoff = 15 + attempt * 15
                # Todo: Replace with logger
                logging.warning(
                    f"Retrying connection to RabbitMQ ({attempt + 1}/{retries}) in {backoff} seconds: {e}"
                )
                await asyncio.sleep(backoff)  # exponential(ish) backoff before retrying
            else:
                logging.error(
                    f"Failed to connect to RabbitMQ after {retries} attempts: {e}"
                )
                raise

    # Create RabbitMQ Queues if they don't already exist
    await rabbit_router.broker.declare_queue(q_medcat_14.qin)
    await rabbit_router.broker.declare_queue(q_medcat_14.qout)
    await rabbit_router.broker.declare_queue(q_medcat_14.qerr)

    yield


app = FastAPI(lifespan=lifespan, debug=cfg.fastapi_debug)

###############################################################################
# FastAPI Routes
###############################################################################


@app.get("/")
def root():
    """
    Root endpoint that returns the version and status of the NeuLANDER API.
    Returns:
        dict: A dictionary containing the NeuLANDER API version and status.
    """

    return {"neulander_api_version": __version__, "status": "ok"}


###############################################################################
# MedCAT
###############################################################################


class MedCATModelVersion(str, Enum):
    V_1_4 = "1.4"
    V_1_12 = "1.12"


available_medcat_models = [MedCATModelVersion.V_1_4]


@app.post(
    path="/medcat/{medcat_model_version}",
    description="NER+L using MedCAT version 1.4",
    tags=["MedCAT"],
    response_model=PublishResponse,
)
async def medcat_annotate(
    message: AzureBlobDocIn,
    medcat_model_version: MedCATModelVersion,
    logger: Logger,
    broker: Annotated[RabbitBroker, Depends(broker)],
    api_key_valid: bool = Depends(get_api_key),
):
    """
    Asynchronously annotates a message using the specified MedCAT model version and publishes the result to a RabbitMQ broker.
    Args:
        message (AzureBlobDocIn): The message to be annotated.
        medcat_model_version (MedCATModelVersion): The version of the MedCAT model to use for annotation.
        logger (Logger): The logger instance for logging information.
        broker (Annotated[RabbitBroker, Depends(broker)]): The RabbitMQ broker dependency.
    Returns:
        The response from the RabbitMQ broker after publishing the annotated message.
    Raises:
        HTTPException: If the specified MedCAT model version is not found in the available versions.
    """

    if medcat_model_version == MedCATModelVersion.V_1_4:
        response = await publish(broker, q_medcat_14.qin, message.model_dump())
    else:
        raise HTTPException(
            status_code=400,
            detail=f"MedCAT model version {medcat_model_version} not found in available versions - [{available_medcat_models}]",
        )

    logger.info(f"Published message: {response.model_dump_json()}")

    return response


###############################################################################
# Presidio DeID
###############################################################################


# @app.post(
#     path="/rabbit/presidio",
#     name="De-Id using Presidio",
#     description="Deidentification using Microsoft Presidio",
#     tags=["De-Id"],
#     response_model=PublishResponse,
# )
# async def deid_presidio(
#     message: m.DocIn, logger: Logger, broker: Annotated[RabbitBroker, Depends(broker)]
# ):
#     response = await publish(broker, q_presidio.qin, message)
#     logger.info(f"Published message: {response.model_dump_json()}")
#     return response

###############################################################################
# FastStream Endpoints
###############################################################################


@rabbit_router.subscriber(queue=q_medcat_14.qout)
async def print_messages_from_out_queue(msg: dict, logger: Logger):
    logger.info(msg=msg)


app.include_router(router=rabbit_router, tags=["RabbitMQ"])
