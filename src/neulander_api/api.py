import os
from contextlib import asynccontextmanager
from typing import Dict

import neulander_core.models.medcat as m
from dotenv import find_dotenv, load_dotenv
from fastapi import Depends, FastAPI
from faststream.rabbit import RabbitBroker, RabbitQueue
from faststream.rabbit.fastapi import Logger, RabbitRouter
from neulander_core.config import Settings, WorkerQueues
from neulander_core.models.core import PublishResponse
from pamqp.commands import Basic
from typing_extensions import Annotated

from neulander_api import __version__

load_dotenv(find_dotenv())


cfg = Settings(
    rabbitmq_host=os.getenv("RABBITMQ_HOST", "localhost"),
    rabbitmq_username=os.getenv("RABBITMQ_USERNAME", "neulander"),
    rabbitmq_password=os.getenv("RABBITMQ_PASSWORD", "neulander"),
    rabbitmq_port=os.getenv("RABBITMQ_PORT", default="5672"),
    fastapi_debug=os.getenv("FASTAPI_DEBUG", False),
)
print(cfg.rabbitmq_connection_string)
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
q_presidio = WorkerQueues(worker_name="presidio")


async def publish(
    broker: RabbitBroker, queue: RabbitQueue, message: Dict
) -> PublishResponse:
    """
    Publish a message to a specified RabbitMQ queue using the provided broker.

    Args:
        broker (RabbitBroker): The RabbitMQ broker instance used for publishing the message.
        queue (RabbitQueue): The RabbitMQ queue to which the message will be published.
        message (Dict): The message to be published, represented as a dictionary.

        PublishResponse: An object containing the ID of the message, the response status (Ack or Nack),
                        and the name of the queue to which the message was published.
    """

    response: Basic.Ack | Basic.Nack = await broker.publish(
        message=message,
        queue=queue,
        mandatory=True,
        immediate=False,
    )

    out = PublishResponse(id=message.id, response=response.name, queue=queue.name)

    return out


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Context manager for the FastAPI application lifespan.

    This context manager ensures that RabbitMQ queues are created if they don't already exist
    when the FastAPI application starts, and performs any necessary cleanup when the application stops.
    """
    # Create RabbitMQ Queues if they don't already exist
    await rabbit_router.broker.connect()

    await rabbit_router.broker.declare_queue(q_medcat_14.qin)
    await rabbit_router.broker.declare_queue(q_medcat_14.qout)
    await rabbit_router.broker.declare_queue(q_medcat_14.qerr)

    await rabbit_router.broker.declare_queue(q_presidio.qin)
    await rabbit_router.broker.declare_queue(q_presidio.qout)
    await rabbit_router.broker.declare_queue(q_presidio.qerr)

    yield


app = FastAPI(lifespan=lifespan, debug=cfg.fastapi_debug)


@app.get("/")
def root():
    """
    Root endpoint that returns the version and status of the NeuLANDER API.
    Returns:
        dict: A dictionary containing the NeuLANDER API version and status.
    """

    return {"neulander_api_version": __version__, "status": "ok"}


@app.post(
    path="/rabbit/medcat_14",
    description="NER+L using MedCAT version 1.4",
    tags=["MedCAT"],
    response_model=PublishResponse,
)
async def medcat_14(
    message: m.DocIn, logger: Logger, broker: Annotated[RabbitBroker, Depends(broker)]
):
    response = await publish(broker, q_medcat_14.qin, message)

    logger.info(f"Published message: {response}")
    return response


@app.post(
    path="/rabbit/presidio",
    name="De-Id using Presidio",
    description="Deidentification using Microsoft Presidio",
    tags=["De-Id"],
    response_model=PublishResponse,
)
async def deid_presidio(
    message: m.DocIn, logger: Logger, broker: Annotated[RabbitBroker, Depends(broker)]
):
    response = await publish(broker, q_presidio.qin, message)
    logger.info(f"Published message: {response.model_dump_json()}")
    return response


@rabbit_router.subscriber(queue=q_medcat_14.qout)
async def print_messages_from_out_queue(msg: dict, logger: Logger):
    logger.info(msg=msg)


app.include_router(router=rabbit_router, tags=["RabbitMQ"])
