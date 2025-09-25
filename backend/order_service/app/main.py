import asyncio
import json
import logging
import os
import sys
import time
from decimal import Decimal
from typing import List, Optional

import aio_pika
import httpx
from fastapi import Depends, FastAPI, HTTPException, Query, Response, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, joinedload

from .db import Base, SessionLocal, engine, get_db
from .models import Order, OrderItem
from .schemas import (
    OrderCreate,
    OrderItemResponse,
    OrderResponse,
    OrderStatusUpdate,
    OrderUpdate,
)

# --- Standard Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Reduce noise from third-party libs
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("uvicorn.error").setLevel(logging.INFO)

# --- Service URLs Configuration (exported for tests to import) ---
# Typical local defaults: customer=8001, product=8002 (adjust if your repo uses different ports).
CUSTOMER_SERVICE_URL = os.getenv("CUSTOMER_SERVICE_URL", "http://localhost:8001")
PRODUCT_SERVICE_URL = os.getenv("PRODUCT_SERVICE_URL", "http://localhost:8002")

logger.info(
    "Order Service configured with CUSTOMER_SERVICE_URL=%s, PRODUCT_SERVICE_URL=%s",
    CUSTOMER_SERVICE_URL,
    PRODUCT_SERVICE_URL,
)

# --- RabbitMQ Configuration ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")

# Global RabbitMQ connection and channel objects
rabbitmq_connection: Optional[aio_pika.Connection] = None
rabbitmq_channel: Optional[aio_pika.Channel] = None
rabbitmq_exchange: Optional[aio_pika.Exchange] = None

# --- FastAPI Application Setup ---
app = FastAPI(
    title="Order Service API",
    description="Manages orders for mini-ecommerce app, with synchronous stock deduction.",
    version="1.0.0",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- RabbitMQ Helper Functions ---
async def connect_to_rabbitmq():
    """Establishes an asynchronous connection to RabbitMQ."""
    global rabbitmq_connection, rabbitmq_channel, rabbitmq_exchange

    rabbitmq_url = (
        f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASS}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/"
    )
    max_retries = 10
    retry_delay_seconds = 5

    for i in range(max_retries):
        try:
            logger.info(
                "Order Service: Attempting to connect to RabbitMQ (attempt %s/%s)...",
                i + 1,
                max_retries,
            )
            rabbitmq_connection = await aio_pika.connect_robust(rabbitmq_url)
            rabbitmq_channel = await rabbitmq_connection.channel()
            rabbitmq_exchange = await rabbitmq_channel.declare_exchange(
                "ecomm_events", aio_pika.ExchangeType.DIRECT, durable=True
            )
            logger.info(
                "Order Service: Connected to RabbitMQ and declared 'ecomm_events' exchange."
            )
            return True
        except Exception as e:
            logger.warning("Order Service: Failed to connect to RabbitMQ: %s", e)
            if i < max_retries - 1:
                await asyncio.sleep(retry_delay_seconds)
            else:
                logger.critical(
                    "Order Service: Failed to connect to RabbitMQ after %s attempts. "
                    "RabbitMQ functionality will be limited.",
                    max_retries,
                )
                return False
    return False


async def close_rabbitmq_connection():
    """Closes the RabbitMQ connection."""
    if rabbitmq_connection:
        logger.info("Order Service: Closing RabbitMQ connection.")
        await rabbitmq_connection.close()


async def publish_event(routing_key: str, message_data: dict):
    """Publishes a message to the RabbitMQ exchange."""
    if not rabbitmq_exchange:
        logger.error(
            "Order Service: RabbitMQ exchange not available. Cannot publish event '%s'.",
            routing_key,
        )
        return
    try:
        message_body = json.dumps(message_data).encode("utf-8")
        message = aio_pika.Message(
            body=message_body,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await rabbitmq_exchange.publish(message, routing_key=routing_key)
        logger.info(
            "Order Service: Published event '%s' with data: %s",
            routing_key,
            message_data,
        )
    except Exception as e:
        logger.error(
            "Order Service: Failed to publish event '%s': %s", routing_key, e, exc_info=True
        )


async def consume_stock_events(db_session_factory: Session):
    if not rabbitmq_channel or not rabbitmq_exchange:
        logger.error(
            "Order Service: RabbitMQ channel or exchange not available for consuming stock events."
        )
        return

    stock_deducted_queue_name = "order_service_stock_deducted_queue"
    stock_deduction_failed_queue_name = "order_service_stock_deduction_failed_queue"

    try:
        stock_deducted_queue = await rabbitmq_channel.declare_queue(
            stock_deducted_queue_name, durable=True
        )
        await stock_deducted_queue.bind(
            rabbitmq_exchange, routing_key="product.stock.deducted"
        )

        stock_deduction_failed_queue = await rabbitmq_channel.declare_queue(
            stock_deduction_failed_queue_name, durable=True
        )
        await stock_deduction_failed_queue.bind(
            rabbitmq_exchange, routing_key="product.stock.deduction.failed"
        )

        async def process_message(message: aio_pika.abc.AbstractIncomingMessage):
            async with message.process():
                try:
                    message_data = json.loads(message.body.decode("utf-8"))
                    routing_key = message.routing_key
                    order_id = message_data.get("order_id")

                    if not order_id:
                        logger.error(
                            "Order Service: Received message with no order_id: %s",
                            message_data,
                        )
                        return

                    local_db_session = db_session_factory()
                    try:
                        db_order = (
                            local_db_session.query(Order)
                            .filter(Order.order_id == order_id)
                            .first()
                        )
                        if not db_order:
                            logger.warning(
                                "Order Service: Received event for non-existent order ID: %s. Routing key: %s.",
                                order_id,
                                routing_key,
                            )
                            return

                        if routing_key == "product.stock.deducted":
                            db_order.status = "confirmed"
                        elif routing_key == "product.stock.deduction.failed":
                            db_order.status = "failed"
                        else:
                            logger.warning(
                                "Order Service: Unknown routing key '%s' for order %s.",
                                routing_key,
                                order_id,
                            )
                            return

                        local_db_session.add(db_order)
                        local_db_session.commit()
                        local_db_session.refresh(db_order)
                        logger.info(
                            "Order Service: Order %s status updated to %s.",
                            order_id,
                            db_order.status,
                        )

                    except Exception as db_e:
                        local_db_session.rollback()
                        logger.critical(
                            "Order Service: DB error updating order %s status: %s",
                            order_id,
                            db_e,
                            exc_info=True,
                        )
                    finally:
                        local_db_session.close()

                except json.JSONDecodeError as e:
                    logger.error(
                        "Order Service: Failed to decode RabbitMQ message body: %s. Message: %s",
                        e,
                        message.body,
                    )
                except Exception as e:
                    logger.error(
                        "Order Service: Unhandled error processing stock event: %s",
                        e,
                        exc_info=True,
                    )

        await asyncio.gather(
            stock_deducted_queue.consume(process_message),
            stock_deduction_failed_queue.consume(process_message),
        )

    except Exception as e:
        logger.critical(
            "Order Service: Error in RabbitMQ consumer for stock events: %s",
            e,
            exc_info=True,
        )


# --- FastAPI Event Handlers ---
@app.on_event("startup")
async def startup_event():
    max_retries = 10
    retry_delay_seconds = 5
    for i in range(max_retries):
        try:
            logger.info(
                "Order Service: Attempting to connect to PostgreSQL and create tables (attempt %s/%s)...",
                i + 1,
                max_retries,
            )
            Base.metadata.create_all(bind=engine)
            logger.info(
                "Order Service: Successfully connected to PostgreSQL and ensured tables exist."
            )
            break
        except OperationalError as e:
            logger.warning("Order Service: Failed to connect to PostgreSQL: %s", e)
            if i < max_retries - 1:
                logger.info("Order Service: Retrying in %s seconds...", retry_delay_seconds)
                time.sleep(retry_delay_seconds)
            else:
                logger.critical(
                    "Order Service: Failed to connect to PostgreSQL after %s attempts. Exiting.",
                    max_retries,
                )
                sys.exit(1)
        except Exception as e:
            logger.critical(
                "Order Service: Unexpected error during DB startup: %s", e, exc_info=True
            )
            sys.exit(1)

    if await connect_to_rabbitmq():
        asyncio.create_task(consume_stock_events(SessionLocal))
    else:
        logger.error(
            "Order Service: RabbitMQ connection failed at startup. Async order processing will not work."
        )


@app.on_event("shutdown")
async def shutdown_event():
    await close_rabbitmq_connection()


# --- Root & Health ---
@app.get("/", status_code=status.HTTP_200_OK, summary="Root endpoint")
async def read_root():
    return {"message": "Welcome to the Order Service!"}


@app.get("/health", status_code=status.HTTP_200_OK, summary="Health check endpoint")
async def health_check():
    return {"status": "ok", "service": "order-service"}


# --- Create Order ---
@app.post(
    "/orders/",
    response_model=OrderResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new order and publish 'order.placed' event for stock deduction",
)
async def create_order(order: OrderCreate, db: Session = Depends(get_db)):
    if not order.items:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Order must contain at least one item.",
        )

    # Validate customer via Customer Service
    async with httpx.AsyncClient() as client:
        customer_validation_url = f"{CUSTOMER_SERVICE_URL}/customers/{order.user_id}"
        logger.info(
            "Order Service: Validating customer ID %s via Customer Service at %s",
            order.user_id,
            customer_validation_url,
        )
        try:
            response = await client.get(customer_validation_url, timeout=3)
            response.raise_for_status()
            customer_data = response.json()

            if not order.shipping_address and customer_data.get("shipping_address"):
                order.shipping_address = customer_data["shipping_address"]

        except httpx.HTTPStatusError as e:
            if e.response.status_code == status.HTTP_404_NOT_FOUND:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid user_id: Customer {order.user_id} not found.",
                )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to validate customer with Customer Service.",
            )
        except httpx.RequestError:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Customer Service is currently unavailable. Please try again later.",
            )

    total_amount = sum(
        Decimal(str(item.quantity)) * Decimal(str(item.price_at_purchase))
        for item in order.items
    )

    db_order = Order(
        user_id=order.user_id,
        shipping_address=order.shipping_address,
        total_amount=total_amount,
        status="pending",
    )

    db.add(db_order)
    db.flush()  # get order_id before commit

    for item in order.items:
        db_order_item = OrderItem(
            order_id=db_order.order_id,
            product_id=item.product_id,
            quantity=item.quantity,
            price_at_purchase=item.price_at_purchase,
            item_total=Decimal(str(item.quantity)) * Decimal(str(item.price_at_purchase)),
        )
        db.add(db_order_item)

    try:
        db.commit()
        db.refresh(db_order)
        db.refresh(db_order, attribute_names=["items"])

        order_event_data = {
            "order_id": db_order.order_id,
            "user_id": db_order.user_id,
            "total_amount": float(db_order.total_amount),
            "items": [
                {
                    "product_id": item.product_id,
                    "quantity": item.quantity,
                    "price_at_purchase": float(item.price_at_purchase),
                }
                for item in db_order.items
            ],
            "order_date": db_order.order_date.isoformat(),
            "status": db_order.status,
        }
        await publish_event("order.placed", order_event_data)

        return db_order
    except Exception as e:
        db.rollback()
        logger.error("Order Service: Error creating order or publishing event: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not create order or publish event. Please check logs.",
        )


# --- Read/List/Update/Delete ---
@app.get("/orders/", response_model=List[OrderResponse], summary="Retrieve a list of all orders")
def list_orders(
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    user_id: Optional[int] = Query(None, ge=1, description="Filter orders by user ID."),
    status: Optional[str] = Query(
        None,
        pattern="^(pending|processing|shipped|cancelled|confirmed|completed|failed)$",
    ),
):
    logger.info(
        "Order Service: Listing orders (skip=%s, limit=%s, user_id=%s, status=%s)",
        skip,
        limit,
        user_id,
        status,
    )
    query = db.query(Order).options(joinedload(Order.items))

    if user_id:
        query = query.filter(Order.user_id == user_id)
    if status:
        query = query.filter(Order.status == status)

    return query.offset(skip).limit(limit).all()


@app.get("/orders/{order_id}", response_model=OrderResponse, summary="Retrieve a single order by ID")
def get_order(order_id: int, db: Session = Depends(get_db)):
    order = (
        db.query(Order)
        .options(joinedload(Order.items))
        .filter(Order.order_id == order_id)
        .first()
    )
    if not order:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")
    return order


@app.patch("/orders/{order_id}/status", response_model=OrderResponse, summary="Update the status of an order")
async def update_order_status(
    order_id: int, new_status: OrderStatusUpdate, db: Session = Depends(get_db)
):
    db_order = db.query(Order).filter(Order.order_id == order_id).first()
    if not db_order:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")

    db_order.status = new_status
    try:
        db.add(db_order)
        db.commit()
        db.refresh(db_order)
        db.refresh(db_order, attribute_names=["items"])
        return db_order
    except Exception as e:
        db.rollback()
        logger.error("Order Service: Error updating status for order %s: %s", order_id, e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not update order status.",
        )


@app.delete("/orders/{order_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete an order by ID")
def delete_order(order_id: int, db: Session = Depends(get_db)):
    order = db.query(Order).filter(Order.order_id == order_id).first()
    if not order:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")

    try:
        db.delete(order)
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error("Order Service: Error deleting order %s: %s", order_id, e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while deleting the order.",
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/orders/{order_id}/items", response_model=List[OrderItemResponse], summary="Retrieve all items for a specific order")
def get_order_items(order_id: int, db: Session = Depends(get_db)):
    order = db.query(Order).filter(Order.order_id == order_id).first()
    if not order:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")
    return order.items
