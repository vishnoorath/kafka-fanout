import pytest
import sqlalchemy
from sqlalchemy import select
import uuid
from app.db import init_db, session_scope
from app.models import Env, Outbox

@pytest.mark.asyncio
async def test_outbox_uniqueness():
    # Run DB init and migrations
    await init_db()

    # Use a unique name for env to avoid conflicts with previous tests or runs
    env_name = f"Test Outbox Env {uuid.uuid4()}"

    async with session_scope() as session:
        env = Env(name=env_name, enabled=1, delivery_mode="outbox")
        session.add(env)
        await session.commit()
        env_id = env.id

    try:
        async with session_scope() as session:
            row1 = Outbox(
                env_id=env_id,
                idempotency_key="topic:1:100",
                payload=b"message1",
                headers_json="[]",
                destinations_json="[]"
            )
            session.add(row1)
            await session.commit()

        async with session_scope() as session:
            row2 = Outbox(
                env_id=env_id,
                idempotency_key="topic:1:100",
                payload=b"message2",
                headers_json="[]",
                destinations_json="[]"
            )
            session.add(row2)
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                await session.commit()
    finally:
        # Cleanup env
        async with session_scope() as session:
            await session.execute(
                sqlalchemy.delete(Env).where(Env.id == env_id)
            )
            await session.commit()


@pytest.mark.asyncio
async def test_outbox_flow():
    import asyncio
    from app.db import init_db, session_scope
    from app.models import Env, Outbox, DomainGrouping, Destination, SourceConfig
    from app.runtime.consumer import ConsumerTask
    from app.runtime.outbox_dispatcher import OutboxDispatcher
    from app.runtime.producer import ProducerPool
    from unittest.mock import AsyncMock, patch, MagicMock

    await init_db()

    # Clear outbox table to ensure no conflicts from previous aborted test runs
    async with session_scope() as session:
        await session.execute(sqlalchemy.delete(Outbox))
        await session.commit()

    env_name = f"Integration Test Env {uuid.uuid4()}"
    async with session_scope() as session:
        env = Env(
            name=env_name,
            enabled=1,
            delivery_mode="outbox"
        )
        session.add(env)
        await session.flush()

        src = SourceConfig(
            env_id=env.id,
            brokers="localhost:9092",
            topic="source-topic",
            consumer_group="group",
            offset_reset="earliest"
        )
        session.add(src)

        dg = DomainGrouping(
            env_id=env.id,
            position=1,
            name="DG1"
        )
        session.add(dg)
        await session.flush()

        dest = Destination(
            domain_grouping_id=dg.id,
            position=1,
            topic="dest-topic",
            use_source_broker=1
        )
        session.add(dest)

        from app.models import MatchCondition, MatchConditionValue
        mc = MatchCondition(
            domain_grouping_id=dg.id,
            position=1,
            key_path="type",
            operator="equals"
        )
        session.add(mc)
        await session.flush()

        mcv = MatchConditionValue(
            match_condition_id=mc.id,
            position=1,
            value="alert"
        )
        session.add(mcv)
        await session.commit()
        env_id = env.id

    class MockMsg:
        def __init__(self, topic, partition, offset, key, value):
            self.topic = topic
            self.partition = partition
            self.offset = offset
            self.key = key
            self.value = value

    mock_messages = [
        MockMsg("source-topic", 0, 10, b"key1", b'{"type": "alert", "msg": "hello"}'),
        MockMsg("source-topic", 0, 11, b"key2", b'{"type": "other", "msg": "ignore"}'),
        MockMsg("source-topic", 0, 12, b"key3", b'{"type": "alert", "msg": "hello2"}'),
    ]

    class AsyncIterator:
        def __init__(self, items):
            self.items = items
            self.idx = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.idx >= len(self.items):
                raise StopAsyncIteration
            item = self.items[self.idx]
            self.idx += 1
            return item

    pool = ProducerPool()
    mock_producer = AsyncMock()
    pool.get_producer = AsyncMock(return_value=mock_producer)
    
    dlq_mock = MagicMock()
    
    consumer = ConsumerTask(env_id, pool, dlq_mock, mode="outbox")
    
    mock_consumer_instance = MagicMock()
    mock_consumer_instance.start = AsyncMock()
    mock_consumer_instance.stop = AsyncMock()
    mock_consumer_instance.commit = AsyncMock()
    mock_consumer_instance.__aiter__ = lambda s: AsyncIterator(mock_messages)
    
    with patch("app.runtime.consumer.AIOKafkaConsumer", return_value=mock_consumer_instance):
        await consumer.start()
        await asyncio.sleep(0.5)
        await consumer.stop()

    async with session_scope() as session:
        result = await session.execute(select(Outbox).where(Outbox.env_id == env_id))
        outbox_rows = result.scalars().all()
        assert len(outbox_rows) == 2
        assert outbox_rows[0].idempotency_key == f"{env_id}:source-topic:0:10"
        assert outbox_rows[1].idempotency_key == f"{env_id}:source-topic:0:12"
        assert outbox_rows[0].dispatched_at is None
        assert outbox_rows[1].dispatched_at is None

    dispatcher = OutboxDispatcher(env_id, pool, dlq_mock)
    await dispatcher.start()
    await asyncio.sleep(1.5)
    await dispatcher.stop()

    assert mock_producer.send_and_wait.call_count == 2
    first_call_args = mock_producer.send_and_wait.call_args_list[0]
    kwargs = first_call_args.kwargs
    assert kwargs["topic"] == "dest-topic"
    assert b"hello" in kwargs["value"]
    headers = kwargs["headers"]
    assert headers[0][0] == "X-Source-Coord"
    assert headers[0][1] == b"source-topic:0:10"

    async with session_scope() as session:
        result = await session.execute(select(Outbox).where(Outbox.env_id == env_id))
        outbox_rows = result.scalars().all()
        assert outbox_rows[0].dispatched_at is not None
        assert outbox_rows[1].dispatched_at is not None

    async with session_scope() as session:
        await session.execute(sqlalchemy.delete(Env).where(Env.id == env_id))
        await session.commit()


@pytest.mark.asyncio
async def test_outbox_destination_down():
    import json
    from app.db import init_db, session_scope
    from app.models import Env, Outbox
    from app.runtime.outbox_dispatcher import OutboxDispatcher
    from app.runtime.producer import ProducerPool
    from unittest.mock import AsyncMock, MagicMock

    await init_db()

    env_id = str(uuid.uuid4())
    idempotency_key = f"{env_id}:topic:0:100"

    async with session_scope() as session:
        env = Env(id=env_id, name=f"Test Env Down {env_id}", enabled=1, delivery_mode="outbox")
        session.add(env)
        
        # Clear outbox table to ensure no conflicts
        await session.execute(sqlalchemy.delete(Outbox))
        await session.commit()

    async with session_scope() as session:
        row = Outbox(
            env_id=env_id,
            idempotency_key=idempotency_key,
            payload=b"message",
            headers_json="[]",
            destinations_json=json.dumps([{
                "topic": "test-dest",
                "brokers": "localhost:9092",
                "security_protocol": "PLAINTEXT",
                "headers": []
            }]),
            attempts=0,
            last_error=None
        )
        session.add(row)
        await session.commit()

    pool = ProducerPool()
    mock_producer = AsyncMock()
    pool.get_producer = AsyncMock(return_value=mock_producer)
    
    mock_producer.send_and_wait.side_effect = [Exception("Broker down")] * 4 + [None]
    
    dlq_mock = MagicMock()
    dispatcher = OutboxDispatcher(env_id, pool, dlq_mock)
    
    async with session_scope() as session:
        row_from_db = (await session.execute(select(Outbox).where(Outbox.idempotency_key == idempotency_key))).scalar_one()

    await dispatcher._dispatch_row(row_from_db)

    async with session_scope() as session:
        db_row = (await session.execute(select(Outbox).where(Outbox.idempotency_key == idempotency_key))).scalar_one()
        assert db_row.dispatched_at is None
        assert db_row.attempts == 1
        assert "Broker down" in db_row.last_error

    await dispatcher._dispatch_row(db_row)

    async with session_scope() as session:
        db_row = (await session.execute(select(Outbox).where(Outbox.idempotency_key == idempotency_key))).scalar_one()
        assert db_row.dispatched_at is not None

    async with session_scope() as session:
        await session.execute(sqlalchemy.delete(Env).where(Env.id == env_id))
        await session.commit()


