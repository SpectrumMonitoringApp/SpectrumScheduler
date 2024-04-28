import os
import asyncio
import json
import aiohttp
from dotenv import load_dotenv
from aiokafka import AIOKafkaProducer

load_dotenv()

class PollingManager:
    def __init__(self):
        self.connections = {}
        self.polling_tasks = {}
        self.kafka_producer = None

    async def initialize_kafka_producer(self, bootstrap_servers):
        self.kafka_producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
        await self.kafka_producer.start()

    async def close_kafka_producer(self):
        if self.kafka_producer is not None:
            await self.kafka_producer.stop()

    async def send_kafka_message(self, topic, message):
        await self.kafka_producer.send_and_wait(topic, message)
        print(f"Sent message to Kafka topic {topic}: {message.decode('utf-8')}")

    async def add_connection(self, conn_id, resource_type, poll_interval, data_stores):
        if conn_id in self.connections:
            print(f"Connection {conn_id} already exists.")
            return

        self.connections[conn_id] = {
            "type": resource_type,
            "poll_interval": poll_interval,
            "data_stores": data_stores,
        }
        self.polling_tasks[conn_id] = asyncio.create_task(
            self.start_polling(conn_id, poll_interval)
        )

        print(f"Added and started polling for connection {conn_id}.")

    async def remove_connection(self, conn_id):
        conn_task = self.polling_tasks.get(conn_id)

        if conn_task:
            conn_task.cancel()

            try:
                await conn_task
            except asyncio.CancelledError:
                pass

            del self.connections[conn_id]
            del self.polling_tasks[conn_id]

            print(f"Removed connection {conn_id}.")

    async def start_polling(self, conn_id, interval):
        try:
            while conn_id in self.connections:
                print(f"Polling database for connection {conn_id}.")
                resource_type = self.connections[conn_id]["type"]
                data_stores = self.connections[conn_id]["data_stores"]
                payload = json.dumps(
                    {"dataSourceId": conn_id, "type": resource_type, "dataStores": data_stores}
                )

                print("payload: ", payload)
                await self.send_kafka_message("data-source-polls", payload.encode('utf-8'))
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            print(f"Polling task {conn_id} was cancelled.")

    async def fetch_data_with_auth(self):
        internal_api_url = "https://api.tryspectrum.site/internal-components/scheduler"
        headers = {"Authorization": "Internal abcd1234"}

        async with aiohttp.ClientSession() as session:
            async with session.get(internal_api_url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    # Handle HTTP errors
                    print(f"Failed to fetch data: HTTP Error {response.status}")
                    return None

    async def fetch_connection_info_from_db(self):
        resources = await self.fetch_data_with_auth()
        resources_to_poll = []

        for resource in resources:
            resource_id = resource["id"]
            resource_poll_type = resource["type"]
            resource_poll_interval = resource["pollInterval"]
            resource_data_stores = resource["dataStores"]

            print(
                f"resource_id: {resource_id}, resource_poll_interval: {resource_poll_interval}, dataStores: {resource_data_stores}"
            )

            resources_to_poll.append(
                (
                    resource_id,
                    {
                        "type": resource_poll_type,
                        "poll_interval": resource_poll_interval,
                        "data_stores": resource_data_stores,
                    },
                )
            )

        return resources_to_poll

    async def update_connections_from_db(self):
        while True:
            try:
                connection_infos = await self.fetch_connection_info_from_db()
                known_connections = set(self.connections.keys())
                fetched_connections = set(conn_id for conn_id, _ in connection_infos)

                # Remove connections no longer present or deactivated
                for conn_id in known_connections - fetched_connections:
                    await self.remove_connection(conn_id)

                # Add new connections or update existing ones
                for conn_id, config in connection_infos:
                    resourceType = config["type"]
                    interval = config["poll_interval"]
                    data_stores = config["data_stores"]

                    if conn_id in self.connections:
                        connection_active_data_stores = list(map(lambda x: x['name'], self.connections[conn_id]["data_stores"]))
                        new_connection_data_stores = list(map(lambda x: x['name'], data_stores))

                        l1_sorted = sorted(connection_active_data_stores)
                        l2_sorted = sorted(new_connection_data_stores)

                        if self.connections[conn_id]["poll_interval"] != interval or l1_sorted != l2_sorted:
                            await self.remove_connection(conn_id)
                            await self.add_connection(conn_id, resourceType, interval, data_stores)
                    else:
                        await self.add_connection(conn_id, resourceType, interval, data_stores)

            except Exception as e:
                print(f"Error updating connections from DB: {e}")

            await asyncio.sleep(10)


async def main():
    print('Starting polling data');
    KAFKA_BOOTSTRAP_SERVER_ONE = os.getenv('KAFKA_BOOTSTRAP_SERVER_ONE')
    KAFKA_BOOTSTRAP_SERVER_TWO = os.getenv('KAFKA_BOOTSTRAP_SERVER_TWO')

    try:
        manager = PollingManager()

        await manager.initialize_kafka_producer([KAFKA_BOOTSTRAP_SERVER_ONE, KAFKA_BOOTSTRAP_SERVER_TWO])

        db_update_task = asyncio.create_task(manager.update_connections_from_db())

        await db_update_task
        await manager.close_kafka_producer()

    except Exception as error:
        print(error)

asyncio.run(main())
