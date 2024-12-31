import time
import json
import asyncio
import numpy as np

from datetime import datetime, timedelta
from importlib.metadata import entry_points

from reduct import Client, BucketSettings, QuotaType

HIGH_DISTANCE = 10.0  # Example threshold for high distance
HIGH_AVERAGE_SPEED = 2.0  # Example threshold for high average speed

async def create_trajectory_bucket():
    async with Client("http://localhost:8383", api_token="my-token") as client:
        settings = BucketSettings(
            quota_type=QuotaType.FIFO,
            quota_size=1000_000_000,
        )
        await client.create_bucket("trajectory_data", settings, exist_ok=True)


async def generate_trajectory_data(frequency: int = 10, duration: int = 1):
    interval = 1 / frequency
    start_time = datetime.now()

    for i in range(frequency * duration):
        time_step = i * interval
        x = np.sin(2 * np.pi * time_step) + 0.2 * np.random.randn()
        y = np.cos(2 * np.pi * time_step) + 0.2 * np.random.randn()
        yaw = np.degrees(np.arctan2(y, x)) + np.random.uniform(-5, 5)
        speed = abs(np.sin(2 * np.pi * time_step)) + 0.1 * np.random.randn()
        timestamp = start_time + timedelta(seconds=time_step)

        yield {
            "timestamp": timestamp.isoformat(),
            "position": {"x": round(x, 2), "y": round(y, 2)},
            "orientation": {"yaw": round(yaw, 2)},
            "speed": round(speed, 2),
        }
        await asyncio.sleep(interval)


def calculate_trajectory_metrics(trajectory: list) -> tuple:
    positions = np.array([[point["position"]["x"], point["position"]["y"]] for point in trajectory])
    speeds = np.array([point["speed"] for point in trajectory])

    deltas = np.diff(positions, axis=0)
    distances = np.sqrt(np.sum(deltas**2, axis=1))
    total_distance = np.sum(distances)

    average_speed = np.mean(speeds)

    return total_distance, average_speed


async def store_trajectory_data():
    trajectory_data = []
    async for data_point in generate_trajectory_data(frequency=10, duration=1):
        trajectory_data.append(data_point)

    total_distance, average_speed = calculate_trajectory_metrics(trajectory_data)

    labels = {
        "total_distance": total_distance,
        "average_speed": average_speed,
        "high_distance": total_distance > HIGH_DISTANCE,
        "high_average_speed": average_speed > HIGH_AVERAGE_SPEED,
    }
    
    packed_data = pack_trajectory_data(trajectory_data)
    
    timestamp = datetime.now()

    async with Client("http://localhost:8383", api_token="my-token") as client:
        bucket = await client.get_bucket("trajectory_data")
        await bucket.write("trajectory_data", packed_data, timestamp, labels=labels)


def pack_trajectory_data(trajectory: list) -> bytes:
    """Pack trajectory data json format"""
    return json.dumps(trajectory).encode("utf-8")


async def query_by_label(bucket_name, entry_name, label_key, label_value):
    async with Client("http://localhost:8383", api_token="my-token") as client:
        try:
            bucket = await client.get_bucket(bucket_name)

            async for record in bucket.query(
                entry_name,
                when={
                    label_key: {"$eq": label_value}
                },
            ):
                print(record)

        except Exception as e:
            print(f"Error querying data by label: {e}")
            return None

        
async def query_by_timestamp(bucket_name, entry_name, time_difference_in_hours):
    async with Client("http://localhost:8383", api_token="my-token") as client:
        try:
            bucket = await client.get_bucket(bucket_name)

            end_time = time.time() * 1000000
            start_time = end_time - (time_difference_in_hours * 3600) * 10000000

            async for record in bucket.query(
                entry_name
                
            ):
                print(record)

            async for record in bucket.query(
                entry_name,
                end = end_time,
                start = start_time
            ):
                print(record)

        except Exception as e:
            print(f"Error querying data by label: {e}")
            return None


async def main():

    await create_trajectory_bucket()
    
    await store_trajectory_data()
    
    label_query_result = await query_by_label("trajectory_data", "trajectory_data", "&high_distance", "False")
    
    if label_query_result:
        print(f"Data queried by label: {label_query_result}")
     
asyncio.run(main())