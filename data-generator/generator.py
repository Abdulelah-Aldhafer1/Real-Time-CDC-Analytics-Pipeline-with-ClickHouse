import os
import time
import json
import random
import logging
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_batch
from faker import Faker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class DataGenerator:
    def __init__(self):
        self.fake = Faker()
        
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'database': os.getenv('POSTGRES_DB', 'engagement_db'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD')
        }
        
        self.rate = int(os.getenv('GENERATION_RATE', 10000))  # events per minute
        self.batch_size = 100
        
        self.content_items = []
        self.users = []
        self.devices = ['ios', 'android', 'web', 'desktop']
        self.event_types = ['play', 'pause', 'finish', 'click']
        
        logger.info(f"Generator initialized: {self.rate} events/minute")

    def connect_db(self):
        """Connect to database with retries"""
        for attempt in range(3):
            try:
                conn = psycopg2.connect(**self.db_config)
                return conn
            except psycopg2.OperationalError as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < 2:
                    time.sleep(2)
                else:
                    raise

    def load_data(self):
        """Load content and generate users"""
        conn = self.connect_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, slug, title FROM content")
                self.content_items = cursor.fetchall()
                logger.info(f"Loaded {len(self.content_items)} content items")
        finally:
            conn.close()
        
        self.users = [self.fake.uuid4() for _ in range(1000)]
        logger.info(f"Generated {len(self.users)} users")

    def generate_event(self):
        """Generate a single event"""
        content = random.choice(self.content_items)
        user = random.choice(self.users)
        event_type = random.choice(self.event_types)
        device = random.choice(self.devices)
        
        duration = None
        if event_type != 'click':
            duration = random.randint(1000, 300000)  # 1-300 seconds
        
        event_time = datetime.now() - timedelta(seconds=random.randint(0, 300))
        
        payload = {
            'session_id': self.fake.uuid4(),
            'ip': self.fake.ipv4(),
            'user_agent': self.fake.user_agent()
        }
        
        return (
            content[0],   
            user,        
            event_type,
            event_time,
            duration,
            device,
            json.dumps(payload)
        )

    def insert_batch(self, events):
        """Insert a batch of events"""
        conn = self.connect_db()
        try:
            with conn.cursor() as cursor:
                execute_batch(
                    cursor,
                    """
                    INSERT INTO engagement_events 
                    (content_id, user_id, event_type, event_ts, duration_ms, device, raw_payload)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    events
                )
                conn.commit()
                return len(events)
        except Exception as e:
            logger.error(f"Insert failed: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    def run(self):
        """Main generation loop"""
        logger.info("Starting data generation...")
        
        self.load_data()
        
        if not self.content_items:
            logger.error("No content found")
            return
        
        total_events = 0
        start_time = time.time()
        
        while True:
            minute_start = time.time()
            events_this_minute = 0
            
            # generate events for this minute
            while events_this_minute < self.rate:
                batch = []
                for _ in range(min(self.batch_size, self.rate - events_this_minute)):
                    batch.append(self.generate_event())
                
                inserted = self.insert_batch(batch)
                events_this_minute += inserted
                total_events += inserted
            
            elapsed = time.time() - minute_start
            total_elapsed = time.time() - start_time
            rate = total_events / (total_elapsed / 60)
            
            logger.info(f"Minute complete: {events_this_minute} events | Total: {total_events} | Rate: {rate:.0f}/min")
            
            # Sleep for remainder of minute
            sleep_time = max(0, 60 - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

def main():
    generator = DataGenerator()
    
    if os.getenv('BURST_MODE', '').lower() == 'true':
        # Burst mode for testing
        count = int(os.getenv('BURST_COUNT', 1000))
        logger.info(f"Burst mode: generating {count} events")
        
        generator.load_data()
        events = [generator.generate_event() for _ in range(count)]
        generator.insert_batch(events)
        logger.info("Burst complete")
    else:
        generator.run()

if __name__ == "__main__":
    main()