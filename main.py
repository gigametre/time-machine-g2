import serial
import logging
from datetime import datetime
from TimeMachineClient import TimeMachineClient

# Configure logging
logging.basicConfig(
    filename=f'com4_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def fetch_event_data(event_num: int = 1):
    logging.info(f"Starting TimeMachineClient event {event_num} read")

    tm = TimeMachineClient(
        port='COM4',
        baudrate=9600,
        bytesize=serial.EIGHTBITS,
        parity=serial.PARITY_NONE,
        stopbits=serial.STOPBITS_ONE,
        timeout=1.0,
        inter_byte_delay=0.01,
    )

    try:
        text = tm.download_memory(event_num=event_num, heat_num=0, start_time=None, read_seconds=10.0)
        print(f"=== EVENT {event_num} DATA START ===")
        print(text)
        print(f"=== EVENT {event_num} DATA END ===")

        logging.info(f"Event {event_num} data received")
        logging.info(text)

        return text

    except Exception as e:
        print(f"Error reading event {event_num} data: {e}")
        logging.error(f"Error reading event {event_num} data", exc_info=True)
        return ""

    finally:
        tm.close()
        logging.info("TimeMachineClient closed")


def main():
    tm = TimeMachineClient(
        port='COM4',
        baudrate=9600,
        bytesize=serial.EIGHTBITS,
        parity=serial.PARITY_NONE,
        stopbits=serial.STOPBITS_ONE,
        timeout=1.0,
        inter_byte_delay=0.01,
    )

    t = tm.fetch_event_data(event_num=2)

    print("=== FULL EVENT 2 DATA ===")
    print(t)

if __name__ == '__main__':
    main()

