import serial
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    filename=f'com4_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt',
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

def retransmit(ser, event, heat):
    """
    Send retransmit command to Time Machine G2.
    Command ID: 0x5
    Event: two ASCII digits (00-99)
    Heat: two ASCII digits (00-99)
    Followed by CR/LF
    """
    if not (0 <= event <= 99):
        raise ValueError("Event must be between 0 and 99")
    if not (0 <= heat <= 99):
        raise ValueError("Heat must be between 0 and 99")
    message = b'\x05' + f'{event:02d}'.encode('ascii') + f'{heat:02d}'.encode('ascii') + b'\r\n'
    ser.write(message)
    logging.info(f"Sent retransmit: event={event}, heat={heat}")
    
    # Read and handle response
    response = ser.readline().decode('utf-8', errors='ignore').strip()
    if response:
        print(f"Response: {response}")
        logging.info(f"Response: {response}")




def main():
    try:
        # Open COM port 4
        ser = serial.Serial('COM4', baudrate=9600, timeout=1)
        print(f"Connected to {ser.port}")
        logging.info(f"Connected to {ser.port}")
        
        # Listen and log incoming data
        while True:
            if ser.in_waiting > 0:
                data = ser.readline().decode('utf-8', errors='ignore').strip()
                if data:
                    print(data)
                    logging.info(data)
    
    except serial.SerialException as e:
        print(f"Error: {e}")
        logging.error(f"Serial error: {e}")

    finally:
        if ser.is_open:
            ser.close()
            print("COM port closed")
            logging.info("COM port closed")

if __name__ == "__main__":
    #main()
    try:
        # Open COM port 4
        ser = serial.Serial('COM4', baudrate=9600, timeout=1)
        print(f"Connected to {ser.port}")
        logging.info(f"Connected to {ser.port}")
        retransmit(ser, 1, 1)
        send_0x80_zeroes(ser)
    except serial.SerialException as e:
        print(f"Error: {e}")
        logging.error(f"Serial error: {e}") 