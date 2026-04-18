import socket
import threading
import sys
import os
import time
import asyncio
import ssl
import math
from aioquic.asyncio import serve
from aioquic.asyncio.client import connect
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import StreamDataReceived, ConnectionTerminated
from aioquic.asyncio.protocol import QuicConnectionProtocol

# --- Configuration Constants ---
TCP_PORT = 5001
UDP_PORT = 5002
QUIC_PORT = 5003
HEADER_SIZE = 150
CHUNK_SIZE = 4096

# ==========================================
# 0. METRICS HELPER FUNCTION
# ==========================================
def print_receiver_metrics(protocol, save_name, arrival_times, expected_bytes, received_bytes):
    # Calculate Loss based on exact BYTES
    loss_bytes = expected_bytes - received_bytes
    if loss_bytes < 0: loss_bytes = 0 # Safety net for TCP stream combining
    loss_rate = (loss_bytes / expected_bytes) * 100 if expected_bytes > 0 else 0
    
    # Calculate Jitter (Variance in arrival intervals)
    jitter_ms = 0
    if len(arrival_times) > 2:
        intervals = [arrival_times[i] - arrival_times[i-1] for i in range(1, len(arrival_times))]
        jitters = [abs(intervals[i] - intervals[i-1]) for i in range(1, len(intervals))]
        jitter_ms = (sum(jitters) / len(jitters)) * 1000 # Convert to milliseconds

    print(f"\n[!] {protocol.upper()} Transfer Complete: '{save_name}'")
    print(f"    -> Total Expected Size:    {expected_bytes / (1024*1024):.4f} MB")
    print(f"    -> Total Received Size:    {received_bytes / (1024*1024):.4f} MB")
    print(f"    -> Data Loss Rate:         {loss_rate:.2f}%")
    print(f"    -> Average Jitter:         {jitter_ms:.4f} ms")
    print("Enter command (protocol filename ip) or 'q': ", end="", flush=True)

# ==========================================
# 1. TCP LISTENER 
# ==========================================
def listen_for_tcp():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('0.0.0.0', TCP_PORT))
    server_socket.listen(5)
    while True:
        try:
            connection, address = server_socket.accept()
            # Read header
            header = connection.recv(HEADER_SIZE).decode('utf-8').strip()
            filename, expected_bytes = header.split('|')
            expected_bytes = int(expected_bytes)
            save_name = f"tcp_rcv_{filename}"
            
            arrival_times = []
            received_bytes = 0
            
            with open(save_name, 'wb') as file:
                while True:
                    data = connection.recv(CHUNK_SIZE)
                    if not data:
                        break
                    received_bytes += len(data)
                    arrival_times.append(time.time())
                    file.write(data)
            
            print_receiver_metrics("TCP", save_name, arrival_times, expected_bytes, received_bytes)
            connection.close()
        except Exception as e:
            pass

# ==========================================
# 2. UDP LISTENER
# ==========================================
def listen_for_udp():
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.bind(('0.0.0.0', UDP_PORT))
    while True:
        try:
            # Read header
            header_data, address = udp_socket.recvfrom(HEADER_SIZE)
            header_str = header_data.decode('utf-8').strip()
            filename, expected_bytes = header_str.split('|')
            expected_bytes = int(expected_bytes)
            save_name = f"udp_rcv_{filename}"
            
            arrival_times = []
            received_bytes = 0
            
            with open(save_name, 'wb') as file:
                while True:
                    data, address = udp_socket.recvfrom(CHUNK_SIZE)
                    if data == b"EOF_MARKER":
                        break
                    received_bytes += len(data)
                    arrival_times.append(time.time())
                    file.write(data)
                    
            print_receiver_metrics("UDP", save_name, arrival_times, expected_bytes, received_bytes)
        except Exception as e:
            pass

# ==========================================
# 3. QUIC LISTENER 
# ==========================================
# ==========================================
# 3. QUIC LISTENER (Updated)
# ==========================================
class FileReceiverProtocol(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.save_name = None
        self.expected_bytes = 0
        self.received_bytes = 0
        self.arrival_times = []

    def quic_event_received(self, event):
        if isinstance(event, StreamDataReceived):
            if not self.save_name:
                header_str = event.data[:HEADER_SIZE].decode('utf-8').strip()
                filename, expected = header_str.split('|')
                self.expected_bytes = int(expected)
                self.save_name = f"quic_rcv_{filename}"
                
                chunk_data = event.data[HEADER_SIZE:]
                if chunk_data:
                    self.received_bytes += len(chunk_data)
                    self.arrival_times.append(time.time())
                    with open(self.save_name, "wb") as file:
                        file.write(chunk_data)
            else:
                self.received_bytes += len(event.data)
                self.arrival_times.append(time.time())
                with open(self.save_name, "ab") as file:
                    file.write(event.data)
            
            # NEW: If we have received the whole file, tell the Sender!
            if self.received_bytes >= self.expected_bytes and self.expected_bytes > 0:
                self._quic.send_stream_data(event.stream_id, b"ACK", end_stream=True)
                self.transmit()
                    
        elif isinstance(event, ConnectionTerminated):
            if self.save_name:
                print_receiver_metrics("QUIC", self.save_name, self.arrival_times, self.expected_bytes, self.received_bytes)
            self.save_name = None 
            self.arrival_times = []
            self.received_bytes = 0

async def run_quic_server():
    configuration = QuicConfiguration(is_client=False)
    try:
        configuration.load_cert_chain(certfile="ssl_cert.pem", keyfile="ssl_key.pem")
    except FileNotFoundError:
        print("\n[!] QUIC Warning: ssl_cert.pem or ssl_key.pem missing. QUIC listener is disabled.")
        return
        
    await serve("0.0.0.0", QUIC_PORT, configuration=configuration, create_protocol=FileReceiverProtocol)
    await asyncio.Future()

def start_quic_thread():
    asyncio.run(run_quic_server())

# Start background listeners
threading.Thread(target=listen_for_tcp, daemon=True).start()
threading.Thread(target=listen_for_udp, daemon=True).start()
threading.Thread(target=start_quic_thread, daemon=True).start()

# ==========================================
# 4. QUIC SENDER LOGIC
# ==========================================
# ==========================================
# 4. QUIC SENDER LOGIC (Updated)
# ==========================================
# NEW: A custom protocol for the sender to listen for the ACK
class FileSenderProtocol(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # This acts as a signal flare that we can wait for
        self.ack_received = asyncio.Event()

    def quic_event_received(self, event):
        # When we hear the ACK, light the signal flare!
        if isinstance(event, StreamDataReceived) and event.data == b"ACK":
            self.ack_received.set()

async def send_via_quic(filename, target_ip, header):
    configuration = QuicConfiguration(is_client=True)
    configuration.verify_mode = ssl.CERT_NONE
    file_size_bytes = os.path.getsize(filename)
    start_time = time.time()
    
    try:
        # Use our new custom protocol here
        async with connect(target_ip, QUIC_PORT, configuration=configuration, create_protocol=FileSenderProtocol) as protocol:
            stream_id = protocol._quic.get_next_available_stream_id()
            protocol._quic.send_stream_data(stream_id, header, end_stream=False)
            
            with open(filename, "rb") as file:
                while True:
                    chunk = file.read(CHUNK_SIZE)
                    if not chunk:
                        protocol._quic.send_stream_data(stream_id, b"", end_stream=True)
                        protocol.transmit()
                        break
                    protocol._quic.send_stream_data(stream_id, chunk, end_stream=False)
                    protocol.transmit()
                    
                    # NEW: Briefly yield control so aioquic can actually process the network traffic
                    await asyncio.sleep(0) 
            
            print("\n[!] Data buffered. Waiting for network to finish transmission and Receiver to ACK...")
            
            # NEW: Wait indefinitely until the Receiver sends the ACK
            await protocol.ack_received.wait()
            
        total_time = time.time() - start_time
        throughput = (file_size_bytes / (1024 * 1024)) / total_time
        print(f"Success! '{filename}' sent via QUIC.")
        print(f"--> Sender Latency:   {total_time:.4f} seconds")
        print(f"--> Sender Throughput: {throughput:.2f} MB/s\n")
    except Exception as e:
        print(f"QUIC Failed: {e}")
# ==========================================
# 5. THE COMMAND PROMPT (MAIN PROGRAM)
# ==========================================
print("\n=== Unified P2P Node Started ===")
print("Listening simultaneously on TCP (5001), UDP (5002), and QUIC (5003).")
print("Format: <protocol> <filename> <ip_address>\n")

while True:
    command = input("Enter command (protocol filename ip) or 'q': ")
    if command.lower() == 'q':
        print("Quitting program...")
        break
        
    parts = command.split()
    if len(parts) != 3:
        print("Invalid format. Use: tcp test.txt 192.168.1.5")
        continue
        
    protocol, filename, target_ip = parts[0].lower(), parts[1], parts[2]
    
    if not os.path.exists(filename):
        print(f"Error: File '{filename}' not found.")
        continue

    file_size_bytes = os.path.getsize(filename)
    
    # Create the header containing original filename and EXACT file size in bytes
    header_str = f"{filename}|{file_size_bytes}"
    header = header_str.ljust(HEADER_SIZE).encode('utf-8')

    # --- TCP Route ---
    if protocol == 'tcp':
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((target_ip, TCP_PORT))
            start_time = time.time()
            
            client.sendall(header)
            with open(filename, 'rb') as f:
                while chunk := f.read(CHUNK_SIZE):
                    client.sendall(chunk)
                    
            total_time = time.time() - start_time
            client.close()
            throughput = (file_size_bytes / (1024 * 1024)) / total_time
            print(f"\nSuccess! '{filename}' sent via TCP.")
            print(f"--> Sender Latency:   {total_time:.4f} seconds")
            print(f"--> Sender Throughput: {throughput:.2f} MB/s\n")
        except Exception as e:
            print(f"TCP Failed: {e}")

    # --- UDP Route ---
    elif protocol == 'udp':
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            start_time = time.time()
            
            client.sendto(header, (target_ip, UDP_PORT))
            with open(filename, 'rb') as f:
                while chunk := f.read(CHUNK_SIZE):
                    client.sendto(chunk, (target_ip, UDP_PORT))
                # Send the secret EOF marker to close the receiver's loop
                client.sendto(b"EOF_MARKER", (target_ip, UDP_PORT))
                
            total_time = time.time() - start_time
            client.close()
            throughput = (file_size_bytes / (1024 * 1024)) / total_time if total_time > 0 else 0
            print(f"\nSuccess! '{filename}' blasted via UDP.")
            print(f"--> Sender Latency:   {total_time:.4f} seconds")
            print(f"--> Sender Throughput: {throughput:.2f} MB/s\n")
        except Exception as e:
            print(f"UDP Failed: {e}")

    # --- QUIC Route ---
    elif protocol == 'quic':
        # Execute the async function synchronously
        asyncio.run(send_via_quic(filename, target_ip, header))
        
    else:
        print("Error: Protocol must be 'tcp', 'udp', or 'quic'.")
