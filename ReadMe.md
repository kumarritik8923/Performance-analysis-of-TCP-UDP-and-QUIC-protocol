# Protocol Performance Analysis: TCP vs. UDP vs. QUIC

## Overview
We have created a file transfer application . This project evaluates the performance differences between three fundamental transport layer protocols: TCP, UDP, and QUIC. To conduct a fair and accurate analysis, a **Unified P2P Node** was developed in Python. This architecture allows the node to act as both a sender and a listener, seamlessly switching between protocols to measure metrics like throughput, latency, and reliability during file transfers.

## Key Features
* **Unified Architecture:** A single script handles listening and sending capabilities across multiple protocols.
* **TCP Implementation:** Guarantees reliable, ordered delivery to establish a baseline for secure file transfers.
* **UDP Implementation:** Prioritizes speed and low overhead to test raw throughput and packet loss scenarios.
* **QUIC Implementation:** Modern, multiplexed protocol over UDP aiming to combine the speed of UDP with the reliability of TCP.

## Technologies Used
* **Python 3**
* Standard libraries (`socket`, `threading`,`aioquic`, `asyncio`, etc.) for TCP, UDP and QUIC connections.
* Wireshark for data visualization and performance comparison.

## Performance Metrics Analyzed
The project compares the protocols based on:
1. **Transfer Speed (Throughput):** How fast large files are transmitted.
2. **Latency:** The delay in establishing connections and sending the first byte.
3. **Reliability:** How each protocol handles packet loss.
4. **Jitter:** Variance in Latency.

## How to Run
1. Clone this repository to your local machine.
2. Install required libraries.
3. You should TLS certificate for encryption.I have generated self signed certificate through openssl.
2. Run the ft.py script to start the listener on your desired port.
3. Execute the sender commands, specifying the target Protocol,file to send,IP of receiver in same sequence.