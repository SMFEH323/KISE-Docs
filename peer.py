import socket
import threading
import json
import time
import hashlib

# Peer configuration
HOST = input("Enter the host IP address: ")  # Localhost
PORT = int(input("Enter the port to listen on: "))  # User enters the port

# Shared Document (RGA-based)
document = []  # List of (character, uid) tuples
peers = []     # List of connected peers (IP, PORT)
document_lock = threading.Lock()  # Lock to ensure thread-safe access to the document
pending_operations = []  # Buffer for out-of-order operations

peer_status = {}  # Track peer status: True = reachable, False = unreachable
peer_retry_cooldown = {}  # Track cooldown timestamps for unreachable peers

VERBOSE = False

#time.sleep(5) # delay

""" if random.random() < 0.2:  # Simulate 20% message loss
    print(f"[INFO] Simulated loss for operation {operation['uid']} to {ip}:{port}")
    continue """

# Function to apply an operation to the document
def apply_operation(operation):
    global document
    if operation["type"] == "insert":
        position = operation["position"]
        char = operation["character"]
        uid = tuple(operation["uid"])  # Convert to tuple
        if (position < len(document)):
            prev_node = document[position]
            prev_node_ts = prev_node[1][0]
            if (prev_node_ts > uid[0]):
                document.insert(position+1, (char, uid))
            elif (prev_node_ts < uid[0]):
                document.insert(position, (char, uid))
        else:
            document.insert(position, (char, uid))
    elif operation["type"] == "delete":
        uid = tuple(operation["uid"])  # Convert to tuple
        for i, (char, existing_uid) in enumerate(document):
            if existing_uid == uid:
                document[i] = (None, uid)  # Mark as tombstone
                break
    if VERBOSE:
        print(f"[DOCUMENT] {document}")

# Function to process the pending operations buffer
def process_pending_operations():
    global pending_operations
    with document_lock:  # Ensure thread-safe access
        # Sort pending operations by UID (timestamp)
        pending_operations.sort(key=lambda op: tuple(op["uid"]))
        for operation in pending_operations[:]:  # Iterate over a copy
            apply_operation(operation)  # Apply operation
            pending_operations.remove(operation)  # Remove after applying

def send_data_with_length(sock, data):
    try:
        # Serialize the data to JSON and encode it to bytes
        serialized_data = json.dumps(data).encode('utf-8')  # Example: `[]` becomes `b"[]"`
        data_length = len(serialized_data)  # Length of serialized data in bytes

        # Send the 4-byte header (data length)
        sock.sendall(data_length.to_bytes(4, byteorder='big'))

        # Send the serialized data
        sock.sendall(serialized_data)

        # Debugging output
        print(f"[DEBUG] Sent header: {data_length.to_bytes(4, byteorder='big')}, length: {data_length}")
        print(f"[DEBUG] Sent data: {serialized_data}")
    except Exception as e:
        print(f"[ERROR] Failed to send data: {e}")


def receive_data_with_length(sock):
    try:
        #Accumulate exactly 4 bytes for the header
        header = b''
        while len(header) < 4:
            chunk = sock.recv(4 - len(header))  # Receive the remaining bytes needed for the header
            if not chunk:  # Connection closed unexpectedly
                raise ValueError("Connection closed while reading header.")
            header += chunk

        data_length = int.from_bytes(header, byteorder='big')

        if data_length == 0:
            print("[DEBUG] Received zero-length data. Returning empty payload.")
            return None

        # Receive the data in chunks
        data = b''
        while len(data) < data_length:
            chunk = sock.recv(4096)
            if not chunk:
                raise ValueError("Connection closed before receiving full data.")
            data += chunk

        if len(data) != data_length:
            raise ValueError(f"Incomplete data received. Expected {data_length}, got {len(data)} bytes.")

        return json.loads(data.decode('utf-8'))
    except ValueError as e:
        print(f"[ERROR] {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"[ERROR] Failed to decode JSON: {e}")
        return None

# Function to handle incoming messages
def handle_client(conn, addr):
    print(f"[INFO] Connected by {addr}")
    try:
        while True:
            operation = receive_data_with_length(conn)  # Use the updated function
            if not operation:
                print(f"[ERROR] Received invalid or no operation from {addr}. Closing connection.")
                break
            print(f"[MESSAGE] Received from {addr}: {operation}")

            # Handle empty document case
            if operation.get("type") == "document_state" and not operation.get("document"):
                print("[INFO] Received empty document state.")
                continue

            # Handle request for document synchronization
            if operation["type"] == "request_document":
                with document_lock:
                    response = {
                        "type": "document_state",
                        "document": document  # Send the entire document
                    }
                send_data_with_length(conn, response)
            elif operation["type"] == "hash_request":
                response = {"type": "hash_response", "hash": hash_document()}
                send_data_with_length(conn, response)
            elif operation["type"] == "join_request":
                # Send acknowledgment
                ack = {"type": "ack_jr"}
                send_data_with_length(conn, ack)
                
                peer_ip = operation["peer_ip"]
                peer_port = operation["peer_port"]
                # Add the requesting peer to the local peers list
                add_peer(peer_ip, peer_port)
                print(f"[PEER] Peer {peer_ip}:{peer_port} added me as a result of join.")
            else:
                # Send acknowledgment
                ack = {"type": "ack", "uid": operation["uid"]}
                send_data_with_length(conn, ack)
                
                # Add to pending operations and process
                with document_lock:
                    pending_operations.append(operation)
                process_pending_operations()
    except ConnectionResetError:
        print(f"[INFO] Connection with {addr} lost")
    finally:
        conn.close()

# Function to start the server (listens for incoming connections)
def start_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen(5)
    print(f"[SERVER] Listening on {HOST}:{PORT}")
    
    while True:
        conn, addr = server_socket.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()

def broadcast_operation_with_ack(operation, max_retries=3, cooldown=60):
    global peer_status, peer_retry_cooldown

    for ip, port in peers:
        # Skip peers marked as unreachable unless cooldown has expired
        if not peer_status.get((ip, port), True):
            if time.time() - peer_retry_cooldown.get((ip, port), 0) < cooldown:
                print(f"[INFO] Skipping unreachable peer {ip}:{port}. Cooldown active.")
                continue
            else:
                print(f"[INFO] Retrying unreachable peer {ip}:{port}.")
                peer_status[(ip, port)] = True  # Retry the peer

        retry_count = 0
        while retry_count < max_retries:
            try:
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.settimeout(3)  # Set a 3-second timeout for ACK
                client_socket.connect((ip, port))
                send_data_with_length(client_socket, operation)  # Send operation

                # Wait for acknowledgment
                ack = receive_data_with_length(client_socket)
                if ack.get("type") == "ack" and ack.get("uid") == operation["uid"]:
                    print(f"[ACK] Received acknowledgment from {ip}:{port} for {operation['uid']}")
                    break
                else:
                    raise Exception("Invalid or missing ACK")
            except Exception:
                retry_count += 1
                print(f"[ERROR] Attempt {retry_count}/{max_retries} failed for {ip}:{port}")
                time.sleep(1)  # Wait before retrying
            finally:
                client_socket.close()

        # Mark peer as unreachable if retries are exhausted
        if retry_count == max_retries:
            print(f"[WARNING] Peer {ip}:{port} marked as unreachable after {max_retries} retries.")
            peer_status[(ip, port)] = False
            peer_retry_cooldown[(ip, port)] = time.time()


# Function to insert a character into the document
def insert_character(position, character):
    uid = (int(time.time()), f"peer-{PORT}")  # Unique ID: timestamp + peer ID
    operation = {
        "type": "insert",
        "position": position,
        "character": character,
        "uid": uid
    }
    with document_lock:
        pending_operations.append(operation)  # Add to pending operations

    process_pending_operations()  # Process operations
    broadcast_operation_with_ack(operation)  # Broadcast to peers

# Function to delete a character from the document
def delete_character(uid):
    operation = {
        "type": "delete",
        "uid": uid
    }
    with document_lock:
        pending_operations.append(operation)  # Add to pending operations
    process_pending_operations()  # Process operations
    broadcast_operation_with_ack(operation)  # Broadcast to peers

# Function to add a peer to the list
def add_peer(ip, port):
    try:
        test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test_socket.connect((ip, port))
        test_socket.close()
        
        peers.append((ip, port))
        peer_status[(ip, port)] = True  # Mark as reachable
        print(f"[PEER] Added peer {ip}:{port}")
        
        # Auto-sync document state from the new peer
        print(f"[SYNC] Automatically syncing with {ip}:{port}...")
        request_document(ip, port)
    except ConnectionRefusedError:
        print(f"[ERROR] Unable to connect to peer {ip}:{port}.")

def request_document(target_ip, target_port):
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((target_ip, target_port))
        send_data_with_length(client_socket, {"type": "request_document"})

        # Use `receive_data_with_length` to handle larger data
        response = receive_data_with_length(client_socket)
        if response and response["type"] == "document_state":
            global document
            with document_lock:
                document = response["document"]
            print(f"[SYNC] Document synchronized: {document}")
        else:
            print(f"[ERROR] Invalid or no response received from {target_ip}:{target_port}")
    except ConnectionRefusedError:
        print(f"[ERROR] Unable to connect to {target_ip}:{target_port}")
    finally:
        client_socket.close()


def join_peer(ip, port, retries=3):
    attempt = 0
    while attempt < retries:
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.settimeout(3)  # Set a 3-second timeout for ACK
            client_socket.connect((ip, port))
            
            # Send a join request with your information
            join_request = {"type": "join_request", "peer_ip": HOST, "peer_port": PORT}
            send_data_with_length(client_socket, join_request)
            print(f"[JOIN] Sent join request to {ip}:{port}")
            
            # Wait for acknowledgment
            ack = receive_data_with_length(client_socket)
            if ack.get("type") == "ack_jr":
                print(f"[ACK] Received acknowledgment from {ip}:{port} for join request")
                break
            else:
                raise Exception("Invalid or missing ACK")
        except Exception:
                attempt += 1
                print(f"[ERROR] Did not receive ACK from {ip}:{port}. Retrying ({attempt}/{retries})...")
                time.sleep(1)  # Wait before retrying
        finally:
            client_socket.close()

    if attempt == retries:
        print(f"[ERROR] Failed to join {ip}:{port} after {retries} attempts.")
    else:
        # Add the peer locally and auto-sync
        add_peer(ip, port)

def hash_document():
    with document_lock:
        document_str = json.dumps(document)  # Convert the document to a JSON string
        return hashlib.sha256(document_str.encode('utf-8')).hexdigest()

def compare_hashes():
    local_hash = hash_document()
    for ip, port in peers:
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((ip, port))
            send_data_with_length(client_socket, {"type": "hash_request"})

            # Receive the hash
            response = receive_data_with_length(client_socket)
            if response["type"] == "hash_response":
                peer_hash = response["hash"]
                if local_hash != peer_hash:
                    print(f"[MISMATCH] Hash mismatch with {ip}:{port}. Sync needed.")
                else:
                    print(f"[MATCH] Hash matches with {ip}:{port}.")
        except ConnectionRefusedError:
            print(f"[ERROR] Could not connect to {ip}:{port}")
        finally:
            client_socket.close()

# Function to clean up tombstones in the document
def clean_up_tombstones():
    global document
    with document_lock:
        # Filter out elements marked as tombstones
        document = [entry for entry in document if entry[0] is not None]
    if VERBOSE:
        print(f"[CLEANUP] Document after cleanup: {document}")

# Timer to periodically clean up tombstones
def start_cleanup_timer(interval=30):
    def cleanup_loop():
        while True:
            time.sleep(interval)
            clean_up_tombstones()
    cleanup_thread = threading.Thread(target=cleanup_loop, daemon=True)
    cleanup_thread.start()

# Main function to run server and interact with the user
if __name__ == "__main__":
    # Start the server in a separate thread
    server_thread = threading.Thread(target=start_server)
    server_thread.daemon = True
    server_thread.start()

    # Start tombstone cleanup timer
    start_cleanup_timer(interval=30)  # Cleanup every 30 seconds
    
    print("[INFO] Server started. Enter 'join', 'edit', 'view', 'sync', 'finalize', or 'exit'")
    while True:
        command = input(">> ").strip()
        if command == "join":
            target_ip = input("Enter peer IP: ")
            target_port = int(input("Enter peer PORT: "))
            join_peer(target_ip, target_port)  # Use join_peer for full join process
        elif command == "edit":
            action = input("Enter action (insert/delete): ").strip().lower()
            if action == "insert":
                position = int(input("Enter position: "))
                character = input("Enter character: ")
                insert_character(position, character)
            elif action == "delete":
                uid = input("Enter UID to delete (e.g., '(timestamp, peer_id)'): ")
                try:
                    uid_input = eval(uid)  # Use eval to convert the tuple-like string to a tuple
                    if isinstance(uid, tuple) and len(uid) == 2:
                        delete_character(uid)
                    else:
                        print("[ERROR] Invalid UID format. Must be a tuple of (timestamp, peer_id).")
                except Exception as e:
                    print(f"[ERROR] Failed to parse UID: {e}")
        elif command == "view":
            with document_lock:
                print(f"[DOCUMENT] {document}")
        elif command == "sync":
            target_ip = input("Enter the IP of the peer to sync with: ")
            target_port = int(input("Enter the port of the peer to sync with: "))
            request_document(target_ip, target_port)    
        elif command == "finalize":
            compare_hashes()
        elif command == "exit":
            print("[INFO] Exiting...")
            break
