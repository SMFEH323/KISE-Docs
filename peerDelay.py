import socket
import threading
import json
import time
import random

# Peer configuration
HOST = input("Enter the host IP address: ")  # Localhost
PORT = int(input("Enter the port to listen on: "))  # User enters the port
VERBOSE = False

# Shared Document (RGA-based)
document = []  # List of (character, uid) tuples
peers = []     # List of connected peers (IP, PORT)
document_lock = threading.Lock()  # Lock to ensure thread-safe access to the document
pending_operations = []  # Buffer for out-of-order operations
operation_history = []  # List of all operations

is_synced = False

# Function to apply an operation to the document
def apply_operation(operation):
    global document
    if operation not in operation_history:
            operation_history.append(operation)  # Log the operation

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
    print(f"[DOCUMENT] {document}")

def request_operations(target_ip, target_port, last_received_uid=None):
    message = json.dumps({
        "type": "request_operations",
        "last_uid": last_received_uid  # UID of the last operation this peer received
    })
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((target_ip, target_port))
        client_socket.sendall(message.encode('utf-8'))

        # Receive the operation history
        data = client_socket.recv(4096).decode('utf-8')
        response = json.loads(data)
        if response["type"] == "operation_history":
            global pending_operations
            with document_lock:
                for op in response["operations"]:
                    if op not in operation_history:
                        pending_operations.append(op)
            process_pending_operations()
    except ConnectionRefusedError:
        print(f"[ERROR] Unable to connect to {target_ip}:{target_port}")
    finally:
        client_socket.close()

# Function to process the pending operations buffer
def process_pending_operations():
    global pending_operations
    with document_lock:  # Ensure thread-safe access
        # Sort pending operations by UID (timestamp)
        pending_operations.sort(key=lambda op: tuple(op["uid"]))
        for operation in pending_operations[:]:  # Iterate over a copy
            apply_operation(operation)  # Apply operation
            pending_operations.remove(operation)  # Remove after applying

def request_document(target_ip, target_port):
    global is_synced
    message = json.dumps({"type": "request_document"})
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((target_ip, target_port))
        client_socket.sendall(message.encode('utf-8'))

        # Receive the document
        data = client_socket.recv(4096).decode('utf-8')  # Larger buffer for document
        response = json.loads(data)
        if response["type"] == "document_state":
            global document
            with document_lock:
                document = response["document"]
            is_synced = True
            print(f"[SYNC] Document synchronized: {document}")
    except ConnectionRefusedError:
        is_synced = False
        print(f"[ERROR] Unable to connect to {target_ip}:{target_port}")
    finally:
        client_socket.close()


# Function to handle incoming messages
def handle_client(conn, addr):
    global is_synced
    print(f"[INFO] Connected by {addr}")
    try:
        while True:
            data = conn.recv(1024).decode('utf-8')
            if not data:
                break
            print(f"[MESSAGE] Received from {addr}: {data}")
            operation = json.loads(data)
            
            # Handle request for document synchronization
            if operation["type"] == "request_document":
                with document_lock:
                    response = {
                        "type": "document_state",
                        "document": document  # Send the entire document
                    }
                conn.sendall(json.dumps(response).encode('utf-8'))
            elif operation["type"] == "request_operations":
                last_uid = tuple(operation["last_uid"]) if operation.get("last_uid") else None
                with document_lock:
                    # Send only operations after the last UID
                    if last_uid:
                        operations_to_send = [
                            op for op in operation_history if tuple(op["uid"]) > last_uid
                        ]
                    else:
                        operations_to_send = operation_history
                    response = {
                        "type": "operation_history",
                        "operations": operations_to_send
                    }
                conn.sendall(json.dumps(response).encode('utf-8'))
            else:
                # Add to pending operations and process
                with document_lock:
                    pending_operations.append(operation)
                process_pending_operations()
    except ConnectionResetError:
        is_synced = False
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

# Function to send an operation to all peers
def broadcast_operation(operation, peers):
    global is_synced
    message = json.dumps(operation)
    for ip, port in peers:
        client_socket = None
        try:
            delay = random.uniform(0.5, 2.0)  # Random delay between 0.5 and 2 seconds
            time.sleep(delay)  # Simulate delay
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((ip, port))
            client_socket.sendall(message.encode('utf-8'))
        except ConnectionRefusedError:
            is_synced = False
            print(f"[ERROR] Could not connect to {ip}:{port}")
        finally:
            if client_socket:
                client_socket.close()

# Auto-sync before edits
def auto_sync_if_needed():
    global is_synced
    if not peers:
        print("[INFO] No peers available. Editing offline.")
        is_synced = True  # Allow offline edits
        return True

    print("[SYNC] Auto-syncing with peers...")
    is_synced = sync_with_multiple_peers()
    return is_synced

# Function to insert a character into the document
def insert_character(position, character):
    if not auto_sync_if_needed():
        print("[ERROR] Cannot edit. Please sync with a peer first.")
        return
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
    broadcast_operation(operation, peers)  # Broadcast to peers

# Function to delete a character from the document
def delete_character(uid):
    if not auto_sync_if_needed():
        print("[ERROR] Cannot edit. Please sync with a peer first.")
        return
    operation = {
        "type": "delete",
        "uid": uid
    }
    with document_lock:
        pending_operations.append(operation)  # Add to pending operations
    process_pending_operations()  # Process operations
    broadcast_operation(operation, peers)  # Broadcast to peers

def sync_with_multiple_peers():
    global document, is_synced
    documents = request_documents_from_peers()
    if documents:
        merged_document = merge_documents(documents)
        with document_lock:
            document = merged_document
            is_synced = True
        print(f"[SYNC] Document synchronized from multiple peers: {document}")
        return True
    else:
        is_synced = False
        print("[ERROR] No peers responded. Unable to sync.")
        return False


def request_documents_from_peers():
    documents = []  # Store document responses from peers
    for ip, port in peers:
        try:
            message = json.dumps({"type": "request_document"})
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((ip, port))
            client_socket.sendall(message.encode('utf-8'))

            # Receive the document
            data = client_socket.recv(4096).decode('utf-8')
            response = json.loads(data)
            if response["type"] == "document_state":
                documents.append(response["document"])
        except ConnectionRefusedError:
            print(f"[ERROR] Could not connect to {ip}:{port}")
        finally:
            client_socket.close()
    return documents


# Function to add a peer to the list
def add_peer(ip, port):
    try:
        test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test_socket.connect((ip, port))
        test_socket.close()
        peers.append((ip, port))
        print(f"[PEER] Added peer {ip}:{port}")
    except ConnectionRefusedError:
        print(f"[ERROR] Unable to connect to peer {ip}:{port}.")

def merge_documents(documents):
    global document
    merged_document = []  # Resulting merged document
    seen_uids = set()  # Track UIDs to avoid duplicates

    # Combine all operations
    for doc in documents:
        for char, uid in doc:
            # Ensure `uid` is a tuple
            if not isinstance(uid, tuple):
                uid = tuple(uid)  # Convert to tuple if it's not already
            
            if uid not in seen_uids:
                merged_document.append((char, uid))
                seen_uids.add(uid)

    # Sort the merged document by UID (to ensure correct order)
    #merged_document.sort(key=lambda x: x[1])  # Sort by UID
    return merged_document


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

def view_document():
    with document_lock:
        # Extract only characters that are not tombstones
        viewable = "".join(char for char, uid in document if char is not None)
    print(f"[DOCUMENT] {viewable}")

def save_document():
    if not peers:
        print("[WARNING] Saving offline. Document is not synchronized with any peers.")
    file_name = input("Enter the file name to save the document (e.g., 'document.txt'): ").strip()
    with document_lock:
        viewable = "".join(char for char, uid in document if char is not None)
    try:
        with open(file_name, "w") as file:
            file.write(viewable)
        print(f"[SAVE] Document saved to {file_name}")
    except IOError as e:
        print(f"[ERROR] Failed to save the document: {e}")


# Main function to run server and interact with the user
if __name__ == "__main__":
    # Start the server in a separate thread
    server_thread = threading.Thread(target=start_server)
    server_thread.daemon = True
    server_thread.start()

    # Start tombstone cleanup timer
    start_cleanup_timer(interval=30)  # Cleanup every 30 seconds
    
    print("[INFO] Server started. Enter 'add', 'edit', 'view', 'save', 'sync', 'reconnect', 'peers', or 'exit'")
    while True:
        command = input(">> ").strip().lower()
        if command == "add":
            # Add a peer
            target_ip = input("Enter peer IP: ")
            target_port = int(input("Enter peer PORT: "))
            add_peer(target_ip, target_port)
        elif command == "edit":
            # Edit the document (insert or delete)
            print("[EDIT] Would you like to insert or delete?")
            action = input("Enter 'insert' or 'delete': ").strip().lower()
            if action == "insert":
                position = int(input("Enter position to insert at: "))
                character = input("Enter character to insert: ")
                insert_character(position, character)
            elif action == "delete":
                uid = input("Enter UID of the character to delete (e.g., '(timestamp, peer_id)'): ")
                delete_character(eval(uid))  # Convert string input to tuple
            else:
                print("[ERROR] Invalid action. Please enter 'insert' or 'delete'.")
        elif command == "view":
            # View the document
            view_document()
        elif command == "save":
            # Save the document to a file
            save_document()
        elif command == "sync":
            sync_with_multiple_peers()
        elif command == "reconnect":
            target_ip = input("Enter the IP of the peer to sync with: ")
            target_port = int(input("Enter the port of the peer to sync with: "))
            last_uid = operation_history[-1]["uid"] if operation_history else None
            request_operations(target_ip, target_port, last_uid)
        elif command == "peers":
            print("[PEERS] Connected peers:")
            for p in peers:
                print(f"- {p[0]}:{p[1]}")
        elif command == "exit":
            print("[INFO] Exiting...")
            break
        else:
            print("[ERROR] Unknown command. Please enter a valid command.")
        