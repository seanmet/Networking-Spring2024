import socket
import threading
import time


class ClientSession(threading.Thread):
    def __init__(self, client_socket, backend_socket, request_data):
        threading.Thread.__init__(self)
        self.client_socket = client_socket
        self.backend_socket = backend_socket
        self.request_data = request_data
        self.buffer_size = 1024

    def run(self):
        try:
            self.backend_socket.sendall(self.request_data)  # Forward request to the backend server
            backend_response = self.backend_socket.recv(self.buffer_size)  # Receive response from the backend
            self.client_socket.sendall(backend_response)  # Send response back to the client
        finally:
            self.client_socket.close()  # Ensure the client socket is closed


class LoadBalancer:
    def __init__(self, balancer_ip, balancer_port, backend_servers):
        self.balancer_ip = balancer_ip
        self.balancer_port = balancer_port
        self.backend_servers = backend_servers
        self.backend_connections = {}
        self.client_listener = None
        self.buffer_size = 1024
        self.active_sessions = []
        self.last_update_time = time.time()
        self.server_loads = {backend_id: 0 for backend_id in self.backend_servers}
        self._initialize_connections()

    def _initialize_connections(self):
        for backend_id, backend_ip in self.backend_servers.items():
            backend_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            backend_conn.connect((backend_ip, self.balancer_port))
            self.backend_connections[backend_id] = backend_conn

        self.client_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_listener.bind((self.balancer_ip, self.balancer_port))
        self.client_listener.listen(5)

    def start_balancing(self):
        while True:
            client_socket, client_address = self.client_listener.accept()
            request_data = client_socket.recv(self.buffer_size)
            chosen_backend = self._select_backend(request_data)
            session = ClientSession(client_socket, chosen_backend, request_data)
            self.active_sessions.append(session)
            session.start()

    def _select_backend(self, request_data):
        current_time = time.time()
        elapsed_time = current_time - self.last_update_time
        self.last_update_time = current_time

        for backend_id in self.server_loads:
            self.server_loads[backend_id] = max(self.server_loads[backend_id] - elapsed_time, 0)

        projected_loads = self._calculate_projected_loads(request_data)
        optimal_backend = min(projected_loads, key=projected_loads.get)

        self.server_loads[optimal_backend] = projected_loads[optimal_backend]
        return self.backend_connections[optimal_backend]

    def _calculate_projected_loads(self, request_data):
        weights = {"M": [2, 2, 1], "V": [1, 1, 3], "P": [1, 1, 2]}
        request_type = request_data[0]
        request_time = int(request_data[1:])

        return {
            backend_id: load + request_time * weights[request_type][backend_id]
            for backend_id, load in self.server_loads.items()
        }


if __name__ == '__main__':
    servers = {0: '192.168.0.101', 1: '192.168.0.102', 2: '192.168.0.103'}
    ip_address = '10.0.0.1'
    port_number = 80
    load_balancer = LoadBalancer(ip_address, port_number, servers)
    load_balancer.start_balancing()
