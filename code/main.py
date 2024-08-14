import socket
import threading
import time


class SessionHandler(threading.Thread):
    def __init__(self, client_conn, server_conn, data):
        threading.Thread.__init__(self)
        self.client_conn = client_conn
        self.server_conn = server_conn
        self.data = data
        self.bufsize = 1024

    def run(self):
        try:
            self.server_conn.sendall(self.data)  # Send data to the chosen backend server
            response = self.server_conn.recv(self.bufsize)  # Get the response from the backend server
            self.client_conn.sendall(response)  # Forward the response to the client
        finally:
            self.client_conn.close()  # Close the client connection after completion


class Balancer:
    def __init__(self, balancer_host, balancer_port, servers):
        self.host = balancer_host
        self.port = balancer_port
        self.servers = servers
        self.connections = {}
        self.listener = None
        self.bufsize = 1024
        self.sessions = []
        self.update_time = time.time()
        self.loads = {server_id: 0 for server_id in self.servers}
        self._setup()

    def _setup(self):
        for server_id, server_host in self.servers.items():
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn.connect((server_host, self.port))
            self.connections[server_id] = conn

        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.bind((self.host, self.port))
        self.listener.listen(5)

    def start(self):
        while True:
            client_conn, client_addr = self.listener.accept()
            data = client_conn.recv(self.bufsize)
            selected_server = self._pick_server(data)
            handler = SessionHandler(client_conn, selected_server, data)
            self.sessions.append(handler)
            handler.start()

    def _pick_server(self, data):
        now = time.time()
        time_diff = now - self.update_time
        self.update_time = now

        for server_id in self.loads:
            self.loads[server_id] = max(self.loads[server_id] - time_diff, 0)

        estimated_loads = self._estimate_load(data)
        chosen_server = min(estimated_loads, key=estimated_loads.get)

        self.loads[chosen_server] = estimated_loads[chosen_server]
        return self.connections[chosen_server]

    def _estimate_load(self, data):
        factors = {"M": [2, 2, 1], "V": [1, 1, 3], "P": [1, 1, 2]}
        category = data[0]
        duration = int(data[1:])

        return {
            server_id: load + duration * factors[category][server_id]
            for server_id, load in self.loads.items()
        }


if __name__ == '__main__':
    backend_servers = {0: '192.168.0.101', 1: '192.168.0.102', 2: '192.168.0.103'}
    balancer_host = '10.0.0.1'
    balancer_port = 80
    balancer = Balancer(balancer_host, balancer_port, backend_servers)
    balancer.start()
