from prometheus_client import start_http_server, Gauge
import random, time

# Example metric: Random CPU usage value
g = Gauge('fake_cpu_usage', 'Random CPU usage')

if __name__ == '__main__':
    start_http_server(8001)  # Prometheus scrapes from this port
    while True:
        g.set(random.random() * 100)
        
