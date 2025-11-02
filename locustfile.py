from locust import HttpUser, task, between
import csv, random

class ScamDetectorUser(HttpUser):
    # Keep load constant (no wait between tasks)
    wait_time = between(0, 0)
    host = "http://<YOUR_RAY_HEAD_NODE_IP>:8000"

    def on_start(self):
        """Load test data from CSV once per worker."""
        with open("test_data.csv", "r") as f:
            self.samples = [row for row in csv.DictReader(f)]
        if not self.samples:
            raise ValueError("No test samples found in test_data.csv")

    @task
    def predict(self):
        """Send request to the /predict endpoint."""
        payload = random.choice(self.samples)
        self.client.post("/predict", json=payload)
