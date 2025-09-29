import requests
import json
import logging

class ElectricClient:
    def __init__(self, electric_url="http://electric:3000"):
        self.base_url = electric_url
        self.session = requests.Session()
        logging.info("ElectricClient initialized.")

    def _get_shape_url(self, shape_definition):
        # In a real implementation, the shape definition would be more complex
        # and likely passed as a parameter. For our specific use case, we
        # are hardcoding a simple shape to get all characters.
        # The shape definition is a placeholder for now.
        return f"{self.base_url}/api/shape"

    def subscribe(self, shape_definition):
        """
        Subscribes to a shape and yields data as it arrives.
        This is a generator function that will block and listen for streaming data.
        """
        shape_url = self._get_shape_url(shape_definition)
        headers = {'Accept': 'application/x-ndjson'}

        logging.info(f"Connecting to Electric shape at: {shape_url}")
        try:
            with self.session.get(shape_url, headers=headers, stream=True) as response:
                response.raise_for_status()
                logging.info("Successfully subscribed to Electric shape. Waiting for data...")
                for line in response.iter_lines():
                    if line:
                        try:
                            # Each line is a JSON object
                            yield json.loads(line)
                        except json.JSONDecodeError:
                            logging.warning(f"Could not decode JSON from line: {line}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to connect or subscribe to Electric: {e}")
            # In a production scenario, you'd want robust retry logic here.
            return