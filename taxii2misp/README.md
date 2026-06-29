# MISP to TAXII Connector

This project implements a connector that synchronizes threat intelligence data from TAXII (Trusted Automated eXchange of Indicator Information) to MISP (Malware Information Sharing Platform). The connector retrieves STIX (Structured Threat Information Expression) data from a TAXII server and pushes it to a MISP instance, enabling organizations to share and collaborate on threat intelligence.

## Project Structure

The project is organized as follows:

```
misp-taxii-connector
├── taxii2misp
│   ├── main.py                # Main logic for TAXII to MISP synchronization
│   ├── clients
│   │   ├── misp_client.py     # MISP client implementation
│   │   └── taxii_client.py    # TAXII client implementation
│   ├── config
│   │   └── settings.py        # Configuration settings for the application
│   ├── services
│   │   └── stix_processor.py   # STIX processing logic
│   ├── utils
│   │   └── signal_handlers.py  # Utility functions for signal handling
│   └── docker
│       ├── Dockerfile         # Dockerfile for building the Docker image
│       └── entrypoint.sh      # Entrypoint script for the Docker container
├── docker-compose.yml          # Docker Compose configuration
├── .env                        # Environment variables for the application
└── README.md                   # Project documentation
```

## Getting Started

To deploy the TAXII to MISP service using Docker Compose, follow these steps:

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd misp-taxii-connector
   ```

2. **Create a `.env` file**:
   Populate the `.env` file with the necessary environment variables, such as TAXII and MISP URLs, API keys, and other configuration settings.

3. **Build and run the Docker containers**:
   Use Docker Compose to build and start the service:
   ```bash
   docker-compose up --build
   ```

4. **Access the logs**:
   Monitor the logs to ensure the service is running correctly:
   ```bash
   docker-compose logs -f
   ```

## Configuration

The application configuration can be found in `taxii2misp/config/settings.py`. Adjust the settings according to your environment and requirements.

## Contributing

Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.