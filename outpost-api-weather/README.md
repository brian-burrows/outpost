# Outpost API Weather Service

The Outpost Weather Service is designed as the dedicated persistence and access layer for all weather-related data.
Its core responsibility is to maintain the data store and provide consistent methods for both retrieval and storage.

The service governs the data store schemas, including:

- City Definitions: Metadata for locations where weather data is collected.
- Weather Data: Definitions for both historical measurements and future forecasts.

## Implementation plan

The deployment strategy is structured in two distinct phases, progressing from a standard REST architecture to a high-performance, layered backend.

### Stage 1 deployment : (HTTP/REST)

This phase establishes initial service functionality and clear access boundaries using standard HTTP protocols.

- All microservices will communicate with the Weather Service via HTTP endpoints utilizing internal network discovery (e.g., Docker or Kubernetes service names).
- External clients access the service via a central API Gateway, limited to Read-Only REST API routes.
- NGINX will be deployed in front of the public routes to block all HTTP write requests.

### Stage 2 deployment : Migration to private backend and RPC layering

This phase shifts the internal communication to a more performant model, adhering to the single-database-per-service principle.

- Microservices will transition to communicating with the Weather Service via a Remote Procedure Call (RPC) interface (e.g., gRPC or Apache Thrift).
  - This offers enhanced performance, lower latency, and stricter contract definitions for high-volume data ingestion.
- Clients will consume data from the Weather Service via a specialized Central REST API layer or a Federated Graph layer.
  - This approach decouples the client-facing API from the internal data model.
- The responsibility for validating user and service credentials will be fully delegated to a separate, dedicated Authentication Service, ensuring robust security across all communication paths
  - Authorization (i.e., what data the customer can access) is still the responsibility of the Outpost Weather Service
