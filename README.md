# Simple Object Storage API
## Notes

- **Base64 Encoding**: All binary data must be encoded as Base64 before sending or receiving through the API.
- **ID Field**: The `id` is a unique identifier for the blob. It can be anything as long as it's a string.
- **Metadata**: The system stores blob metadata (id, size, timestamp, backend) separately from the actual data.
- **Bearer Token**: Every request must include `Authorization: Bearer hello1234<token>`.
- **Backend Selection**: The backend is chosen by the API endpoint (`/local`, `/db`, `/s3`), for example: "http://localhost:9000/v1/local/blobs" or "http://localhost:9000/v1/db/blobs" or "http://localhost:9000/v1/s3/blobs".
- **S3 Access**: S3 interactions are done over plain HTTP. i have include my own bucket whcih is controlled by a policy to allow POSTs and GETs,  into a folder on the bucket called uploads.
## Backends

1. **Local File System**  
   Stores blobs as files in a specified directory on the server.

2. **Database Table**  
   Saves blob data directly in a separate database table.

3. **Amazon S3 Storage**  
   Uploads blobs to an S3 bucket via HTTP requests.

