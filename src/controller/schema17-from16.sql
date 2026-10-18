-- Bounded pieces retain one original parsed provider output. Each insertion and
-- its original operation receipt share the database owner's transaction.
CREATE TABLE charter_output_chunks (
    request_id TEXT NOT NULL REFERENCES charter_requests(id),
    chunk_index INTEGER NOT NULL CHECK(chunk_index >= 0),
    operation_id TEXT NOT NULL UNIQUE,
    reference TEXT NOT NULL,
    body BLOB NOT NULL,
    PRIMARY KEY(request_id,chunk_index)
);
