// Internal error type for the Penca storage and API layers.
//
// Variants will emerge as we port the storage clients. Expected categories:
// - resource not found (catalog, schema, table, branch, transaction)
// - resource already exists (duplicate create)
// - invalid argument (malformed identifiers, missing required fields)
// - storage I/O (Postgres, S3, cold format read/write failures)
// - transaction conflicts (expired tx, concurrent commit)
//
// The gRPC server crate (penca-server-grpc) maps these to tonic::Status codes.
