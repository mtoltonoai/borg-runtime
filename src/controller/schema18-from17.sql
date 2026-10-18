-- One same-store terminal observation credit per marked native Usage charge.
-- Caller and accepted policy remain in the original immutable Usage receipt.
CREATE TABLE native_request_diagnostic_credits (
    attempt_id TEXT NOT NULL,
    request_index INTEGER NOT NULL CHECK(request_index>0),
    reservation_id TEXT NOT NULL UNIQUE,
    diagnostic_id TEXT NOT NULL UNIQUE,
    consumed INTEGER NOT NULL CHECK(consumed IN(0,1)),
    PRIMARY KEY(attempt_id,request_index)
);
