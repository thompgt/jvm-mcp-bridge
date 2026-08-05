-- Sample schema for the README quickstart. Small on purpose: enough tables that a question
-- genuinely requires a schema lookup before a query, and one column (customers.email) that
-- exercises the redaction rule in bridge.example.yaml.

CREATE TABLE customers (
    id         SERIAL PRIMARY KEY,
    name       TEXT        NOT NULL,
    email      TEXT        NOT NULL,
    country    TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    id          SERIAL PRIMARY KEY,
    customer_id INTEGER     NOT NULL REFERENCES customers (id),
    status      TEXT        NOT NULL,
    total_cents BIGINT      NOT NULL,
    placed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    id         SERIAL PRIMARY KEY,
    order_id   INTEGER NOT NULL REFERENCES orders (id),
    sku        TEXT    NOT NULL,
    quantity   INTEGER NOT NULL,
    unit_cents BIGINT  NOT NULL
);

-- Deliberately NOT in the allowlist in bridge.example.yaml. A query that reaches this table
-- through a join must still be denied — that is the case the guardrail tests assert.
CREATE TABLE internal_audit (
    id      SERIAL PRIMARY KEY,
    actor   TEXT NOT NULL,
    action  TEXT NOT NULL,
    secret  TEXT NOT NULL
);

INSERT INTO customers (name, email, country) VALUES
    ('Ada Lovelace',    'ada@example.com',    'GB'),
    ('Grace Hopper',    'grace@example.com',  'US'),
    ('Alan Turing',     'alan@example.com',   'GB'),
    ('Barbara Liskov',  'barbara@example.com','US');

INSERT INTO orders (customer_id, status, total_cents) VALUES
    (1, 'shipped',   12900),
    (1, 'pending',    4500),
    (2, 'shipped',   88000),
    (3, 'cancelled',  2300),
    (4, 'pending',   15750);

INSERT INTO order_items (order_id, sku, quantity, unit_cents) VALUES
    (1, 'SKU-ANALYTIC-1', 1, 12900),
    (2, 'SKU-CABLE-7',    3,  1500),
    (3, 'SKU-SERVER-2',   1, 88000),
    (4, 'SKU-CABLE-7',    1,  2300),
    (5, 'SKU-ANALYTIC-1', 1, 12900),
    (5, 'SKU-CABLE-7',    1,  2850);

INSERT INTO internal_audit (actor, action, secret) VALUES
    ('system', 'rotate-key', 'this-row-must-never-reach-a-model');
