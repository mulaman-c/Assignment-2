CREATE SCHEMA IF NOT EXISTS customer_service;
CREATE TABLE IF NOT EXISTS customer_service.state (
    key TEXT PRIMARY KEY,
    payload JSONB NOT NULL
);

CREATE SCHEMA IF NOT EXISTS order_service;
CREATE TABLE IF NOT EXISTS order_service.state (
    key TEXT PRIMARY KEY,
    payload JSONB NOT NULL
);

CREATE SCHEMA IF NOT EXISTS restaurant_service;
CREATE TABLE IF NOT EXISTS restaurant_service.state (
    key TEXT PRIMARY KEY,
    payload JSONB NOT NULL
);

CREATE SCHEMA IF NOT EXISTS payment_service;
CREATE TABLE IF NOT EXISTS payment_service.state (
    key TEXT PRIMARY KEY,
    payload JSONB NOT NULL
);

CREATE SCHEMA IF NOT EXISTS delivery_service;
CREATE TABLE IF NOT EXISTS delivery_service.state (
    key TEXT PRIMARY KEY,
    payload JSONB NOT NULL
);

CREATE SCHEMA IF NOT EXISTS notification_service;
CREATE TABLE IF NOT EXISTS notification_service.state (
    key TEXT PRIMARY KEY,
    payload JSONB NOT NULL
);

CREATE SCHEMA IF NOT EXISTS admin_service;
CREATE TABLE IF NOT EXISTS admin_service.state (
    key TEXT PRIMARY KEY,
    payload JSONB NOT NULL
);
