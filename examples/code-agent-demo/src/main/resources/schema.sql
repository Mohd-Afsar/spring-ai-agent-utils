-- Sample e-commerce schema for the database agent demo

CREATE TABLE IF NOT EXISTS customers (
    id         BIGINT       PRIMARY KEY AUTO_INCREMENT,
    name       VARCHAR(100) NOT NULL,
    email      VARCHAR(150) NOT NULL UNIQUE,
    country    VARCHAR(60)  NOT NULL,
    created_at DATE         NOT NULL
);

CREATE TABLE IF NOT EXISTS categories (
    id   BIGINT      PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(80) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS products (
    id          BIGINT         PRIMARY KEY AUTO_INCREMENT,
    name        VARCHAR(150)   NOT NULL,
    category_id BIGINT         NOT NULL REFERENCES categories(id),
    price       DECIMAL(10, 2) NOT NULL,
    stock       INT            NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS orders (
    id          BIGINT         PRIMARY KEY AUTO_INCREMENT,
    customer_id BIGINT         NOT NULL REFERENCES customers(id),
    order_date  DATE           NOT NULL,
    status      VARCHAR(20)    NOT NULL DEFAULT 'pending',
    total       DECIMAL(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS order_items (
    id         BIGINT         PRIMARY KEY AUTO_INCREMENT,
    order_id   BIGINT         NOT NULL REFERENCES orders(id),
    product_id BIGINT         NOT NULL REFERENCES products(id),
    quantity   INT            NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS reviews (
    id          BIGINT      PRIMARY KEY AUTO_INCREMENT,
    product_id  BIGINT      NOT NULL REFERENCES products(id),
    customer_id BIGINT      NOT NULL REFERENCES customers(id),
    rating      TINYINT     NOT NULL CHECK (rating BETWEEN 1 AND 5),
    comment     VARCHAR(500),
    reviewed_at DATE        NOT NULL
);
