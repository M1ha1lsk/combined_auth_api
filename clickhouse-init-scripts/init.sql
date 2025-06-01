CREATE TABLE IF NOT EXISTS customers
(
    customer_id UInt64,
    username String,
    email String,
    created_at DateTime,
    updated_at DateTime,
    batch_time DateTime,
    is_deleted Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS sellers
(
    seller_id UInt64,
    username String,
    email String,
    created_at DateTime,
    updated_at DateTime,
    batch_time DateTime,
    is_deleted Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY seller_id;

CREATE TABLE IF NOT EXISTS products
(
    product_id UUID,
    product_name String,
    product_description String,
    category String,
    price Float32,
    seller_id UInt64,
    created_at DateTime,
    updated_at DateTime,
    is_deleted Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS fact_sales
(
    sale_id UUID,
    customer_id UInt64,
    product_id UUID,
    seller_id UInt64,
    sale_date DateTime,
    quantity UInt32,
    total_amount Float32
)
ENGINE = MergeTree
ORDER BY sale_id;