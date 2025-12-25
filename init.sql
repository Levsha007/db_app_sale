-- Создание таблиц для системы управления магазином

-- Таблица покупателей
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE,
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица товаров
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    weight_kg DECIMAL(6,3),
    stock_quantity INTEGER NOT NULL,
    is_available BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица заказов
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10,2) NOT NULL,
    is_paid BOOLEAN DEFAULT FALSE,
    is_delivered BOOLEAN DEFAULT FALSE,
    delivery_address TEXT
);

-- Таблица состава заказов
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    price_per_item DECIMAL(10,2) NOT NULL,
    total_price DECIMAL(10,2) NOT NULL
);

-- Таблица отзывов
CREATE TABLE IF NOT EXISTS reviews (
    review_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(product_id),
    customer_id INTEGER REFERENCES customers(customer_id),
    rating INTEGER CHECK (rating BETWEEN 1 AND 5),
    comment_text TEXT,
    review_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_approved BOOLEAN DEFAULT TRUE
);

-- Таблица поставщиков
CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id SERIAL PRIMARY KEY,
    company_name VARCHAR(200) NOT NULL,
    contact_person VARCHAR(100),
    phone VARCHAR(20),
    email VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE
);

-- Добавление тестовых данных покупателей
INSERT INTO customers (first_name, last_name, email, phone) VALUES
('Иван', 'Иванов', 'ivan@mail.ru', '+79991234567'),
('Мария', 'Петрова', 'maria@yandex.ru', '+79997654321'),
('Алексей', 'Сидоров', 'alex@list.ru', '+79995551234')
ON CONFLICT DO NOTHING;

-- Добавление тестовых данных товаров
INSERT INTO products (product_name, description, price, weight_kg, stock_quantity) VALUES
('Ноутбук ASUS', '15.6 дюймов, 16 ГБ ОЗУ, SSD 512 ГБ', 89999.99, 2.3, 10),
('Смартфон iPhone 14', '128 ГБ, черный цвет', 79999.50, 0.172, 25),
('Наушники Sony', 'Беспроводные, с шумоподавлением', 14999.00, 0.25, 50),
('Монитор Samsung', '27 дюймов, 4K, IPS матрица', 45999.00, 5.1, 8)
ON CONFLICT DO NOTHING;

-- Добавление тестовых данных заказов
INSERT INTO orders (customer_id, total_amount, delivery_address) VALUES
(1, 89999.99, 'Москва, ул. Ленина, д. 10'),
(2, 154999.49, 'Санкт-Петербург, пр. Победы, д. 5'),
(3, 29998.00, 'Екатеринбург, ул. Мира, д. 15')
ON CONFLICT DO NOTHING;

-- Добавление тестовых данных состава заказов
INSERT INTO order_items (order_id, product_id, quantity, price_per_item, total_price) VALUES
(1, 1, 1, 89999.99, 89999.99),
(2, 2, 1, 79999.50, 79999.50),
(2, 3, 1, 14999.00, 14999.00),
(3, 3, 2, 14999.00, 29998.00)
ON CONFLICT DO NOTHING;

-- Добавление тестовых данных отзывов
INSERT INTO reviews (product_id, customer_id, rating, comment_text) VALUES
(1, 1, 5, 'Отличный ноутбук, быстрый и тихий'),
(2, 2, 4, 'Хороший телефон, но дорогой'),
(3, 3, 5, 'Наушники супер, звук чистейший')
ON CONFLICT DO NOTHING;

-- Добавление тестовых данных поставщиков
INSERT INTO suppliers (company_name, contact_person, phone, email) VALUES
('Электроникс Трейд', 'Ольга Смирнова', '+74951234567', 'info@electronics.ru'),
('Гаджет Про', 'Дмитрий Козлов', '+74957654321', 'supply@gadgetpro.com')
ON CONFLICT DO NOTHING;