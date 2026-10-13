"""Static reference data shared by the generator (and mirrored in dbt seeds)."""

# (city, traffic weight)
CITIES = [
    ("Casablanca", 5.0), ("Rabat", 3.0), ("Marrakech", 3.0), ("Tangier", 2.0),
    ("Fes", 2.0), ("Agadir", 1.5), ("Oujda", 1.0), ("Tetouan", 1.0),
    ("Meknes", 1.0), ("Kenitra", 1.0),
]

# (product_id, name, category, price in MAD)
PRODUCTS = [
    ("P001", "Wireless Earbuds", "Electronics", 349.00),
    ("P002", "Smartphone Case", "Electronics", 89.00),
    ("P003", "Power Bank 20000mAh", "Electronics", 299.00),
    ("P004", "Bluetooth Speaker", "Electronics", 459.00),
    ("P005", "Smart Watch", "Electronics", 799.00),
    ("P006", "Running Shoes", "Sports", 599.00),
    ("P007", "Yoga Mat", "Sports", 149.00),
    ("P008", "Water Bottle", "Sports", 59.00),
    ("P009", "Gym Backpack", "Sports", 229.00),
    ("P010", "Cotton T-Shirt", "Fashion", 99.00),
    ("P011", "Denim Jacket", "Fashion", 389.00),
    ("P012", "Leather Wallet", "Fashion", 179.00),
    ("P013", "Sunglasses", "Fashion", 249.00),
    ("P014", "Desk Lamp", "Home", 129.00),
    ("P015", "Coffee Maker", "Home", 549.00),
    ("P016", "Throw Pillow", "Home", 69.00),
    ("P017", "Argan Oil", "Beauty", 119.00),
    ("P018", "Face Cream", "Beauty", 139.00),
    ("P019", "Perfume 50ml", "Beauty", 429.00),
    ("P020", "Hair Dryer", "Beauty", 259.00),
]

DEVICES = ["mobile", "desktop", "tablet"]
PAYMENT_METHODS = ["card", "wallet", "bank_transfer"]
