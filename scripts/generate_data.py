"""Generate synthetic e-commerce dataset for the ETL pipeline."""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

CATEGORIES = {
    "Technology": ["Phones", "Computers", "Accessories", "Machines", "Copiers"],
    "Furniture": ["Chairs", "Tables", "Bookcases", "Furnishings"],
    "Office Supplies": ["Binders", "Paper", "Art", "Labels", "Fasteners", "Envelopes", "Storage"],
}

SHIP_MODES = ["First Class", "Second Class", "Standard Class", "Same Day"]
SEGMENTS = ["Consumer", "Corporate", "Home Office"]
REGIONS = {
    "West": [("Los Angeles", "California"), ("San Francisco", "California"), ("Seattle", "Washington")],
    "East": [("New York City", "New York"), ("Philadelphia", "Pennsylvania"), ("Boston", "Massachusetts")],
    "Central": [("Chicago", "Illinois"), ("Dallas", "Texas"), ("Houston", "Texas")],
    "South": [("Miami", "Florida"), ("Atlanta", "Georgia"), ("Nashville", "Tennessee")],
}

PRODUCT_NAMES = {
    "Phones": ["iPhone 14 Pro", "Samsung Galaxy S23", "Google Pixel 7", "OnePlus 11"],
    "Computers": ["Dell XPS 15", "MacBook Pro 14", "ThinkPad X1 Carbon", "HP Spectre x360"],
    "Accessories": ["Logitech MX Keys", "Sony WH-1000XM5", "Anker PowerCore", "Belkin USB Hub"],
    "Machines": ["HP LaserJet Pro", "Brother MFC-L2710", "Epson WorkForce"],
    "Copiers": ["Canon ImageRunner", "Xerox VersaLink", "Ricoh IM C3000"],
    "Chairs": ["Herman Miller Aeron", "Steelcase Leap", "IKEA Markus", "Humanscale Freedom"],
    "Tables": ["Uplift V2 Standing Desk", "Flexispot E7", "IKEA Bekant"],
    "Bookcases": ["Billy Bookcase", "Kallax Shelf", "Hemnes Bookcase"],
    "Furnishings": ["Desk Lamp LED", "Monitor Stand", "Cable Management Kit"],
    "Binders": ["Avery Heavy Duty", "Cardinal Premier", "Five Star Flex"],
    "Paper": ["HP Premium32", "Hammermill Color Copy", "Southworth Linen"],
    "Art": ["Sharpie S-Gel Pens", "Post-it Super Sticky", "Expo Dry Erase"],
    "Labels": ["Avery Address Labels", "Dymo LabelWriter", "Brother P-touch"],
    "Fasteners": ["Staples Heavy Duty", "Bostitch EZ Squeeze", "Stanley Bostitch"],
    "Envelopes": ["Quality Park Clasp", "Mead Square Deal", "Universal Peel"],
    "Storage": ["Sterilite Storage", "Banker Box", "Rubbermaid Roughneck"],
}

BASE_PRICES = {
    "Technology": (150, 1800),
    "Furniture": (80, 900),
    "Office Supplies": (5, 120),
}


def random_date(start: datetime, end: datetime) -> datetime:
    return start + timedelta(days=random.randint(0, (end - start).days))


def generate_customers(n: int = 200) -> list[dict]:
    customers = []
    first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer",
                   "Michael", "Linda", "William", "Barbara", "David", "Susan"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
                  "Miller", "Davis", "Rodriguez", "Martinez", "Wilson", "Anderson"]
    for i in range(1, n + 1):
        customers.append({
            "customer_id": f"CUST-{i:04d}",
            "customer_name": f"{random.choice(first_names)} {random.choice(last_names)}",
            "segment": random.choice(SEGMENTS),
        })
    return customers


def generate_orders(customers: list[dict], n: int = 5000) -> list[dict]:
    orders = []
    start = datetime(2021, 1, 1)
    end = datetime(2023, 12, 31)

    for i in range(1, n + 1):
        customer = random.choice(customers)
        category = random.choice(list(CATEGORIES.keys()))
        sub_category = random.choice(CATEGORIES[category])
        product_name = random.choice(PRODUCT_NAMES.get(sub_category, ["Generic Product"]))
        product_id = f"PROD-{abs(hash(product_name)) % 9999:04d}"

        region = random.choice(list(REGIONS.keys()))
        city, state = random.choice(REGIONS[region])

        order_date = random_date(start, end)
        ship_days = {"First Class": 2, "Second Class": 5, "Standard Class": 7, "Same Day": 1}
        ship_mode = random.choice(SHIP_MODES)
        ship_date = order_date + timedelta(days=ship_days[ship_mode] + random.randint(0, 2))

        low, high = BASE_PRICES[category]
        unit_price = round(random.uniform(low, high), 2)
        quantity = random.randint(1, 10)
        discount = random.choice([0, 0, 0, 0.1, 0.2, 0.3, 0.4, 0.5])
        sales = round(unit_price * quantity * (1 - discount), 2)
        margin = random.uniform(0.05, 0.45)
        profit = round(sales * margin * random.uniform(0.5, 1.2), 2)

        orders.append({
            "order_id": f"ORD-{i:05d}",
            "customer_id": customer["customer_id"],
            "order_date": order_date.strftime("%Y-%m-%d"),
            "ship_date": ship_date.strftime("%Y-%m-%d"),
            "ship_mode": ship_mode,
            "product_id": product_id,
            "product_name": product_name,
            "category": category,
            "sub_category": sub_category,
            "sales": sales,
            "quantity": quantity,
            "discount": discount,
            "profit": profit,
            "city": city,
            "state": state,
            "country": "United States",
            "region": region,
        })
    return orders


def save_csv(data: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"Saved {len(data)} rows → {path}")


if __name__ == "__main__":
    out = Path(__file__).parent.parent / "data" / "raw"
    customers = generate_customers(200)
    orders = generate_orders(customers, 5000)
    save_csv(customers, out / "customers.csv")
    save_csv(orders, out / "orders.csv")
    print("Data generation complete.")
