"""
Generador de datos sintéticos de retail.

Simula el export diario de un sistema POS: tiendas, productos y transacciones
de venta. Pensado para alimentar el pipeline de ingesta sin depender de un
dataset externo
"""

from __future__ import annotations

import argparse
import csv
import random
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path

random.seed(42)  # reproducibilidad: mismos datos en cada corrida de demo

CATEGORIES = ["Electrónica", "Hogar", "Ropa", "Deportes", "Juguetes", "Belleza"]
CITIES = ["Cali", "Bogotá", "Medellín", "Barranquilla", "Bucaramanga"]


@dataclass(frozen=True)
class Store:
    store_id: int
    store_name: str
    city: str


@dataclass(frozen=True)
class Product:
    product_id: int
    product_name: str
    category: str
    unit_price: float


def generate_stores(n: int = 8) -> list[Store]:
    return [
        Store(store_id=i, store_name=f"Tienda {city} {i}", city=city)
        for i, city in enumerate(random.choices(CITIES, k=n), start=1)
    ]


def generate_products(n: int = 40) -> list[Product]:
    products = []
    for i in range(1, n + 1):
        category = random.choice(CATEGORIES)
        price = round(random.uniform(8_000, 850_000), 2)
        products.append(
            Product(
                product_id=i,
                product_name=f"{category} Item {i:03d}",
                category=category,
                unit_price=price,
            )
        )
    return products


def generate_daily_sales(
    sale_date: date,
    stores: list[Store],
    products: list[Product],
    n_transactions: int = 250,
) -> list[dict]:
    """Genera transacciones de un día. Cada fila es una línea de venta."""
    rows = []
    for i in range(1, n_transactions + 1):
        store = random.choice(stores)
        product = random.choice(products)
        quantity = random.randint(1, 5)
        # ~3% de filas "sucias" a propósito, para que los tests de dbt
        # tengan algo real que atrapar (cantidad negativa simulando
        # una devolución mal registrada en el POS de origen).
        if random.random() < 0.03:
            quantity = -quantity

        rows.append(
            {
                "sale_id": f"{sale_date.isoformat()}-{i:05d}",
                "sale_date": sale_date.isoformat(),
                "store_id": store.store_id,
                "product_id": product.product_id,
                "quantity": quantity,
                "unit_price": product.unit_price,
                "total_amount": round(quantity * product.unit_price, 2),
            }
        )
    return rows


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days", type=int, default=14, help="Días de histórico a generar"
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--transactions-per-day", type=int, default=250)
    args = parser.parse_args()

    stores = generate_stores()
    products = generate_products()

    write_csv(args.output_dir / "stores.csv", [asdict(s) for s in stores])
    write_csv(args.output_dir / "products.csv", [asdict(p) for p in products])

    today = date.today()
    for offset in range(args.days):
        sale_date = today - timedelta(days=offset)
        rows = generate_daily_sales(
            sale_date, stores, products, args.transactions_per_day
        )
        write_csv(args.output_dir / f"sales_{sale_date.isoformat()}.csv", rows)

    print(
        f"Generado: {len(stores)} tiendas, {len(products)} productos, "
        f"{args.days} días de ventas en {args.output_dir}"
    )


if __name__ == "__main__":
    main()
