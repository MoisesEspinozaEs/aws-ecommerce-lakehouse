"""
Seed the product catalog the Glue job joins against.

Generates 500 fake products and uploads them as Parquet to the lake's reference
prefix. Run this once after terraform apply, before the Glue job.

Usage:
    python seed_products.py --bucket <your-lake-bucket>
"""

import argparse
import io
import random

import boto3
import pandas as pd

CATEGORIES = ["electronics", "books", "home", "fashion", "toys"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True, help="Lake S3 bucket name")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()

    rows = [
        {
            "product_id": f"p{i}",
            "category": random.choice(CATEGORIES),
            "price": round(random.uniform(5, 500), 2),
        }
        for i in range(1, 501)
    ]
    df = pd.DataFrame(rows)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3 = boto3.client("s3", region_name=args.region)
    key = "reference/products/products.parquet"
    s3.put_object(Bucket=args.bucket, Key=key, Body=buffer.getvalue())

    print(f"seeded {len(df)} products to s3://{args.bucket}/{key}")


if __name__ == "__main__":
    main()
