import os

import boto3
from botocore.exceptions import ClientError

from ctos_scrapers.ctos_scraper.config.logging_setup import logger

# ─── ENV CONFIG ───
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")


class CTOSDynamoWriter:
    def __init__(
        self, table_name: str, *, region: str | None = None, unique_key: str = "url"
    ) -> None:
        self.region = region or AWS_REGION
        self.unique_key = unique_key
        self.dynamodb = boto3.resource("dynamodb", region_name=self.region)
        self.table = self.dynamodb.Table(table_name)
        try:
            _ = self.table.table_status  # Validate table access
        except ClientError as e:
            logger.exception(
                "❌ DynamoDB table access error: %s", e.response["Error"]["Message"]
            )
            raise

    def write_item(self, item: dict[str, str]) -> None:
        """Unsafe insert (overwrites if duplicate)."""
        try:
            self.table.put_item(Item=item)
            logger.info("✅ Inserted item: %s", item)
        except ClientError as e:
            logger.exception(
                "❌ Failed to write item: %s", e.response["Error"]["Message"]
            )

    def write_batch(self, items: list[dict[str, str]]) -> None:
        """Unsafe batch insert (overwrites duplicates)."""
        try:
            with self.table.batch_writer() as batch:
                for item in items:
                    batch.put_item(Item=item)
            logger.info("✅ Batch inserted: %d items", len(items))
        except ClientError as e:
            logger.exception(
                "❌ Batch write failed: %s", e.response["Error"]["Message"]
            )

    def fetch_all_items(self) -> list[dict[str, str]]:
        """Returns all items in the table."""
        try:
            all_items = []
            response = self.table.scan()
            all_items.extend(response.get("Items", []))

            while "LastEvaluatedKey" in response:
                response = self.table.scan(
                    ExclusiveStartKey=response["LastEvaluatedKey"]
                )
                all_items.extend(response.get("Items", []))
        except ClientError as e:
            logger.exception(
                "❌ Failed to fetch items: %s", e.response["Error"]["Message"]
            )
            return []
        else:
            logger.info(
                "📥 Pulled %d items from DynamoDB table %s",
                len(all_items),
                self.table.name,
            )
            return all_items

    def fetch_batch(
        self,
        *,
        start_key: dict | None = None,
        limit: int = 100,
        projection: list[str] | None = None,
    ) -> tuple[list[dict[str, str]], dict | None]:
        """Fetch a paginated batch of items with optional projection fields."""
        try:
            kwargs = {"Limit": limit}
            if start_key:
                kwargs["ExclusiveStartKey"] = start_key

            if projection:
                attr_names = {f"#{f}": f for f in projection}
                kwargs["ProjectionExpression"] = ", ".join(attr_names.keys())
                kwargs["ExpressionAttributeNames"] = attr_names

            response = self.table.scan(**kwargs)
            items = response.get("Items", [])
            next_key = response.get("LastEvaluatedKey")

            logger.info(
                "📦 Pulled %d items from table %s (has more: %s)",
                len(items),
                self.table.name,
                bool(next_key),
            )
        except ClientError as e:
            logger.exception(
                "❌ Failed to fetch batch from DynamoDB: %s",
                e.response["Error"]["Message"],
            )
            return [], None
        else:
            return items, next_key

    def put_item_safely(self, item: dict[str, str]) -> bool:
        try:
            key_attr = self.unique_key
            key_placeholder = f"#{key_attr}"

            self.table.put_item(
                Item=item,
                ConditionExpression=f"attribute_not_exists({key_placeholder})",
                ExpressionAttributeNames={key_placeholder: key_attr},
            )
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "ConditionalCheckFailedException":
                # Extract the key value for better logging
                key_value = item.get(key_attr, "unknown")
                logger.debug("⏭️ Skipping duplicate entry: %s", key_value)
            else:
                logger.error("❌ Error inserting item: %s", e, exc_info=True)
            return False
        else:
            return True

    def write_batch_safely(self, items: list[dict]) -> int:
        """Safe batch insert — only inserts unique entries."""
        success_count = 0
        duplicate_count = 0

        for item in items:
            if self.put_item_safely(item):
                success_count += 1
            else:
                duplicate_count += 1

        if duplicate_count > 0:
            logger.info("⏭️ Skipped %d duplicate entries", duplicate_count)
        logger.info(
            "✅ Batch safely inserted: %d of %d items", success_count, len(items)
        )
        return success_count

    def get_last_written_prefix(self, field: str | None = None) -> str | None:
        """Returns first letter of last alphabetically written entry (for resuming)."""
        field = field or self.unique_key
        try:
            response = self.table.scan(
                ProjectionExpression="#u",
                ExpressionAttributeNames={"#u": self.unique_key},
            )
            items = response.get("Items", [])
            values = [item[field] for item in items if field in item]

            if not values:
                return None

            sorted_values = sorted(str(v).strip().lower() for v in values)
            last_value = sorted_values[-1]
            logger.info("🔁 Resuming from last written prefix: %s", last_value)
            return last_value[0] if last_value else None

        except ClientError as exc:
            logger.warning("⚠️ Could not fetch last written prefix: %s", str(exc))
            return None
