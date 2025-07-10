from ctos_scrapers.ctos_scraper.assets import postgres_utils


class URLScrapeTracker:
    def __init__(self, table_name: str) -> None:
        self.table_name = table_name
        postgres_utils.ensure_tracking_table(self.table_name)
        self.scraped = postgres_utils.fetch_tracking_scraped(self.table_name)
        self.errors = postgres_utils.fetch_tracking_errors(self.table_name)

    def mark_scraped(self, prefix: str, page: int) -> None:
        self.scraped.add((prefix, page))
        postgres_utils.insert_tracking(self.table_name, prefix, page, "scraped")

    def log_error(self, prefix: str, page: int, url: str) -> None:
        self.errors.append((prefix, page, url))
        postgres_utils.insert_tracking(self.table_name, prefix, page, "error", url=url)

    def get_resume_point(self) -> str | None:
        seen = {prefix for prefix, _ in self.scraped}
        for c in [chr(x) for x in range(97, 123)] + [str(n) for n in range(10)]:
            if c not in seen:
                return c
        return None

    def get_last_prefix(self) -> str | None:
        if not self.scraped:
            return None
        return sorted(self.scraped)[-1][0]

    def get_last_scraped_page_for_prefix(self, prefix: str) -> int:
        pages = [page for pfx, page in self.scraped if pfx == prefix]
        return max(pages) if pages else 0

    def get_failed_pages(self) -> list[tuple[str, int, str]]:
        failed = []
        for prefix, page, url in self.errors:
            if (prefix, page) not in self.scraped:
                failed.append((prefix, page, url))
        return failed

    def get_failed_pages_with_retry_info(self) -> list[tuple[str, int, str]]:
        """Get failed pages that haven't been successfully scraped yet."""
        failed = []
        for prefix, page, url in self.errors:
            if (prefix, page) not in self.scraped:
                failed.append((prefix, page, url))
        return failed

    def get_retry_statistics(self) -> dict:
        """Get comprehensive statistics about failed pages."""
        return postgres_utils.get_retry_statistics(self.table_name)

    def get_retry_summary(self) -> str:
        """Get a human-readable summary of retry statistics."""
        stats = self.get_retry_statistics()

        if stats["total_failed"] == 0:
            return "✅ No failed pages to retry"

        summary = f"📊 Retry Summary: {stats['total_failed']} total failed pages"

        if stats["total_scraped"] > 0:
            summary += f", {stats['total_scraped']} successfully scraped"

        if stats["retryable_failed"] > 0:
            summary += f" - 🔄 {stats['retryable_failed']} pages available for retry"

        return summary
