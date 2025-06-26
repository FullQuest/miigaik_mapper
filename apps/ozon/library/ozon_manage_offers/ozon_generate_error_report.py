"""Ozon error report generator."""

import csv
import logging

from .params_manager import ParamsConstructor

log = logging.getLogger('ozon_generate_error_report')


class OzonGenerateErrorReport(ParamsConstructor):
    """Class for generating error reports for Ozon."""

    def generate_report(self) -> bool:
        """Generate csv report."""
        error_data = self.fetcher.imported_offers_errors

        csv_columns = ['feed_offer_id', 'errors']
        csv_file = f"{self.fetcher.domain}_errors.csv"

        try:
            with open(csv_file, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                writer.writeheader()
                for error in error_data:
                    writer.writerow(error)
        except IOError:
            print("I/O error")

            return False

        return True
