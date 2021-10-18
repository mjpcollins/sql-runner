
expected_loaded_query_string = """
-- DESCRIPTION: Table to create a unique joining column
-- OVERWRITE: False
-- PARTITION: True
-- PARTITION_BY: DATE

SELECT
    registration,
    specification_make,
    specification_model,
    dealer_location_postcode,
    CONCAT(registration, '-',
           specification_make, '-',
           specification_model, '-',
           dealer_location_postcode) AS joining_column
FROM {100_autotrader_filter}
WHERE DATE(_PARTITIONTIME) = "{partition_date}"
"""
