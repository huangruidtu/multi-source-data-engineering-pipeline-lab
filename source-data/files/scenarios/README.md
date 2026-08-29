# Missing-file scenario

`expected-location-overrides.csv` is intentionally absent. Later file-ingestion work should attempt to discover this expected path and record a deterministic missing-file failure rather than silently treating the feed as complete.

The valid and invalid fixture files are immutable inputs. `product_categories_duplicate.csv` has the same bytes as `product_categories.csv` and is the duplicate-content exercise.
