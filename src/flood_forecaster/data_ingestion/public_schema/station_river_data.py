from datetime import date, timedelta
from typing import Dict, List, Tuple

from sqlalchemy import text

from flood_forecaster import DatabaseConnection
from flood_forecaster.utils.configuration import Config
from flood_forecaster.utils.logging_config import get_logger

logger = get_logger(__name__)

def get_station_mapping(conn) -> Dict[str, int]:
    """
    Get mapping of station names to SWALIM internal IDs.
    Returns: {station_name: swalim_internal_id}
    """
    query = text("""
                 SELECT station_name, swalim_internal_id
                 FROM flood_forecaster.river_station_metadata
                 WHERE swalim_internal_id IS NOT NULL
                 ORDER BY station_name
                 """)

    result = conn.execute(query)
    mapping = {}

    for row in result:
        station_name = row[0]
        swalim_id = row[1]
        mapping[station_name] = swalim_id

    return mapping

def get_existing_data_range(conn, location: str) -> Tuple[date | None, date | None, int]:
    """
    Get the date range and count of existing data for a location.
    Returns: (first_date, last_date, record_count)
    """
    query = text("""
                 SELECT MIN(date) as first_date,
                        MAX(date) as last_date,
                        COUNT(DISTINCT date)  as record_count
                 FROM flood_forecaster.historical_river_level
                 WHERE location_name = :location
                 """)

    result = conn.execute(query, {"location": location}).fetchone()

    if result and result[0]:
        return result[0], result[1], result[2]
    else:
        return None, None, 0


def identify_gaps(conn, location: str, first_date: date, last_date: date) -> List[date]:
    """
    Identify missing dates in the date range for a location.
    Returns list of missing dates.
    """
    # Get all existing dates for this location
    query = text("""
                 SELECT date
                 FROM flood_forecaster.historical_river_level
                 WHERE location_name = :location
                   AND date >= :first_date
                   AND date <= :last_date
                 ORDER BY date
                 """)

    result = conn.execute(query, {
        "location": location,
        "first_date": first_date,
        "last_date": last_date
    })

    existing_dates = {row[0] for row in result}

    # Generate all dates in the range
    all_dates = []
    current = first_date
    while current <= last_date:
        all_dates.append(current)
        current += timedelta(days=1)

    # Find missing dates
    missing_dates = [d for d in all_dates if d not in existing_dates]

    return missing_dates


def fetch_data_from_public_schema(conn, swalim_id: int, missing_dates: List[date]) -> List[Tuple[date, float]]:
    """
    Fetch river data from public.station_river_data for the given SWALIM ID and dates.
    Returns: [(date, reading), ...]
    """
    if not missing_dates:
        return []

    min_date = min(missing_dates)
    max_date = max(missing_dates)

    # Query the public schema table
    query = text("""
                 SELECT reading_date, reading
                 FROM public.station_river_data
                 WHERE station_id = :station_id
                   AND reading_date >= :min_date
                   AND reading_date <= :max_date
                   AND reading IS NOT NULL
                 ORDER BY reading_date
                 """)

    try:
        result = conn.execute(query, {
            "station_id": swalim_id,
            "min_date": min_date,
            "max_date": max_date
        })

        data = [(row[0], row[1]) for row in result]
        return data
    except Exception as e:
        logger.error(f"    ⚠️  Error querying public.station_river_data: {e}")
        return []


def insert_missing_data(conn, location: str, data: List[Tuple[date, float]]) -> int:
    """
    Insert missing data into historical_river_level.
    Returns number of records inserted.
    """
    if not data:
        return 0

    # Build insert query
    insert_query = text("""
                        INSERT INTO flood_forecaster.historical_river_level (location_name, date, level_m)
                        VALUES (:location, :date, :level) ON CONFLICT DO NOTHING
                        """)

    inserted = 0
    for date_val, level_val in data:
        try:
            conn.execute(insert_query, {
                "location": location,
                "date": date_val,
                "level": level_val
            })
            inserted += 1
        except Exception as e:
            logger.error(f"    ⚠️  Failed to insert {date_val}: {e}")

    conn.commit()
    return inserted


def fill_gaps_using_public_schema(config: Config) -> bool:
    """Main gap filling process."""
    logger.info("=" * 80)
    logger.info("FILL GAPS IN HISTORICAL RIVER LEVEL DATA")
    logger.info("=" * 80)
    logger.info("")
    logger.info("This fills data gaps using public.station_river_data")
    logger.info("")

    # Configuration
    db = DatabaseConnection(config)

    total_gaps = 0
    total_filled = 0
    total_missing_in_source = 0

    with db.engine.connect() as conn:
        # Step 1: Get station mapping
        logger.info("Step 1: Loading station mapping")
        logger.info("-" * 80)

        station_mapping = get_station_mapping(conn)

        if not station_mapping:
            logger.error("❌ No station mapping found in river_station_metadata")
            logger.error("   Check that swalim_internal_id is populated")
            return False

        logger.info(f"Found {len(station_mapping)} stations with SWALIM IDs:")
        for station_name, swalim_id in station_mapping.items():
            logger.info(f"  - {station_name}: SWALIM ID {swalim_id}")
        logger.info("")

        # Step 2: Analyze gaps for each station
        logger.info("Step 2: Analyzing data gaps")
        logger.info("-" * 80)

        station_gaps = {}

        for station_name in station_mapping.keys():
            first_date, last_date, count = get_existing_data_range(conn, station_name)

            if first_date is None:
                logger.info(f"📍 {station_name}")
                logger.info(f"   No data exists - skipping (use full data import instead)")
                logger.info("")
                continue

            # Calculate expected records
            expected_records = (last_date - first_date).days + 1
            gap_count = expected_records - count

            logger.info(f"📍 {station_name}")
            logger.info(f"   Date range: {first_date} to {last_date}")
            logger.info(f"   Existing records: {count}")
            logger.info(f"   Expected records: {expected_records}")

            if gap_count > 0:
                logger.info(f"   ⚠️  Gaps detected: {gap_count} missing days")

                # Identify specific missing dates
                missing_dates = identify_gaps(conn, station_name, first_date, last_date)
                station_gaps[station_name] = missing_dates
                total_gaps += len(missing_dates)

                logger.info(f"   Missing dates: {len(missing_dates)}")
                if len(missing_dates) <= 10:
                    for d in missing_dates:
                        logger.info(f"      - {d}")
                else:
                    logger.info(f"      First: {missing_dates[0]}")
                    logger.info(f"      Last: {missing_dates[-1]}")
            else:
                logger.info(f"   ✅ No gaps - data is continuous")

            logger.info("")

        if total_gaps == 0:
            logger.info("✅ No gaps found! All stations have continuous data.")
            logger.info("=" * 80)
            return True

        logger.info(f"📊 Total gaps found: {total_gaps} missing days across {len(station_gaps)} stations")
        logger.info("")

        # Step 3: Confirm before filling
        logger.info("⚠️  This will fetch data from public.station_river_data and fill the gaps.")
        logger.info("")

        logger.info("")
        logger.info("Step 3: Filling gaps from public.station_river_data")
        logger.info("-" * 80)

        # Step 4: Fill gaps for each station
        for station_name, missing_dates in station_gaps.items():
            swalim_id = station_mapping[station_name]

            logger.info(f"📍 {station_name} (SWALIM ID: {swalim_id})")
            logger.info(f"   Fetching data for {len(missing_dates)} missing dates...")

            # Fetch data from public schema
            source_data = fetch_data_from_public_schema(conn, swalim_id, missing_dates)

            if not source_data:
                logger.error(f"   ⚠️  No data found in public.station_river_data")
                total_missing_in_source += len(missing_dates)
                logger.info("")
                continue

            logger.info(f"   Found {len(source_data)} records in source table")

            # Filter to only dates that were missing
            missing_dates_set = set(missing_dates)
            filtered_data = [(d, v) for d, v in source_data if d in missing_dates_set]

            logger.info(f"   Inserting {len(filtered_data)} records...")

            # Insert data
            inserted = insert_missing_data(conn, station_name, filtered_data)
            total_filled += inserted

            if inserted > 0:
                logger.info(f"   ✅ Successfully inserted {inserted} records")

            # Check if any dates still missing
            still_missing = len(missing_dates) - inserted
            if still_missing > 0:
                logger.error(f"   ⚠️  {still_missing} dates still missing (no data in source)")
                total_missing_in_source += still_missing

            logger.info("")

        logger.info("=" * 80)
        logger.info("GAP FILLING COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Total gaps found: {total_gaps}")
        logger.info(f"Successfully filled: {total_filled}")
        logger.info(f"Still missing (no source data): {total_missing_in_source}")
        logger.info("")

        if total_filled > 0:
            logger.info("✅ Gaps have been filled! Run check_river_data_availability.py to verify.")
            logger.info("")
            logger.info("Next steps:")
            logger.info("  1. Verify gaps are filled: python scripts/check_river_data_availability.py")
            logger.info("  2. Run catchup: python scripts/catchup_missing_predictions.py")
        else:
            logger.info("⚠️  No data was filled. The source table may not have the data needed.")
            return False

        logger.info("=" * 80)