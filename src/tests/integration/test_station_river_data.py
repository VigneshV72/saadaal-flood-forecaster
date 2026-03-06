import datetime
import unittest
from unittest.mock import MagicMock, patch

from flood_forecaster.data_ingestion.public_schema import station_river_data


class TestStationRiverDataIntegration(unittest.TestCase):
	def test_get_station_mapping_returns_expected_dictionary(self):
		conn = MagicMock()
		conn.execute.return_value = [
			("Belet Weyne", 101),
			("Jowhar", 202),
		]

		mapping = station_river_data.get_station_mapping(conn)

		self.assertEqual(mapping, {"Belet Weyne": 101, "Jowhar": 202})
		conn.execute.assert_called_once()

	def test_identify_gaps_detects_missing_dates(self):
		conn = MagicMock()
		conn.execute.return_value = [
			(datetime.date(2026, 1, 1),),
			(datetime.date(2026, 1, 3),),
		]

		missing_dates = station_river_data.identify_gaps(
			conn,
			location="Jowhar",
			first_date=datetime.date(2026, 1, 1),
			last_date=datetime.date(2026, 1, 3),
		)

		self.assertEqual(missing_dates, [datetime.date(2026, 1, 2)])
		conn.execute.assert_called_once()

	def test_fetch_data_from_public_schema_with_empty_dates_skips_query(self):
		conn = MagicMock()

		data = station_river_data.fetch_data_from_public_schema(conn, swalim_id=12, missing_dates=[])

		self.assertEqual(data, [])
		conn.execute.assert_not_called()

	@patch("flood_forecaster.data_ingestion.public_schema.station_river_data.get_station_mapping")
	@patch("flood_forecaster.data_ingestion.public_schema.station_river_data.DatabaseConnection")
	def test_fill_gaps_returns_false_when_station_mapping_missing(self, mock_db_connection, mock_get_station_mapping):
		mock_conn = MagicMock()
		mock_db = MagicMock()
		mock_db.engine.connect.return_value.__enter__.return_value = mock_conn
		mock_db_connection.return_value = mock_db
		mock_get_station_mapping.return_value = {}

		result = station_river_data.fill_gaps_using_public_schema(config=MagicMock())

		self.assertFalse(result)
		mock_get_station_mapping.assert_called_once_with(mock_conn)

	@patch("flood_forecaster.data_ingestion.public_schema.station_river_data.identify_gaps")
	@patch("flood_forecaster.data_ingestion.public_schema.station_river_data.get_existing_data_range")
	@patch("flood_forecaster.data_ingestion.public_schema.station_river_data.get_station_mapping")
	@patch("flood_forecaster.data_ingestion.public_schema.station_river_data.DatabaseConnection")
	def test_fill_gaps_returns_true_when_no_gaps_found(
		self,
		mock_db_connection,
		mock_get_station_mapping,
		mock_get_existing_data_range,
		mock_identify_gaps,
	):
		mock_conn = MagicMock()
		mock_db = MagicMock()
		mock_db.engine.connect.return_value.__enter__.return_value = mock_conn
		mock_db_connection.return_value = mock_db

		mock_get_station_mapping.return_value = {"Jowhar": 301}
		# 3 expected days and 3 records means no gap.
		mock_get_existing_data_range.return_value = (
			datetime.date(2026, 1, 1),
			datetime.date(2026, 1, 3),
			3,
		)

		result = station_river_data.fill_gaps_using_public_schema(config=MagicMock())

		self.assertTrue(result)
		mock_identify_gaps.assert_not_called()

	@patch("flood_forecaster.data_ingestion.public_schema.station_river_data.insert_missing_data")
	@patch("flood_forecaster.data_ingestion.public_schema.station_river_data.fetch_data_from_public_schema")
	@patch("flood_forecaster.data_ingestion.public_schema.station_river_data.identify_gaps")
	@patch("flood_forecaster.data_ingestion.public_schema.station_river_data.get_existing_data_range")
	@patch("flood_forecaster.data_ingestion.public_schema.station_river_data.get_station_mapping")
	@patch("flood_forecaster.data_ingestion.public_schema.station_river_data.DatabaseConnection")
	def test_fill_gaps_returns_false_when_source_has_no_data(
		self,
		mock_db_connection,
		mock_get_station_mapping,
		mock_get_existing_data_range,
		mock_identify_gaps,
		mock_fetch_data,
		mock_insert_missing_data,
	):
		mock_conn = MagicMock()
		mock_db = MagicMock()
		mock_db.engine.connect.return_value.__enter__.return_value = mock_conn
		mock_db_connection.return_value = mock_db

		missing_date = datetime.date(2026, 1, 2)
		mock_get_station_mapping.return_value = {"Jowhar": 301}
		# Range includes 3 days but only 2 existing records, so one gap.
		mock_get_existing_data_range.return_value = (
			datetime.date(2026, 1, 1),
			datetime.date(2026, 1, 3),
			2,
		)
		mock_identify_gaps.return_value = [missing_date]
		mock_fetch_data.return_value = []

		result = station_river_data.fill_gaps_using_public_schema(config=MagicMock())

		self.assertFalse(result)
		mock_fetch_data.assert_called_once_with(mock_conn, 301, [missing_date])
		mock_insert_missing_data.assert_not_called()


if __name__ == "__main__":
	unittest.main()
