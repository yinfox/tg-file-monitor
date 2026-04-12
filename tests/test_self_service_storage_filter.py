import unittest

import app.app as app_module


class SelfServiceStorageFilterTestCase(unittest.TestCase):
    def test_interactive_filters_unknown_storage_when_mode_is_115(self):
        resources = [
            {
                "slug": "UNKNOWNSTORAGE0001",
                "title": "诊疗中",
                "unlock_points": 0,
            }
        ]

        built, filtered_out, unknown_count = app_module._build_self_service_interactive_resources(
            resources,
            base_url="https://hdhive.com",
            open_api_key="",
            requested_seasons=[],
            resolution_preference="",
            dolby_preference="any",
            storage_mode="115",
        )

        self.assertEqual(built, [])
        self.assertEqual(filtered_out, 1)
        self.assertEqual(unknown_count, 1)

    def test_interactive_filters_non_target_storage_by_url(self):
        resources = [
            {
                "slug": "NON115RESOURCE0002",
                "title": "诊疗中",
                "url": "https://www.123pan.com/s/demo",
                "unlock_points": 0,
            }
        ]

        built, filtered_out, unknown_count = app_module._build_self_service_interactive_resources(
            resources,
            base_url="https://hdhive.com",
            open_api_key="",
            requested_seasons=[],
            resolution_preference="",
            dolby_preference="any",
            storage_mode="115",
        )

        self.assertEqual(built, [])
        self.assertEqual(filtered_out, 1)
        self.assertEqual(unknown_count, 0)

    def test_interactive_keeps_target_storage_by_url(self):
        resources = [
            {
                "slug": "ONLY115RESOURCE003",
                "title": "诊疗中",
                "url": "https://115.com/s/demo?password=abcd",
                "unlock_points": 0,
            }
        ]

        built, filtered_out, unknown_count = app_module._build_self_service_interactive_resources(
            resources,
            base_url="https://hdhive.com",
            open_api_key="",
            requested_seasons=[],
            resolution_preference="",
            dolby_preference="any",
            storage_mode="115",
        )

        self.assertEqual(len(built), 1)
        self.assertEqual(filtered_out, 0)
        self.assertEqual(unknown_count, 0)
        self.assertEqual(built[0].get("storage_guess"), "115")


if __name__ == "__main__":
    unittest.main()
