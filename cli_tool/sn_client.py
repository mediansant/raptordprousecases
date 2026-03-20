"""
RaptorDB Pro Readiness Analyzer — ServiceNow REST API Client
Handles authentication, pagination, and rate-limit-safe API calls.
"""

import requests
import urllib3
import time
from typing import Optional, Dict, List, Any

# Suppress InsecureRequestWarning for sub-prod instances
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class SNClient:
    """Lightweight ServiceNow REST API client for data collection."""

    def __init__(self, instance_url: str, username: str, password: str,
                 verify_ssl: bool = True, timeout: int = 30):
        self.base_url = instance_url.rstrip("/")
        self.auth = (username, password)
        self.verify = verify_ssl
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json"
        })

    def test_connection(self) -> Dict[str, Any]:
        """Test connection and return instance info."""
        try:
            # Quick check — pull a few system properties
            resp = self.session.get(
                f"{self.base_url}/api/now/table/sys_properties",
                params={
                    "sysparm_query": "name=glide.buildtag",
                    "sysparm_fields": "name,value",
                    "sysparm_limit": 1
                },
                verify=self.verify,
                timeout=self.timeout
            )
            if resp.status_code == 200:
                data = resp.json().get("result", [])
                build = data[0]["value"] if data else "unknown"
                return {
                    "success": True,
                    "message": f"Connected — Build: {build}",
                    "build": build,
                    "status_code": 200
                }
            elif resp.status_code == 401:
                return {"success": False, "message": "Authentication failed. Check credentials.", "status_code": 401}
            elif resp.status_code == 403:
                return {"success": False, "message": "Access denied. User may lack admin/rest_api_explorer role.", "status_code": 403}
            else:
                return {"success": False, "message": f"HTTP {resp.status_code}: {resp.text[:200]}", "status_code": resp.status_code}
        except requests.exceptions.ConnectionError:
            return {"success": False, "message": f"Cannot reach {self.base_url}. Check the instance URL.", "status_code": 0}
        except requests.exceptions.Timeout:
            return {"success": False, "message": "Connection timed out.", "status_code": 0}
        except Exception as e:
            return {"success": False, "message": f"Error: {str(e)}", "status_code": 0}

    def get_table(self, table: str, query: str = "", fields: str = "",
                  limit: int = 500, order_by: str = "",
                  display_value: str = "false",
                  max_pages: int = 20) -> List[Dict]:
        """
        Fetch records from a table with auto-pagination.
        Returns list of record dicts.
        """
        all_records = []
        offset = 0

        for page in range(max_pages):
            params = {
                "sysparm_limit": limit,
                "sysparm_offset": offset,
                "sysparm_display_value": display_value,
                "sysparm_exclude_reference_link": "true"
            }
            if fields:
                params["sysparm_fields"] = fields
            combined_query = "^".join(filter(None, [query, order_by]))
            if combined_query:
                params["sysparm_query"] = combined_query

            try:
                resp = self.session.get(
                    f"{self.base_url}/api/now/table/{table}",
                    params=params,
                    verify=self.verify,
                    timeout=self.timeout
                )

                if resp.status_code == 429:
                    # Rate limited — wait and retry (max 3 attempts per page)
                    retry_count = getattr(self, "_retry_count", 0) + 1
                    self._retry_count = retry_count
                    if retry_count > 3:
                        self._retry_count = 0
                        break
                    time.sleep(2 * retry_count)
                    continue
                self._retry_count = 0

                if resp.status_code != 200:
                    break

                records = resp.json().get("result", [])
                if not records:
                    break

                all_records.extend(records)
                offset += limit

                # Check if we got fewer than limit (last page)
                if len(records) < limit:
                    break

                # Small delay to be nice to the instance
                time.sleep(0.1)

            except Exception:
                break

        return all_records

    def get_stats(self, table: str, query: str = "",
                  count: bool = True,
                  avg_fields: str = "",
                  sum_fields: str = "",
                  group_by: str = "") -> Dict:
        """
        Use the Stats API for aggregation queries.
        Returns the stats result dict.
        """
        params = {"sysparm_display_value": "false"}

        if query:
            params["sysparm_query"] = query
        if count:
            params["sysparm_count"] = "true"
        if avg_fields:
            params["sysparm_avg_fields"] = avg_fields
        if sum_fields:
            params["sysparm_sum_fields"] = sum_fields
        if group_by:
            params["sysparm_group_by"] = group_by

        try:
            resp = self.session.get(
                f"{self.base_url}/api/now/stats/{table}",
                params=params,
                verify=self.verify,
                timeout=self.timeout
            )
            if resp.status_code == 200:
                return resp.json().get("result", {})
            return {}
        except Exception:
            return {}

    def get_row_count(self, table: str, query: str = "") -> int:
        """Get row count for a table using Stats API."""
        stats = self.get_stats(table, query=query, count=True)
        try:
            return int(stats.get("stats", {}).get("count", 0))
        except (ValueError, TypeError, AttributeError):
            return 0

    def get_properties(self, prefix: str = "", names: List[str] = None) -> List[Dict]:
        """Fetch system properties by prefix or specific names."""
        if names:
            query = "nameIN" + ",".join(names)
        elif prefix:
            query = f"nameSTARTSWITH{prefix}"
        else:
            query = ""

        return self.get_table(
            "sys_properties",
            query=query,
            fields="name,value,description",
            limit=200
        )
