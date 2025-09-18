"""A simplified Tap for the Twitter API."""

import copy
from typing import Any, Dict, List, Optional
from singer_sdk import Tap
from singer_sdk import typing as th
from tap_rest_api_msdk.streams import DynamicTwitterStream

class TapTwitterApi(Tap):
    """Twitter API Tap class."""
    name = "tap-twitter-api"

    config_jsonschema = th.PropertiesList(
        th.Property("api_keys", th.ObjectType(th.Property("X-API-Key", th.StringType, required=True)), required=True),
        th.Property("start_date", th.DateTimeType, required=True),
        th.Property("max_ingestion_limit", th.IntegerType),
        th.Property("streams", th.ArrayType(th.ObjectType())),
    ).to_dict()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.total_records_processed = 0
        self.max_ingestion_limit = self.config.get("max_ingestion_limit")
        self.reached_max_limit = False

    def discover_streams(self) -> List[DynamicTwitterStream]:
        """Return a list of discovered streams."""
        streams: List[DynamicTwitterStream] = []

        for base_stream_config in self.config.get("streams", []):
            if self.reached_max_limit:
                self.logger.info("Global record limit reached, skipping remaining stream definitions.")
                break

            iter_cfg = base_stream_config.get("iteration_config")
            if not iter_cfg:
                streams.append(DynamicTwitterStream(tap=self, name=base_stream_config["name"], schema=base_stream_config["schema"], config=base_stream_config))
                continue

            values: List[Any] = []
            if "from_registry" in iter_cfg:
                reg_key = str(iter_cfg["from_registry"])
                if not self.state.get("registry", {}).get(reg_key):
                    self.logger.info(f"Skipping stream '{base_stream_config['name']}' because its source registry '{reg_key}' is empty.")
                    continue
                values = self.state["registry"][reg_key]
            elif "values" in iter_cfg:
                values = list(iter_cfg["values"])

            if not values:
                self.logger.warning(f"Skipping stream '{base_stream_config['name']}' because no values were found for its iteration_config.")
                continue

            api_param_key = iter_cfg.get("api_param_key")
            for val in values:
                s_config = copy.deepcopy(base_stream_config)
                safe_val = str(val).replace("#", "hash_").replace(" ", "_").replace(":", "_")
                s_config["name"] = f"{base_stream_config['name']}__{safe_val}"
                s_config.setdefault("params", {})
                s_config["params"][api_param_key] = iter_cfg.get("api_param_template", "{value}").replace("{value}", str(val))
                if "metadata_key" in iter_cfg:
                    s_config["inject_metadata"] = {iter_cfg["metadata_key"]: val}
                
                streams.append(DynamicTwitterStream(tap=self, name=s_config["name"], schema=s_config["schema"], config=s_config))
        return streams
    