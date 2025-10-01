"""Template parsing component using Drain3."""

from typing import Any, Dict

import polars as pl
from drain3 import TemplateMiner
from drain3.masking import MaskingInstruction
from drain3.template_miner_config import TemplateMinerConfig
from loguru import logger

from .base import BaseComponent


class TemplateParserComponent(BaseComponent):
    """Configurable template extraction component using Drain3"""

    def _validate_config(self):
        pass  # All parameters are optional for Drain3

    def __init__(self, config: Dict):
        super().__init__(config)

        # Configure Drain3
        drain_config = TemplateMinerConfig()

        # Set drain algorithm parameters directly
        drain_config.drain_sim_th = config.get("similarity_threshold", 0.4)
        drain_config.drain_depth = config.get("depth", 4)
        drain_config.drain_max_children = config.get("max_children", 100)
        drain_config.drain_max_clusters = config.get("max_clusters", 1024)

        # Set extra delimiters
        extra_delimiters = config.get("extra_delimiters", ["_"])
        if extra_delimiters:
            drain_config.drain_extra_delimiters = extra_delimiters

        # Set masking patterns
        masking_rules = config.get(
            "masking_rules",
            [
                {
                    "regex_pattern": r"((?<=[^A-Za-z0-9])|^)(([0-9a-f]{2,}:){3,}([0-9a-f]{2,}))((?=[^A-Za-z0-9])|$)",
                    "mask_with": "ID",
                },
                {
                    "regex_pattern": r"((?<=[^A-Za-z0-9])|^)(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})((?=[^A-Za-z0-9])|$)",
                    "mask_with": "IP",
                },
                {
                    "regex_pattern": r"((?<=[^A-Za-z0-9])|^)(0x[a-f0-9A-F]+)((?=[^A-Za-z0-9])|$)",
                    "mask_with": "HEX",
                },
                {
                    "regex_pattern": r"((?<=[^A-Za-z0-9])|^)([\-\+]?\d+)((?=[^A-Za-z0-9])|$)",
                    "mask_with": "NUM",
                },
            ],
        )

        # Convert masking rules to MaskingInstruction objects
        if masking_rules:
            masking_instructions = []
            for rule in masking_rules:
                masking_instructions.append(
                    MaskingInstruction(
                        pattern=rule["regex_pattern"], mask_with=rule["mask_with"]
                    )
                )
            drain_config.masking_instructions = masking_instructions

        drain_config.mask_prefix = config.get("mask_prefix", "<:")
        drain_config.mask_suffix = config.get("mask_suffix", ":>")

        self.template_miner = TemplateMiner(config=drain_config)

    def process(self, data: pl.DataFrame) -> pl.DataFrame:
        """Extract templates from preprocessed logs."""

        if "Content" not in data.columns:
            raise ValueError("Input DataFrame must have 'Content' column")

        templates: list[Dict[str, Any]] = []
        content_list = data["Content"].to_list()

        for i, content in enumerate(content_list):
            if content is None:
                continue

            row = dict(data.row(i, named=True))
            result = self.template_miner.add_log_message(str(content))
            parameters = self.template_miner.get_parameter_list(
                result["template_mined"], str(content)
            )

            row.update(
                {
                    "EventTemplate": result["template_mined"],
                    "TemplateId": result["cluster_id"],
                    "Parameters": parameters,
                }
            )

            templates.append(row)

        if not templates:
            return pl.DataFrame()

        result_df = pl.from_dicts(templates)
        unique_templates = result_df["TemplateId"].n_unique()
        logger.info(
            "Extracted {} unique templates from {} logs",
            unique_templates,
            len(result_df),
        )

        return result_df
