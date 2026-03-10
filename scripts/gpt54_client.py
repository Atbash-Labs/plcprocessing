#!/usr/bin/env python3
"""
GPT-5.4 multimodal client for artifact extraction.

Handles image (P&ID, engineering diagram) and document (SOP PDF/text)
understanding via the OpenAI API, producing structured JSON that feeds
into the artifact normalization pipeline.
"""

import os
import sys
import json
import base64
import time
from pathlib import Path
from typing import Dict, List, Optional, Any

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*_a, **_kw):
        return False

load_dotenv()


class GPT54Client:
    """
    OpenAI GPT-5.4 client for multimodal artifact extraction.

    Supports:
    - Image inputs (P&IDs, engineering diagrams)
    - Text/PDF inputs (SOPs, procedures)
    - Structured JSON output via response_format
    """

    DEFAULT_MODEL = "gpt-5.4"

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
    ):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError(
                "OPENAI_API_KEY not found. Set it in .env or pass api_key."
            )
        self.model = model or os.getenv("OPENAI_MODEL", self.DEFAULT_MODEL)

        import openai
        self.client = openai.OpenAI(api_key=self.api_key, timeout=300.0)

    # ------------------------------------------------------------------
    # Image encoding helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _encode_image(image_path: str) -> str:
        with open(image_path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")

    @staticmethod
    def _image_media_type(path: str) -> str:
        ext = Path(path).suffix.lower()
        return {
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".gif": "image/gif",
            ".webp": "image/webp",
            ".bmp": "image/bmp",
            ".tiff": "image/tiff",
            ".tif": "image/tiff",
        }.get(ext, "image/png")

    # ------------------------------------------------------------------
    # Core extraction methods
    # ------------------------------------------------------------------

    def extract_from_image(
        self,
        image_path: str,
        source_kind: str = "pid",
        existing_entities: Optional[Dict[str, List[str]]] = None,
        verbose: bool = False,
    ) -> Dict[str, Any]:
        """
        Extract structured process facts from an image (P&ID / diagram).

        Args:
            image_path: Path to the image file.
            source_kind: "pid", "diagram", or "sop".
            existing_entities: Dict mapping label -> list of known names
                for entity linking hints.
            verbose: Print debug output.

        Returns:
            Raw parsed JSON dict from GPT-5.4.
        """
        b64 = self._encode_image(image_path)
        media = self._image_media_type(image_path)

        system_prompt = self._build_system_prompt(source_kind, existing_entities)
        user_content = [
            {
                "type": "image_url",
                "image_url": {
                    "url": f"data:{media};base64,{b64}",
                    "detail": "high",
                },
            },
            {
                "type": "text",
                "text": self._build_user_prompt(source_kind, image_path),
            },
        ]

        return self._call(system_prompt, user_content, verbose=verbose)

    def extract_from_text(
        self,
        text: str,
        source_file: str = "",
        source_kind: str = "sop",
        existing_entities: Optional[Dict[str, List[str]]] = None,
        verbose: bool = False,
    ) -> Dict[str, Any]:
        """
        Extract structured process facts from text (SOP / procedure).

        Args:
            text: The document text content.
            source_file: Original file path for reference.
            source_kind: "sop", "procedure", or "manual".
            existing_entities: Dict mapping label -> list of known names.
            verbose: Print debug output.

        Returns:
            Raw parsed JSON dict from GPT-5.4.
        """
        system_prompt = self._build_system_prompt(source_kind, existing_entities)
        user_content = [
            {
                "type": "text",
                "text": self._build_user_prompt(source_kind, source_file)
                    + f"\n\n--- DOCUMENT CONTENT ---\n{text[:60000]}",
            },
        ]

        return self._call(system_prompt, user_content, verbose=verbose)

    def resolve_entities(
        self,
        extracted_mentions: Dict[str, List[str]],
        existing_entities: Dict[str, List[str]],
        verbose: bool = False,
    ) -> Dict[str, Dict[str, str]]:
        """
        Use GPT to match extracted entity mentions to existing graph nodes.

        Args:
            extracted_mentions: Dict mapping label -> list of names from extraction.
                e.g. {"Equipment": ["Brew Kettle BK-001", "HX-200"], "ScadaTag": ["TT501"]}
            existing_entities: Dict mapping label -> list of known names in graph.
                e.g. {"Equipment": ["BK-001", "HX-200-A"], "ScadaTag": ["Area5/TT501.PV"]}

        Returns:
            Dict mapping label -> {extracted_name: resolved_name_or_null}.
            resolved_name is the exact existing name if matched, or null if no match.
        """
        system_prompt = """You are an expert at matching industrial equipment and tag names across different naming conventions.

You will receive two lists per entity type:
- "extracted": names found in a P&ID, SOP, or engineering diagram
- "existing": names already in the plant's ontology database

Your job is to determine which extracted names refer to the same entity as which existing names.

Industrial naming conventions vary: a P&ID might say "Brew Kettle BK-001" while the SCADA system has "BK-001" or "BK_001_BrewKettle". Tags like "TT-501" might appear in SCADA as "Area5/TT501.PV" or "TT_501_Temperature".

ViewComponents are SCADA UI elements that visualize equipment. A ViewComponent whose name or path references an equipment ID likely VISUALIZES that equipment. When you see Equipment in the extracted list AND ViewComponent in the existing list, also return a "visualizes" key mapping equipment names to the ViewComponent names that display them.

Rules:
- Match based on tag numbers, equipment IDs, and functional identity -- not just substring overlap.
- If an extracted name clearly refers to an existing entity, map it to the EXACT existing name.
- If there is no plausible match, map it to null.
- When in doubt, prefer no match over a wrong match.

Return JSON:
{
  "Equipment": { "extracted_name": "existing_name_or_null", ... },
  "ScadaTag": { "extracted_name": "existing_name_or_null", ... },
  "visualizes": { "equipment_name": ["ViewComponent_name", ...], ... }
}"""

        user_parts = []
        for label in extracted_mentions:
            ext_list = extracted_mentions[label]
            exist_list = existing_entities.get(label, [])
            if not ext_list:
                continue
            user_parts.append(f"## {label}")
            user_parts.append(f"Extracted: {json.dumps(ext_list)}")
            user_parts.append(f"Existing:  {json.dumps(exist_list[:200])}")
            user_parts.append("")

        vc_list = existing_entities.get("ViewComponent", [])
        if "Equipment" in extracted_mentions and vc_list:
            user_parts.append("## ViewComponent (existing only — for VISUALIZES linking)")
            user_parts.append(f"Existing:  {json.dumps(vc_list[:300])}")
            user_parts.append("")

        if not user_parts:
            return {}

        user_content = [{"type": "text", "text": "\n".join(user_parts)}]
        result = self._call(system_prompt, user_content, verbose=verbose)

        mappings: Dict[str, Any] = {}
        for label, matches in result.items():
            if isinstance(matches, dict):
                mappings[label] = {
                    k: v for k, v in matches.items()
                    if isinstance(k, str)
                }
        return mappings

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _call(
        self,
        system_prompt: str,
        user_content: List[Dict],
        verbose: bool = False,
    ) -> Dict[str, Any]:
        """Make the actual API call and parse JSON response."""
        if verbose:
            print(f"[GPT54] Calling {self.model}...", file=sys.stderr, flush=True)

        start = time.time()
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            max_completion_tokens=16000,
            temperature=0.1,
            response_format={"type": "json_object"},
        )
        elapsed = time.time() - start

        text = response.choices[0].message.content or "{}"
        if verbose:
            tokens = response.usage
            print(
                f"[GPT54] Done in {elapsed:.1f}s "
                f"(in={tokens.prompt_tokens}, out={tokens.completion_tokens})",
                file=sys.stderr, flush=True,
            )

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {"error": "Failed to parse GPT response as JSON", "raw": text[:2000]}

    def _build_system_prompt(
        self,
        source_kind: str,
        existing_entities: Optional[Dict[str, List[str]]] = None,
    ) -> str:
        entity_hint = ""
        if existing_entities:
            parts = []
            for label, names in existing_entities.items():
                sample = names[:30]
                parts.append(f"  {label}: {json.dumps(sample)}")
            entity_hint = (
                "\n\nKnown entities already in the ontology (use these exact names when possible):\n"
                + "\n".join(parts)
            )

        return f"""You are an expert process engineer analyzing industrial plant documentation.

Your job is to extract structured facts from a {source_kind.upper()} source and return them as JSON.

Extract ALL of the following when present:
1. Equipment facts: names, services, functions, media handled, operations performed, operating parameters with ranges/limits.
2. Tag/instrument facts: tag names, what they measure, units, process context.
3. Process media: fluids, gases, utilities, products flowing through the system.
4. Unit operations: what operations each piece of equipment performs (pumping, heating, mixing, filtration, etc.).
5. Chemical species: chemicals, additives, reactants, products mentioned.
6. Reactions: any chemical or physical transformations described.
7. Relationships: connections between equipment, tags, media, operations, and species.

Rules:
- Use exact equipment/tag names from the source when possible.
- When a name matches a known entity, use that exact name.
- For operating parameters, extract numeric limits when available.
- Classify media as: utility, product, waste, solvent, or gas.
- Classify operations as: transfer, thermal, mixing, separation, cleaning, or reaction.
- Return valid JSON matching the schema described below.
{entity_hint}

Response schema:
{{
  "equipment_facts": [{{
    "equipment_name": "string",
    "service": "string",
    "function": "string",
    "media_handled": ["string"],
    "operations_performed": ["string"],
    "operating_parameters": [{{
      "parameter": "string",
      "unit": "string",
      "normal_low": number_or_null,
      "normal_high": number_or_null,
      "alarm_low": number_or_null,
      "alarm_high": number_or_null,
      "trip_low": number_or_null,
      "trip_high": number_or_null
    }}]
  }}],
  "tag_facts": [{{
    "tag_name": "string",
    "measures": "string",
    "process_context": "string",
    "unit": "string"
  }}],
  "process_media": [{{
    "name": "string",
    "category": "string",
    "phase": "string",
    "description": "string"
  }}],
  "unit_operations": [{{
    "name": "string",
    "category": "string",
    "description": "string"
  }}],
  "chemical_species": [{{
    "name": "string",
    "category": "string",
    "cas_number": "string",
    "description": "string"
  }}],
  "reactions": [{{
    "name": "string",
    "category": "string",
    "description": "string",
    "species_involved": ["string"]
  }}],
  "relationships": [{{
    "source_type": "string",
    "source_name": "string",
    "relationship": "string",
    "target_type": "string",
    "target_name": "string"
  }}]
}}"""

    def _build_user_prompt(self, source_kind: str, source_path: str) -> str:
        kind_labels = {
            "pid": "P&ID (Piping and Instrumentation Diagram)",
            "diagram": "engineering diagram",
            "sop": "Standard Operating Procedure",
            "procedure": "operating procedure",
            "manual": "equipment manual",
        }
        label = kind_labels.get(source_kind, source_kind)
        return (
            f"Analyze this {label} and extract all process facts.\n"
            f"Source file: {source_path}\n"
            f"Return a single JSON object with all extracted facts."
        )
