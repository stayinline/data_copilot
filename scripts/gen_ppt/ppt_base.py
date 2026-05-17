"""Shared base module for PPT image generation."""

import re
import sys
import urllib.request
from pathlib import Path

import dashscope
from dashscope.aigc.image_generation import ImageGeneration
from dashscope.api_entities.dashscope_response import Message

# Load config
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import MODEL_API_KEY

IMAGE_API_URL = "https://dashscope.aliyuncs.com/api/v1"
dashscope.base_http_api_url = IMAGE_API_URL
API_KEY = MODEL_API_KEY

OUTPUT_DIR = Path(__file__).parent.parent.parent / "docs" / "ppt-images"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PROMPTS_MD = Path(__file__).parent.parent.parent / "docs" / "AI-Competition-Presentation-Image-Prompts.md"

IMAGE_SIZE = "2K"
WATERMARK = False

STYLE_PREFIX = (
    "深蓝背景，电光蓝发光点缀，16:9比例。"
    "画面干净简洁，不要添加多余元素。"
    "画面中所有文字必须使用中文。"
)


def load_prompt(slide_id: str) -> str:
    """Load the prompt for a slide from the prompts markdown file."""
    if not PROMPTS_MD.exists():
        raise FileNotFoundError(f"Prompts file not found: {PROMPTS_MD}")

    content = PROMPTS_MD.read_text(encoding="utf-8")
    # Match section like "## S1 — ..." up to next "## S" or end
    m = re.search(rf"(## {slide_id}\s*—[^\n]*\n.*?)(?=## S\d+|\Z)", content, re.DOTALL)
    if not m:
        raise ValueError(f"Slide {slide_id} not found in {PROMPTS_MD}")

    section = m.group(1)
    lines = []
    in_prompt = False
    for line in section.split("\n"):
        stripped = line.strip()
        if stripped.startswith("**提示词：**"):
            in_prompt = True
            continue
        if not in_prompt:
            continue
        if stripped.startswith("> "):
            lines.append(stripped[2:])
        elif stripped == ">" or stripped == "":
            if lines:
                lines.append("")
        elif stripped.startswith("|") or stripped.startswith("**") or stripped.startswith("## "):
            break
        elif stripped:
            lines.append(stripped)

    # Clean consecutive blank lines
    cleaned, prev_blank = [], False
    for l in lines:
        if l == "":
            if not prev_blank:
                cleaned.append(l)
            prev_blank = True
        else:
            cleaned.append(l)
            prev_blank = False

    prompt = "\n".join(cleaned).strip()
    if not prompt:
        raise ValueError(f"No prompt found for slide {slide_id}")
    return prompt


def generate(slide_id: str, force: bool = False) -> str | None:
    """Generate image for a slide, loading prompt from prompts markdown file."""
    output_path = OUTPUT_DIR / f"{slide_id}.png"

    if output_path.exists() and not force:
        print(f"  [{slide_id}] Already exists, skipping.")
        return str(output_path)

    prompt = load_prompt(slide_id)
    enhanced = f"{STYLE_PREFIX}\n{prompt}"
    print(f"  [{slide_id}] Generating ({len(enhanced)} chars)...")

    message = Message(role="user", content=[{"text": enhanced}])

    try:
        rsp = ImageGeneration.call(
            model="wan2.7-image-pro",
            api_key=API_KEY,
            messages=[message],
            watermark=WATERMARK,
            n=1,
            size=IMAGE_SIZE,
        )

        if rsp.status_code != 200:
            print(f"  [{slide_id}] FAILED - {rsp.code} - {rsp.message}")
            return None

        output = rsp.output
        if not output or not output.get("choices"):
            print(f"  [{slide_id}] FAILED - No choices in response")
            return None

        choice = output["choices"][0]
        image_url = None
        for item in choice["message"]["content"]:
            if item.get("type") == "image":
                image_url = item["image"]
                break

        if not image_url:
            print(f"  [{slide_id}] FAILED - No image URL")
            return None

        print(f"  [{slide_id}] Downloading...")
        urllib.request.urlretrieve(image_url, str(output_path))
        print(f"  [{slide_id}] Saved to {output_path}")
        return str(output_path)

    except Exception as e:
        print(f"  [{slide_id}] EXCEPTION: {e}")
        import traceback
        traceback.print_exc()
        return None
