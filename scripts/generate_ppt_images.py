"""
Generate all PPT slide images for AI Data Copilot presentation.
Calls wan2.7-image-pro via DashScope API using config.py settings.
Reads prompts from docs/AI-Competition-Presentation-Image-Prompts.md.

Usage:
  python scripts/generate_ppt_images.py           # Generate all slides
  python scripts/generate_ppt_images.py S1 S3 S5  # Generate specific slides
  python scripts/generate_ppt_images.py --regenerate S2  # Force regenerate one slide
"""

import sys
import re
import urllib.request
from pathlib import Path

import dashscope
from dashscope.aigc.image_generation import ImageGeneration
from dashscope.api_entities.dashscope_response import Message

# Load config
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import MODEL_API_KEY

# Correct base URL for DashScope image generation API
IMAGE_API_URL = "https://dashscope.aliyuncs.com/api/v1"
dashscope.base_http_api_url = IMAGE_API_URL

API_KEY = MODEL_API_KEY

OUTPUT_DIR = Path(__file__).parent.parent / "docs" / "ppt-images"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Image settings
IMAGE_SIZE = "2K"
WATERMARK = False

# All slide IDs in order
ALL_SLIDES = [
    "S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8",
    "S9", "S10", "S11", "S12", "S13", "S14", "S15", "S16", "S17",
]


def parse_prompts(md_path: str, slide_ids: list[str]) -> dict[str, str]:
    """Parse slide prompts from the markdown file."""
    content = Path(md_path).read_text(encoding="utf-8")
    result = {}

    for sid in slide_ids:
        # Find section from "## S{id}" to next "## S" or end
        section_match = re.search(rf"(## {sid}\s*—[^\n]*\n.*?)(?=## S\d+|\Z)", content, re.DOTALL)
        if not section_match:
            print(f"  WARNING: Could not find section {sid}")
            continue

        section = section_match.group(1)
        prompt_lines = []
        in_prompt = False

        for line in section.split("\n"):
            stripped = line.strip()

            if stripped.startswith("**提示词：**"):
                in_prompt = True
                continue

            if not in_prompt:
                continue

            if stripped.startswith("> "):
                prompt_lines.append(stripped[2:])
            elif stripped == ">" or stripped == "":
                if prompt_lines:
                    prompt_lines.append("")  # preserve paragraph breaks
            elif stripped.startswith("|") or stripped.startswith("**") or stripped.startswith("## "):
                if prompt_lines:
                    break
            elif stripped:
                prompt_lines.append(stripped)

        # Clean: remove empty trailing lines, collapse consecutive blank lines
        cleaned = []
        prev_blank = False
        for line in prompt_lines:
            if line == "":
                if not prev_blank:
                    cleaned.append(line)
                prev_blank = True
            else:
                cleaned.append(line)
                prev_blank = False

        prompt = "\n".join(cleaned).strip()
        if prompt:
            result[sid] = prompt
            print(f"  Parsed {sid}: {len(prompt)} chars")
        else:
            print(f"  WARNING: No prompt found for {sid}")

    return result


def apply_global_style(prompt: str) -> str:
    """Apply global style constraints to the prompt."""
    style_prefix = (
        "科技竞赛风格PPT插图，深蓝主色调（#0A1628），电光蓝（#00D4FF）发光点缀。"
        "16:9宽屏比例，渐变背景，发光线条，半透明卡片，圆角模块。"
        "竞赛级路演感，信息密度高但不杂乱。"
        "画面中所有文字必须使用中文。"
    )

    # Avoid duplicating style if already present
    if "科技竞赛" not in prompt and "16:9" not in prompt:
        return f"{style_prefix}\n{prompt}"
    return prompt


def generate_image(slide_id: str, prompt: str, force: bool = False) -> str | None:
    """Generate a single image via wan2.7-image-pro sync API."""
    output_path = OUTPUT_DIR / f"{slide_id}.png"

    if output_path.exists() and not force:
        print(f"  {slide_id}: Already exists, skipping. (use --regenerate {slide_id} to overwrite)")
        return str(output_path)

    enhanced_prompt = apply_global_style(prompt)
    print(f"  {slide_id}: Generating ({len(enhanced_prompt)} chars)...")

    message = Message(
        role="user",
        content=[{"text": enhanced_prompt}],
    )

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
            print(f"  {slide_id}: FAILED - {rsp.code} - {rsp.message}")
            return None

        output = rsp.output
        if not output or not output.get("choices"):
            print(f"  {slide_id}: FAILED - No choices in response")
            return None

        choice = output["choices"][0]
        content_items = choice["message"]["content"]
        image_url = None
        for item in content_items:
            if item.get("type") == "image":
                image_url = item["image"]
                break

        if not image_url:
            print(f"  {slide_id}: FAILED - No image URL in response")
            return None

        print(f"  {slide_id}: Downloading from URL...")
        urllib.request.urlretrieve(image_url, str(output_path))
        print(f"  {slide_id}: Saved to {output_path}")
        return str(output_path)

    except Exception as e:
        print(f"  {slide_id}: EXCEPTION: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    # Parse arguments
    args = sys.argv[1:]
    force_regenerate = False
    slide_ids = ALL_SLIDES  # default: all

    if "--regenerate" in args:
        force_regenerate = True
        args = [a for a in args if a != "--regenerate"]

    if args:
        slide_ids = args

    md_path = Path(__file__).parent.parent / "docs" / "AI-Competition-Presentation-Image-Prompts.md"

    print(f"\n{'='*60}")
    print(f"AI Data Copilot - PPT Image Generator")
    print(f"{'='*60}")
    print(f"Model: wan2.7-image-pro")
    print(f"Size: {IMAGE_SIZE}")
    print(f"Output: {OUTPUT_DIR}")
    mode = "REGENERATE" if force_regenerate else "NORMAL"
    print(f"Mode: {mode}")
    print(f"Slides: {', '.join(slide_ids)} ({len(slide_ids)} total)")
    print(f"{'='*60}\n")

    # Parse prompts
    print("Parsing prompts...")
    prompts = parse_prompts(str(md_path), slide_ids)

    if not prompts:
        print("No prompts found. Exiting.")
        return

    print(f"\nGenerating {len(prompts)} images...\n")

    # Generate sequentially
    results = {}
    for sid, prompt in prompts.items():
        result = generate_image(sid, prompt, force=force_regenerate)
        results[sid] = result
        print()

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    ok = []
    fail = []
    for sid in slide_ids:
        path = results.get(sid)
        if path:
            ok.append(sid)
            print(f"  {sid}: [OK] {path}")
        else:
            fail.append(sid)
            print(f"  {sid}: [FAILED]")

    print(f"\nResult: {len(ok)}/{len(slide_ids)} succeeded")
    if fail:
        print(f"  Failed: {', '.join(fail)}")
    if ok:
        print(f"  Succeeded: {', '.join(ok)}")


if __name__ == "__main__":
    main()
