"""
download_zappy_images.py
--------------------------
Self-hosts every Zappy's image instead of depending on any IPFS gateway
at runtime. Background: the site was rewriting image URLs to Cloudflare's
public IPFS gateway (cloudflare-ipfs.com) as a fix -- turns out that
gateway was fully decommissioned by Cloudflare in August 2024, so that
"fix" pointed at a dead service. Rather than keep gateway-hopping and
hoping the next one stays up, this downloads every image ONCE and serves
it as an ordinary static file alongside the rest of the site -- nothing
left to go down, get rate-limited, or get decommissioned out from under
us again.

Designed to run in GitHub Actions (see .github/workflows/sync-zappy-images.yml),
NOT locally -- this needs `requests` and `pillow`, a full outbound internet
connection, and takes a few minutes for ~1700 images. Actions runners have
all of that for free; a local machine might not.

What it does:
  1. Reads every {asset_id: {name, image_url, VLT, INS, SPK}} entry from
     the existing zappy_stats.json.
  2. For each one, tries a short list of currently-live IPFS gateways in
     order until one responds -- ipfs.io and dweb.link (the official
     successor Cloudflare itself pointed users to when it shut its own
     gateway down). Whichever responds first wins; this isn't betting
     everything on one gateway's uptime the way the previous fix did.
  3. Resizes each image down to a reasonable thumbnail size (these render
     as small square cards on the site, never full resolution) and saves
     it as a JPEG in zappy-images/, keeping the repo size sane.
  4. Writes an updated zappy_stats.json with image_url rewritten to a
     relative local path (./zappy-images/{asset_id}.jpg) instead of any
     IPFS URL.
  5. Logs anything that failed on every gateway tried, so those can be
     looked at individually rather than silently becoming missing images.
"""

import json
import os
import sys
import concurrent.futures
import requests
from PIL import Image
from io import BytesIO

STATS_PATH = "zappy_stats.json"
OUTPUT_DIR = "zappy-images"
THUMBNAIL_SIZE = (400, 400)  # max dimension -- these are small cards, not full art
CONCURRENCY = 12             # parallel downloads; polite, not hammering any one gateway
REQUEST_TIMEOUT = 15         # seconds per gateway attempt

# Tried in order per image. dweb.link is the gateway Cloudflare itself
# named as the successor when it shut cloudflare-ipfs.com down -- see the
# module docstring for why that history matters here.
GATEWAYS = [
    "https://dweb.link/ipfs/{cid}",
    "https://ipfs.io/ipfs/{cid}",
]


def extract_cid(image_url: str) -> str | None:
    """Pulls the CID out of an existing ipfs.io-style URL."""
    if "/ipfs/" not in image_url:
        return None
    return image_url.split("/ipfs/", 1)[1]


def fetch_image_bytes(cid: str) -> bytes | None:
    """Tries each gateway in order, returns the first successful response's bytes."""
    for template in GATEWAYS:
        url = template.format(cid=cid)
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200 and resp.content:
                return resp.content
        except requests.RequestException:
            continue
    return None


def process_one(asset_id: str, entry: dict) -> tuple[str, bool]:
    """Downloads, resizes, and saves one Zappy's image. Returns (asset_id, success)."""
    cid = extract_cid(entry.get("image_url", ""))
    if not cid:
        return asset_id, False

    raw = fetch_image_bytes(cid)
    if raw is None:
        return asset_id, False

    try:
        img = Image.open(BytesIO(raw)).convert("RGB")
        img.thumbnail(THUMBNAIL_SIZE, Image.LANCZOS)
        out_path = os.path.join(OUTPUT_DIR, f"{asset_id}.jpg")
        img.save(out_path, "JPEG", quality=85)
        return asset_id, True
    except Exception as e:
        print(f"  [{asset_id}] downloaded but failed to process: {e}")
        return asset_id, False


def main():
    with open(STATS_PATH) as f:
        stats = json.load(f)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Idempotent re-runs: an entry whose image_url already points at a
    # local file (from a previous successful run) has no CID to extract
    # and would otherwise get misreported as a fresh failure every time
    # this runs again. Skip those entirely -- only attempt entries still
    # pointing at an external ipfs.io URL.
    already_local = {aid for aid, entry in stats.items() if entry.get("image_url", "").startswith(f"./{OUTPUT_DIR}/")}
    to_process = {aid: entry for aid, entry in stats.items() if aid not in already_local}

    if already_local:
        print(f"Skipping {len(already_local)} already self-hosted from a previous run.")
    print(f"Processing {len(to_process)} remaining Zappies with {CONCURRENCY} parallel workers...")
    succeeded, failed = [], []

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = {pool.submit(process_one, asset_id, entry): asset_id for asset_id, entry in to_process.items()}
        done_count = 0
        for future in concurrent.futures.as_completed(futures):
            asset_id, ok = future.result()
            (succeeded if ok else failed).append(asset_id)
            done_count += 1
            if done_count % 100 == 0:
                print(f"  ...{done_count}/{len(to_process)} processed ({len(failed)} failures so far)")

    # Rewrite zappy_stats.json to point at the local files for everything
    # that succeeded. Anything that failed on every gateway keeps its
    # original ipfs.io URL rather than pointing at a file that doesn't
    # exist -- the site's existing dicebear-avatar fallback still covers
    # that case if the original URL also fails at render time.
    for asset_id in succeeded:
        stats[asset_id]["image_url"] = f"./{OUTPUT_DIR}/{asset_id}.jpg"

    with open(STATS_PATH, "w") as f:
        json.dump(stats, f, indent=2)

    print(f"\nDone. {len(succeeded)} succeeded, {len(failed)} failed.")
    if failed:
        print("Failed asset_ids (kept original ipfs.io URL, unchanged):")
        for asset_id in failed:
            print(f"  {asset_id}")
        # Non-zero exit if a meaningful fraction failed, so the Actions
        # run visibly flags it rather than silently committing a partial
        # result that looks complete.
        if to_process and len(failed) > len(to_process) * 0.05:
            print(f"\nWARNING: {len(failed)} failures is more than 5% of the collection -- check gateway health before trusting this run.")
            sys.exit(1)


if __name__ == "__main__":
    main()
