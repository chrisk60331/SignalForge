"""signalforge-campaigns — manage Customer.io campaign actions and email content."""
from __future__ import annotations

import argparse
import sys

import argcomplete
from dotenv import load_dotenv

from devpost_scraper.cli_shared import _ENV_FILE


def campaigns_main() -> None:
    """Manage Customer.io campaign actions and email content via the App API."""
    from devpost_scraper.campaigns import (
        cmd_get,
        cmd_get_actions,
        cmd_get_campaign,
        cmd_list_campaigns,
        cmd_show_campaign,
        cmd_update,
        cmd_update_all,
    )

    parser = argparse.ArgumentParser(
        prog="signalforge-campaigns",
        description="Sync email HTML with Customer.io campaign actions.",
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    sub.required = True

    sub.add_parser("list-campaigns", help="List all campaigns (id, state, name)")

    p_gc = sub.add_parser("get-campaign", help="Fetch one campaign + all its actions → emails/campaigns/{id}.json")
    p_gc.add_argument("--campaign-id", required=True, metavar="ID", help="Customer.io campaign ID")

    p_sc = sub.add_parser("show-campaign", help="Print a Mermaid flowchart of a campaign's action graph")
    p_sc.add_argument("--campaign-id", required=True, metavar="ID",
                      help="Customer.io campaign ID (must run get-campaign first)")

    p_ga = sub.add_parser("get-actions", help="Fetch all email actions for a campaign and pair with a local folder of HTML files")
    p_ga.add_argument("--campaign-id", required=True, metavar="ID", help="Customer.io campaign ID")
    p_ga.add_argument("--folder", required=True, metavar="PATH", help="Folder of HTML email files (e.g. emails/closed-hackathon)")
    p_ga.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt (for scripting)")

    p_ua = sub.add_parser("update-all", help="Push all manifest-linked HTML files for a campaign to cx.io")
    p_ua.add_argument("--campaign-id", required=True, metavar="ID", help="Customer.io campaign ID")

    p_get = sub.add_parser("get", help="Fetch one action from cx.io and upsert it into emails/manifest.json")
    p_get.add_argument("--campaign-id", required=True, metavar="ID", help="Customer.io campaign ID")
    p_get.add_argument("--action-id", required=True, metavar="ID", help="Customer.io action ID")
    p_get.add_argument("--file", metavar="PATH", default=None,
                       help="Local HTML file to link this action to (e.g. emails/closed-hackathon/variant-a.html)")

    p_update = sub.add_parser("update", help="Push a local HTML file's subject + body to cx.io")
    p_update.add_argument("--file", required=True, metavar="PATH", help="Local HTML file listed in emails/manifest.json")

    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    load_dotenv(_ENV_FILE, override=True)

    try:
        if args.command == "list-campaigns":
            cmd_list_campaigns()
        elif args.command == "get-campaign":
            cmd_get_campaign(campaign_id=args.campaign_id)
        elif args.command == "show-campaign":
            cmd_show_campaign(campaign_id=args.campaign_id)
        elif args.command == "get-actions":
            cmd_get_actions(campaign_id=args.campaign_id, folder=args.folder, yes=args.yes)
        elif args.command == "update-all":
            cmd_update_all(campaign_id=args.campaign_id)
        elif args.command == "get":
            cmd_get(campaign_id=args.campaign_id, action_id=args.action_id, file=args.file)
        elif args.command == "update":
            cmd_update(file=args.file)
    except RuntimeError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        sys.exit(1)
