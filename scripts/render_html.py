"""Render the overview Markdown into a single-file interactive HTML report."""

import argparse
import json
import os

from jinja2 import Environment, FileSystemLoader

from render_markdown_parser import parse_markdown
from report_view_model import build_dashboard_view_model


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def derive_trade_date(report_title, cli_trade_date):
    """Prefer the CLI trade date and fall back to the title suffix."""
    if cli_trade_date:
        return cli_trade_date
    if "·" in report_title:
        return report_title.split("·")[-1].strip()
    return ""


def load_backtest_payload(backtest_path):
    if not backtest_path:
        return None
    with open(backtest_path, encoding="utf-8") as handle:
        return json.load(handle)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="inp", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--title", default="可转债概览")
    parser.add_argument("--trade-date", default="")
    parser.add_argument("--backtest", default="")
    args = parser.parse_args()

    with open(args.inp, encoding="utf-8") as handle:
        report = parse_markdown(handle.read())

    trade_date = derive_trade_date(report.get("title", ""), args.trade_date)
    backtest = load_backtest_payload(args.backtest)
    view_model = build_dashboard_view_model(report, trade_date, backtest)

    with open(os.path.join(SCRIPT_DIR, "static", "style.css"), encoding="utf-8") as handle:
        css = handle.read()
    with open(os.path.join(SCRIPT_DIR, "static", "app.js"), encoding="utf-8") as handle:
        js = handle.read()

    env = Environment(loader=FileSystemLoader(os.path.join(SCRIPT_DIR, "templates")))
    template = env.get_template("base.html.j2")
    html_out = template.render(
        title=args.title,
        view_model=view_model,
        view_model_json=json.dumps(view_model, ensure_ascii=False),
        css=css,
        js=js,
    )

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as handle:
        handle.write(html_out)
    print(f"[done] -> {args.out} ({os.path.getsize(args.out)} bytes)")


if __name__ == "__main__":
    main()
