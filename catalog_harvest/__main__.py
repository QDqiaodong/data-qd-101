import argparse
import json
from pathlib import Path
from .transport import page_fetcher
from .harvest import harvest


def main():
    parser=argparse.ArgumentParser(description="Harvest a local OAI-PMH dataset catalog")
    parser.add_argument("--endpoint",required=True)
    parser.add_argument("--db",type=Path,default=Path("run/catalog.sqlite"))
    args=parser.parse_args()
    try:
        result=harvest(page_fetcher(args.endpoint),args.db)
    except (ValueError,OSError) as exc:
        parser.exit(2,f"harvest failed: {exc}\n")
    print(json.dumps(result,sort_keys=True))


if __name__=="__main__":
    main()
