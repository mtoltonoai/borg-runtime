from pathlib import Path
import sys
root=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(root/'tooling'),str(root/'tests/tooling')]
from rollout_fixture import integration_main
integration_main()
