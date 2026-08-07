import subprocess
import sys
from pathlib import Path


BENCHMARK = Path(__file__).parents[1] / "benchmark" / "native_writer_performance.py"


def test_native_writer_benchmark_smoke(tmp_path):
    subprocess.run([sys.executable, BENCHMARK, "--self-test"], check=True)
    subprocess.run(
        [
            sys.executable,
            BENCHMARK,
            "--profile",
            "smoke",
            "--output-stem",
            tmp_path / "result",
        ],
        check=True,
    )
