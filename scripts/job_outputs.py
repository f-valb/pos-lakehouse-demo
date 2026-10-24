"""Print the notebook results of the latest run of a bundle job (including for_each iterations).

    uv run python scripts/job_outputs.py ml_train [-t dev]
"""

import argparse
import json
import subprocess

from databricks.sdk import WorkspaceClient

ap = argparse.ArgumentParser()
ap.add_argument("job_key")
ap.add_argument("-t", "--target", default="dev")
a = ap.parse_args()
summary = json.loads(subprocess.run(["databricks", "bundle", "summary", "-t", a.target, "-o", "json"],
                                    capture_output=True, text=True, check=True).stdout)
job_id = int(summary["resources"]["jobs"][a.job_key]["id"])
w = WorkspaceClient()
run = next(iter(w.jobs.list_runs(job_id=job_id, limit=1)))
run = w.jobs.get_run(run.run_id)
print(f"{a.job_key} run {run.run_id}: {run.state.result_state.value if run.state.result_state else run.state.life_cycle_state.value}")


def show(task_key, run_id, indent="  "):
    out = w.jobs.get_run_output(run_id)
    r = w.jobs.get_run(run_id)
    state = r.state.result_state.value if r.state.result_state else r.state.life_cycle_state.value
    res = out.notebook_output.result if out.notebook_output and out.notebook_output.result else ""
    print(f"{indent}{task_key:24} {state:9} {res or (out.error or '')[:200]}")


for t in run.tasks:
    sub = w.jobs.get_run(t.run_id)
    if sub.iterations:
        print(f"  {t.task_key} (for_each)")
        for it in sub.iterations:
            show(it.task_key, it.run_id, indent="    ")
    elif t.notebook_task:
        show(t.task_key, t.run_id)
    else:
        print(f"  {t.task_key:24} {t.state.result_state.value if t.state.result_state else t.state.life_cycle_state.value}")
