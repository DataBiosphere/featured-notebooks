# publish to: "terra-notebook-utils-tests" "test-workflow-cost-estimator"
import os
import herzog

# Mock the notebook environment
os.environ['WORKSPACE_NAME'] = "terra-notebook-utils-tests"
os.environ['WORKSPACE_BUCKET'] = "gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"
os.environ['GOOGLE_PROJECT'] = "firecloud-cgl"

with herzog.Cell("markdown"):
    """
    # Workflow Cost Estimator
    This notebook demonstrates cost estimation for finished or in-progress workflows.

    This is an experimental feature:
      - Cost estimates may not be accurate.
      - CPUs, Memory, and runtime is pulled from Terra's Firecloud API
        [monitorSubmission](https://api.firecloud.org/#/Submissions/monitorSubmission) endpoint. This information is
        available for 42 days after workflow completion.
      - GCP Instance type is assumed custom configurations of either N1 or N2 instance type.

    *author: Brian Hannafious, Genomics Institute, University of California Santa Cruz*
    """

with herzog.Cell("markdown"):
    """
    Install the newest version of [terra-notebook-utils](https://github.com/DataBiosphere/terra-notebook-utils)
    """

with herzog.Cell("python"):
    #%pip install --upgrade --no-cache-dir git+https://github.com/DataBiosphere/terra-notebook-utils
    pass

with herzog.Cell("markdown"):
    """
    Define some useful functions.
    """

with herzog.Cell("python"):
    import pandas as pd
    from terra_notebook_utils import costs, workflows

    def list_submissions_chronological():
        listing = [(s['submissionDate'], s) for s in workflows.list_submissions()]
        for date, submission in sorted(listing):
            yield submission

    def cost_for_submission(submission_id: str):
        submission = workflows.get_submission(submission_id)
        for wf in submission['workflows']:
            for shard_info in workflows.estimate_workflow_cost(submission_id, wf['workflowId']):
                shard_info['workflow_id'] = wf['workflowId']
                yield shard_info

    def estimate_job_cost(cpus: int, memory_gb: int, runtime_hours: float, preemptible: bool) -> float:
        return costs.GCPCustomN1Cost.estimate(cpus, memory_gb, runtime_hours * 3600, preemptible)

with herzog.Cell("markdown"):
    """
    List submissions in chronological order.
    """

with herzog.Cell("python"):
    for s in list_submissions_chronological():
        print(s['submissionId'], s['submissionDate'], s['status'])

# Insert a test submission id
submission_id = "7d4d4bbd-6d3a-4e8f-848d-3992f5bd8e33"
# TODO: workflow metadata expires after 40 days, which will cause this test to break. Is there a better way?

with herzog.Cell("python"):
    submission_id = "b3cd3f66-390a-41a9-9cdd-62593442d6fc"
    report = pd.DataFrame()
    for shard_info in cost_for_submission(submission_id):
        shard_info['duration'] /= 3600
        report = report.append(shard_info, ignore_index=True)

with herzog.Cell("python"):
    report.style.format(dict(cost="${:.2f}", duration="{:.2f}h", memory="{:.0f}GB"))

with herzog.Cell("python"):
    print("Total cost: $%.2f" % report['cost'].sum())

with herzog.Cell("markdown"):
    """
    Explore costs for potential workflow configurations and runtimes.
    """

with herzog.Cell("python"):
    # Define configurations for: cpus, memory(GB), runtime(hours), preemptible
    configurations = [(10, 64, 5, False),
                      (8, 32, 10, False),
                      (10, 64, 5, True),
                      (8, 32, 10, True)]
    report = pd.DataFrame()
    for cpus, memory_gb, runtime_hours, preemptible in configurations:
        cost = estimate_job_cost(cpus, memory_gb, runtime_hours, preemptible)
        report = report.append(dict(cost=cost, cpus=cpus, memory=memory_gb, duration=runtime_hours, preemptible=preemptible), ignore_index=True)
    report['preemptible'] = report['preemptible'].astype('bool')

with herzog.Cell("python"):
    report.style.format(dict(cost="${:.2f}", duration="{:.2f}h", memory="{:.0f}GB"))

with herzog.Cell("markdown"):
    """
    ## Contributions
    Contributions, bug reports, and feature requests are welcome on:
      - [terra-notebook-utils GitHub](https://github.com/DataBiosphere/terra-notebook-utils) for general functionality.
      - [bdcat_notebooks GitHub](https://github.com/DataBiosphere/bdcat_notebooks) for this notebook.
    """
################################################ TESTS ################################################ noqa
