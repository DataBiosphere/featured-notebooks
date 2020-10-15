# publish to: "terra-notebook-utils-tests" "test-workflow-cost-estimator"
import os
import callysto

# Mock the notebook environment
os.environ['WORKSPACE_NAME'] = "terra-notebook-utils-tests"
os.environ['WORKSPACE_BUCKET'] = "gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"
os.environ['GOOGLE_PROJECT'] = "firecloud-cgl"

with callysto.Cell("markdown"):
    """
    # Workflow Cost Estimator
    This notebook demonstrates cost estimation for finished or in-progress workflows.

    This is an experimental feature:
      - Cost estimates may not be accurate.
      - CPUs, Memory, and runtime is pulled from Terra's Firecloud API
        [monitorSubmission](https://api.firecloud.org/#/Submissions/monitorSubmission) endpoint. This information is
        available for 42 days after workflow completion.
      - GCP Instance type is assumed custom configurations of eith N1 or N2 instance type.
    """

with callysto.Cell("markdown"):
    """
    Install the newest version of [terra-notebook-utils](https://github.com/DataBiosphere/terra-notebook-utils)
    """

with callysto.Cell("python"):
    #%pip install --upgrade --no-cache-dir git+https://github.com/DataBiosphere/terra-notebook-utils
    pass

with callysto.Cell("markdown"):
    """
    Define some useful functions.
    """

with callysto.Cell("python"):
    from terra_notebook_utils import costs, workflows

    def list_submissions_chronological():
        listing = [(s['submissionDate'], s) for s in workflows.list_submissions()]
        for date, submission in sorted(listing):
            yield submission

    def cost_for_submission(submission_id: str):
        submission = workflows.get_submission(submission_id)
        for wf in submission['workflows']:
            shard_number = 1  # keep track of scattered workflows
            for shard_info in workflows.estimate_workflow_cost(submission_id, wf['workflowId']):
                shard_info['workflow_id'] = wf['workflowId']
                shard_info['shard'] = shard_number
                shard_number += 1
                yield shard_info

    def estimate_job_cost(cpus: int, memory_gb: int, runtime_hours: float, preemptible: bool) -> float:
        return costs.GCPCustomN1Cost.estimate(cpus, memory_gb, runtime_hours * 3600, preemptible)

with callysto.Cell("markdown"):
    """
    List submissions in chronological order.
    """

with callysto.Cell("python"):
    for s in list_submissions_chronological():
        print(s['submissionId'], s['submissionDate'], s['status'])

# Insert a test submission id
submissions = [s for s in list_submissions_chronological()]
submission_id = submissions[-1]['submissionId']

with callysto.Cell("python"):
    # submission_id = ""  # Uncomment and insert your submission id here
    total_cost = 0
    print("%37s" % "workflow_id",
          "%6s" % "shard",
          "%5s" % "cpus",
          "%12s" % "memory",
          "%13s" % "duration",
          "%7s" % "cost")
    for shard_info in cost_for_submission(submission_id):
        total_cost += shard_info['cost']
        print("%37s" % shard_info['workflow_id'],
              "%6i" % shard_info['shard'],
              "%5i" % shard_info['number_of_cpus'],
              "%10iGB" % shard_info['memory'],
              "%12.2fh" % (shard_info['duration'] / 3600),  # convert from seconds to hours
              "%7s" % ("$%.2f" % shard_info['cost']))
        shard_info['duration'] /= 3600  # convert from seconds to hours
    print("%85s" % ("total_cost: $%.2f" % round(total_cost, 2)))

with callysto.Cell("markdown"):
    """
    Explore costs for potential workflow configurations and runtimes.
    """

with callysto.Cell("python"):
    # Define configurations for: cpus, memory(GB), runtime(hours), preemptible
    configurations = [(10, 64, 5, False),
                      (8, 32, 10, False),
                      (10, 64, 5, True),
                      (8, 32, 10, True)]
    print("%8s" % "cpus",
          "%8s" % "memory",
          "%8s" % "runtime",
          "%12s" % "preemptible",
          "%8s" % "cost")
    for cpus, memory_gb, runtime_hours, preemptible in configurations:
        cost = estimate_job_cost(cpus, memory_gb, runtime_hours, preemptible)
        print("%8i" % cpus,
              "%6iGB" % memory_gb,
              "%7ih" % runtime_hours,
              "%12s" % str(preemptible),
              "%8s" % ("$%.2f" % cost))
################################################ TESTS ################################################ noqa
