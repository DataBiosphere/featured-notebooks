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
      - GCP Instance type is assumed custom configurations of eith N1 or N2 instance type.

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
    from terra_notebook_utils import costs, workflows, WORKSPACE_NAME, WORKSPACE_GOOGLE_PROJECT

    def list_submissions_chronological(workspace: str=WORKSPACE_NAME,
                                       workspace_namespace: str=WORKSPACE_GOOGLE_PROJECT):
        listing = [(s['submissionDate'], s) for s in workflows.list_submissions(workspace, workspace_namespace)]
        for date, submission in sorted(listing):
            yield submission

    def cost_for_submission(submission_id: str,
                            workspace: str=WORKSPACE_NAME,
                            workspace_namespace: str=WORKSPACE_GOOGLE_PROJECT):
        workflows_metadata = workflows.get_all_workflows(submission_id, workspace, workspace_namespace)
        for workflow_id, workflow_metadata in workflows_metadata.items():
            if "Submitted" == workflow_metadata['status']:
                print("Workflow has submitted status, cost estimates may be unavailable")
            elif "Failed" == workflow_metadata['status']:
                print("No workflow IDs found, submission has status failed.")
            else:
                shard_number = 1  # keep track of scattered workflows
                for shard_info in workflows.estimate_workflow_cost(workflow_id, workflow_metadata):
                    shard_info['workflow_id'] = workflow_id
                    shard_info['shard'] = shard_number
                    shard_number += 1
                    yield shard_info

    def estimate_job_cost(cpus: int, memory_gb: int, disk_gb: int, runtime_hours: float, preemptible: bool) -> float:
        disk = costs.PersistentDisk.estimate(disk_gb, runtime_hours * 3600)
        compute = costs.GCPCustomN1Cost.estimate(cpus, memory_gb, runtime_hours * 3600, preemptible)
        return disk + compute

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

with herzog.Cell("markdown"):
    """
    Some possible errors you may run into when running the next cells:

    1. If you get a HTTP error, the submission cannot be found. Make sure you are entering a submission ID, not a workflow ID.
    2. If you get a note about there being no workflow IDs and the status being failed, your workflow was cancelled before even running. Usually, this is because Terra detected invalid inputs.
    3. If you get a note about submitted status, and no other information, then your workflow has not started yet. Try waiting a few minutes.


    Terra's ability to detect invalid inputs tends to be fast, so it is unlikely for cases 2 and 3 overlap.
    """

with herzog.Cell("python"):
    # submission_id = "388beeb8-5e44-4215-8a71-89f2625fbc45"  # Uncomment and insert your submission id here
    total_cost = 0
    print("%37s" % "workflow_id",
          "%30s" % "task_name",
          "%5s" % "cpus",
          "%7s" % "memory",
          "%7s" % "disk",
          "%9s" % "duration",
          "%7s" % "cost")
    for shard_info in cost_for_submission(submission_id):
        total_cost += shard_info['cost']
        print("%37s" % shard_info['workflow_id'],
              "%30s" % shard_info['task_name'],
              "%5i" % shard_info['number_of_cpus'],
              "%5iGB" % shard_info['memory'],
              "%5iGB" % shard_info['disk'],
              "%8.2fh" % (shard_info['duration'] / 3600),  # convert from seconds to hours
              "%7s" % ("$%.2f" % shard_info['cost']))
        shard_info['duration'] /= 3600  # convert from seconds to hours
    print("%108s" % ("total_cost: $%.2f" % round(total_cost, 2)))
    # If the output here is blank, your submission probably hasn't started yet. Try re-running this notebook in a few minutes.

with herzog.Cell("markdown"):
    """
    Explore costs for potential workflow configurations and runtimes.
    """

with herzog.Cell("python"):
    # Define configurations for: cpus, memory(GB), runtime(hours), preemptible
    configurations = [(10, 64, 700, 5, False),
                      (8, 32, 700, 10, False),
                      (10, 64, 700, 5, True),
                      (8, 32, 700, 10, True),
                      (8, 32, 400, 10, True),
                      (8, 32, 100, 10, True)]

    print("%8s" % "cpus",
          "%8s" % "memory",
          "%8s" % "disk",
          "%8s" % "runtime",
          "%12s" % "preemptible",
          "%8s" % "cost")
    for cpus, memory_gb, disk_gb, runtime_hours, preemptible in configurations:
        cost = estimate_job_cost(cpus, memory_gb, disk_gb, runtime_hours, preemptible)
        print("%8i" % cpus,
              "%6iGB" % memory_gb,
              "%6iGB" % disk_gb,
              "%7ih" % runtime_hours,
              "%12s" % str(preemptible),
              "%8s" % ("$%.2f" % cost))

with herzog.Cell("markdown"):
    """
    ## Contributions
    Contributions, bug reports, and feature requests are welcome on:
      - [terra-notebook-utils GitHub](https://github.com/DataBiosphere/terra-notebook-utils) for general functionality.
      - [featured-notebooks GitHub](https://github.com/DataBiosphere/featured-notebooks) for this notebook.
    """
################################################ TESTS ################################################ noqa
