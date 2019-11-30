from kubernetes import client, config


class Job:
    """Wrapper class for Kubernetes Job"""

    def __init__(self, api_instance, job_name, *args, **kwargs):
        self._api_instance = api_instance
        self._job = self._job_constructor(job_name, *args, **kwargs)

    def create(self):
        """Creates job instance"""
        response = self._api_instance.create_namespaced_job(
            body=self._job,
            namespace="default",
        )
        return response

    @staticmethod
    def _job_constructor(job_name, *args, **kwargs):
        """Returns a V1Job object"""

        # Resource requirements
        limits = {
            "cpu": "500m",
            "memory": "1Gi",
        }
        requests = {
            "cpu": "200m",
            "memory": "500Mi",
        }
        resources = client.V1ResourceRequirements(
            limits=limits,
            requests=requests,
        )

        # Configureate Pod template container
        container = client.V1Container(
            name="scheduler",
            image="perl",
            command=["perl", "-Mbignum=bpi", "-wle", "print bpi(2000)"],
            resources=resources,
        )

        # Create and configurate a spec section
        labels = {
            "app": job_name,
        }
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels=labels),
            spec=client.V1PodSpec(restart_policy="Never", containers=[container]),
        )

        # Create the specification of deployment
        spec = client.V1JobSpec(
            template=template,
            backoff_limit=4,
        )

        # Instantiate the job object
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=job_name),
            spec=spec,
        )

        return job

if __name__ == "__main__":
    # Configs can be set in Configuration class directly or using helper
    # utility. If no argument provided, the config will be loaded from
    # default location.
    config.load_kube_config()
    batch_v1 = client.BatchV1Api()

    job = Job(batch_v1, job_name="intlcrpglobal-20190101-080000")
    job.create()
