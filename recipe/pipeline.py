import fsspec
from prefect import Flow, task
from pangeo_forge.pipelines.base import AbstractPipeline


@task
def upper(s: str) -> str:
    return s.upper()


@task
def write(x: str):
    dest = "gs://pangeo-forge-scratch/example-pipeline.txt"
    with fsspec.open(dest, "wb") as f:
        f.write(x.encode())


class Pipeline(AbstractPipeline):
    name = "example-pipeline"
    repo = "TomAugspurger/example-pipeline"

    @property
    def sources(self):
        return ['a', 'b', 'c']

    @property
    def targets(self):
        return ['A', 'B', 'C']

    @property
    def flow(self) -> Flow:
        with Flow(self.name) as flow:
            a = upper.map(self.sources)
            write.map(a)


        return flow


pipeline = Pipeline()
flow = pipeline.flow
