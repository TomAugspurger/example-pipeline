from prefect import Flow, task
from pangeo_forge import AbstractPipeline


@task
def upper(s):
    return s.upper()


@task
def consolidate(values):
    return sum(values)


class Pipeline(AbstractPipeline):
    name = "example-pipeline"

    @property
    def sources(self):
        return ['a', 'b', 'c']

    @property
    def targets(self):
        return ['A', 'B', 'C']

    @property
    def flow(self) -> Flow:
        with Flow(self.name) as flow:
            transformed = upper.map(self.sources)
            consolidated = consolidate(transformed)
            print(consolidated)

        return flow
