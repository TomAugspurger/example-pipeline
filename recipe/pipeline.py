from prefect import Flow, task
from prefect.environments.storage import GitHub
from pangeo_forge.pipelines.base import AbstractPipeline


@task
def upper(s: str) -> str:
    return s.upper()



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
            upper.map(self.sources)

        return flow

    @property
    def storage(self): 
        return GitHub("tomaugspurger/example-pipeline", path="recipe/pipeline.py")


pipeline = Pipeline()
