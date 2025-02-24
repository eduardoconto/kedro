from kedro.io.data_catalog import DataCatalog
from kedro.pipeline.modular_pipeline import pipeline
from kedro.pipeline import node
from kedro.runner.sequential_runner import SequentialRunner

catalog = DataCatalog({}, {"x": 0})

def node1(x):
    for i in range(10):
        x = x + 1
    print(f'on node1 {x}')
    return x

def stop_cond(y): # stop condition
    print(f'on stop_cond {y}')
    if y >= 100:
        return True
    else:
        return False 


pipe = pipeline([
    node(func = node1, inputs = "x", outputs = "y"),
    node(func = stop_cond, inputs = "y", outputs = "_stop"),
        # feedback: x < y
    ], feedback = {'x': 'y'})

out = SequentialRunner().run(pipe, catalog)
print(out)


