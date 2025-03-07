from kedro.framework.context.context import KedroContext
from kedro.framework.session.session import KedroSession
from kedro.io.data_catalog import DataCatalog
from kedro.pipeline.modular_pipeline import pipeline
from kedro.pipeline import node
from kedro.runner.sequential_runner import SequentialRunner

def node1(x):
    for i in range(10):
        x = x + 1
    print(f'on node1 {x}')
    return x

def stop_cond(y): # stop condition
    print(f'on stop_cond {y}')
    if y >= 100:
        return y, True
    else:
        return y, False 

pipe = pipeline([
    node(func = node1, inputs = "x", outputs = "y"),
    node(func = stop_cond, inputs = "y", outputs = ["z", "_stop"]),
        # feedback: x < y
    ], feedback = {'x': 'y'})

catalog = DataCatalog({}, {"x": 0})
#out = SequentialRunner().run(pipe, catalog)
#print(out)

def double(x):
    return x*2

pipe1 = pipeline([
    node(func = pipe, inputs = "x", outputs = "y"),
    node(func = double, inputs="y", outputs="z")
    ])

out = SequentialRunner().run(pipe1, catalog)
print(out)
