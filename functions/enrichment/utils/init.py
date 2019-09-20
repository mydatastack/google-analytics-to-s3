from functools import reduce, partial

pipe = lambda fns: lambda x: reduce(lambda v, f: f(v), fns, x)

def init(input_, filters, output, error): 
    def inner(event: dict):
        return pipe([
                    input_,
                    *filters,
                    partial(output, event),
                    error
                    ]) (event)
    return inner
