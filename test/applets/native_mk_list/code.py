#!/usr/bin/env python

@dxpy.entry_point('main')
def main(**job_inputs):
    a = job_inputs['a']
    b = job_inputs['b']

    # Return output
    output = {}
    output["all"] = [a, b]
    return output

dxpy.run()
