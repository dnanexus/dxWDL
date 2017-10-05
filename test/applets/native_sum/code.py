#!/usr/bin/env python

@dxpy.entry_point('main')
def main(**job_inputs):
    a = 0
    if 'a' in job_inputs:
        a = job_inputs['a']
    b = 0
    if 'b' in job_inputs:
        b = job_inputs['b']

    # Return output
    output = {}
    output["result"] = a + b
    return output

dxpy.run()
