# WDL Runner App

This page discusses a possible design of the dx WDL runner that is based on an app, instead
of custom applets built for each workflow fragment.

The app will have a signature like this:
- input: hash containing WDL inputs
- output: hash containing WDL outputs
- files: an array of dx:files
- sourceCode: WDL script

Using hashes to wrap around inputs and outputs is defined as *boxed*
calling convention. Using native dnanexus types (*int*, *float*,
*dx:file*, ...) to accept inputs and return outputs is termed *unboxed*.

A task is compiled into an unboxed applet.

A workflow is decomposed into fragments, each fragment used boxed calling.

Tricky areas:
- Calling native apps, applets, and WDL tasks requires unboxing. Collecting the results
requires boxing the results up.

A task should be compiled into a custom applet to allow setting the runspec, access,
and various other attributes.

A workflow uses boxed calling convention
