> Q: Do you know if it is possible to use AWS ECR directly in conjunction with DNAnexus as a way to host docker images? It would be good to skip the step of saving the images and uploading them.

A: We currently only support DockerHub and dx:// URLs for docker images. I have notified our developers about this feature suggestion. The more users requesting a feature, it will get higher priority on our development roadmap so we thank you for your feedback.

> Q: How does dxWDL work when trying to rerun jobs with exactly the same input parameters? Does it rerun everything, or just pieces that are different?

A: Any workflows on the platform (WDL compiled or not), can have their output data reused if the job executable in any of the stage and the input data are the same as previously-run analysis, and that the output data is intact (no missing objects).

To achieve this, the org-level jobReuse feature needs to be enabled. You can check this by running

`dx describe org-xxx --json | jq .policies`

and look for the "jobReuse" line in the output.

If the line is not found, then please let us know the name of your org account to request for this feature. This is a licensed feature which means that it cannot be enabled for individual user account.

If this line is present but the value is "false", then the org admin will need to turn it on by running the following command:

`dx api org-xxx update '{"policies":{"jobReuse":true}}'`

After the feature is turned on, the reuse works automatically unless options like "--ignore-reuse" is specified at the runtime.

> Q: Is there a way to configure dx workflows so that only the files designated as workflow outputs are kept upon completion? Or perhaps a way to move intermediate files into a different output directory?

A: The intermediate output files can be moved to a specified folder by using the "--reorg" option during runtime. Detailed information on how to use it can be found in

[https://github.com/dnanexus/dxWDL/blob/master/doc/ExpertOptions.md#handling-intermediate-workflow-outputs](https://github.com/dnanexus/dxWDL/blob/master/doc/ExpertOptions.md#handling-intermediate-workflow-outputs)

> Q: What does dxFUSE do if you try to randomly access parts of files that are streamed? As far as I know, you can do this with the regular FUSE package.

A: dxFUSE random access performance is good enough if you are accessing a small part of a file. Otherwise you are better of downloading the file to the local storage insstead of streaming. dxFUSE support mounting Dx containers to access its contents.
