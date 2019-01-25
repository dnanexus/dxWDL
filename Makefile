# Create a public docker image for the dxWDL compiler that allows a simple command line
# invocation:
#

TAG=0.79
create_docker_image:
	docker build -t dnanexus/dxwdl:$TAG .
	docker tag dnanexus/dxwdl:$TAG dnanexus/dxwdl:latest
	docker push dnanexus/dxwdl:$TAG
	docker push dnanexus/dxwdl:latest
