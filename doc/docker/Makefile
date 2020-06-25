all : thinkbox

thinkbox:
	docker build -t dxwdl_workbench .
	docker run \
           --name dxwdl_thinkbox \
           --privileged \
	   --volume /tmp:/tmp \
	   --volume /var/run/docker.sock:/var/run/docker.sock \
	-it dxwdl_workbench

stop:
	docker stop dxwdl_thinkbox || true
	docker rm dxwdl_thinkbox || true
